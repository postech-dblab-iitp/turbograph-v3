#include "statistics/histogram_generator.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/types/data_chunk.hpp"

#include <boost/array.hpp>
#include <numeric>


using namespace boost::accumulators;
namespace duckdb {

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client) {
    Catalog &cat_instance = client->db->GetCatalog();
    SchemaCatalogEntry *schema_cat = cat_instance.GetSchema(*client.get());

    // Find all partitions
    vector<idx_t> part_oids;
    schema_cat->Scan(CatalogType::PARTITION_ENTRY,
        [&](CatalogEntry *entry) {
            part_oids.push_back(entry->GetOid());
        });
    
    // Call CreateHistogram for each partition
    for (auto &part_oid : part_oids) {
        CreateHistogram(client, part_oid);
    }
}

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client, string &part_name) {
    Catalog &cat_instance = client->db->GetCatalog();

    // Get partition catalog
    PartitionCatalogEntry *partition_cat =
        (PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, part_name);
    
    _create_histogram(client, partition_cat);
}

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client, idx_t partition_oid) {
    Catalog &cat_instance = client->db->GetCatalog();

    // Get partition catalog
    PartitionCatalogEntry *partition_cat =
        (PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, partition_oid);
    
    _create_histogram(client, partition_cat);
}

void HistogramGenerator::_create_histogram(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat) {
    Catalog &cat_instance = client->db->GetCatalog();

    // Get universal schema
    vector<LogicalType> universal_schema = std::move(partition_cat->GetTypes());

    // Initialize accumulators for histogram
    boost::array<double, 5> probs = {{0, 0.25, 0.5, 0.75, 1.00}}; // TODO need to define bin size
    // boost::array<double, 10> probs = { 0.990, 0.991, 0.992, 0.993, 0.994,
    //                            0.995, 0.996, 0.997, 0.998, 0.999 };
    _init_accumulators(universal_schema);
    
    // Get PropertySchema IDs
    PropertySchemaID_vector *ps_oids = partition_cat->GetPropertySchemaIDs();
    PropertyToIdxUnorderedMap *property_to_idx_map = partition_cat->GetPropertyToIdxMap();

    // For each property schema, read data & accumulate histogram
    for (auto i = 0; i < ps_oids->size(); i++) {
        vector<idx_t> oids;
        vector<vector<LogicalType>> scan_types;
        vector<vector<idx_t>> scan_projection_mapping;
        vector<idx_t> target_cols_in_univ_schema;

        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, ps_oids->at(i));
        
        auto *prop_key_ids = ps_cat->GetPropKeyIDs();
        for (auto j = 0; j < prop_key_ids->size(); j++) {
            target_cols_in_univ_schema.push_back(property_to_idx_map->at(prop_key_ids->at(j)));
        }
        
        oids.push_back(ps_oids->at(i));
        scan_types.push_back(std::move(ps_cat->GetTypesWithCopy()));
        scan_projection_mapping.push_back(vector<idx_t>(scan_types[0].size()));
        std::iota(std::begin(scan_projection_mapping[0]), std::end(scan_projection_mapping[0]), 1);

        // Initialize ExtentIterators // TODO read only necessary columns
        auto initializeAPIResult =
		    client->graph_store->InitializeScan(ext_its, oids, scan_projection_mapping, scan_types);
        // Initialize DataChunk where data will be read
        DataChunk chunk;
        chunk.Initialize(scan_types[0]);
        
        StoreAPIResult res;

        while(true) {
            res = client->graph_store->doScan(ext_its, chunk, scan_types[0]);
            if (res == StoreAPIResult::DONE) { break; }

            icecream::ic.enable();
            IC(chunk.ToString(10));
            icecream::ic.disable();

            _accumulate_data(chunk, universal_schema, target_cols_in_univ_schema);
        }
    }

    // store histogram info in the partition catalog
    idx_t_vector *offset_infos = partition_cat->GetOffsetInfos();
    idx_t_vector *boundary_values = partition_cat->GetBoundaryValues();

    for (auto i = 0; i < universal_schema.size(); i++) {
        offset_infos->push_back(probs.size());
        for (auto j = 0; j < probs.size(); j++) {
            std::cout << j << ": " << quantile(*accms[i], quantile_probability = probs[j]) << std::endl;
            // boundary_values.push_back(extended_p_square(accms[i][j]));
        }
    }
}

void HistogramGenerator::_init_accumulators(vector<LogicalType> &universal_schema) {
    boost::array<double, 5> probs = {{0, 0.25, 0.5, 0.75, 1.00}}; // TODO need to define bin size
    // boost::array<double, 10> probs = { 0.990, 0.991, 0.992, 0.993, 0.994,
    //                            0.995, 0.996, 0.997, 0.998, 0.999 };

    for (auto i = 0; i < universal_schema.size(); i++) {
        if (universal_schema[i].IsNumeric()) {
            accumulator_set<int64_t, stats<tag::extended_p_square_quantile>> *acc =
                new accumulator_set<int64_t, stats<tag::extended_p_square_quantile>>(
                    extended_p_square_probabilities = probs // Quantiles for bin boundaries
                );
            accms.push_back(acc);
            target_cols.push_back(i);
        } else if (universal_schema[i] == LogicalType::VARCHAR) {
            // singletone histogram type for varchar type
        } else {
            accms.push_back(nullptr);
        }
    }
}

void HistogramGenerator::_accumulate_data(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema) {
    for (auto i = 0; i < target_cols_in_univ_schema.size(); i++) {
        auto &target_accm = *(accms[target_cols_in_univ_schema[i]]);
        auto &target_vec = chunk.data[i];

        switch(universal_schema[target_cols_in_univ_schema[i]].id()) {
            case LogicalTypeId::INTEGER: {
                int32_t *target_vec_data = (int32_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    target_accm(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::BIGINT: {
                int64_t *target_vec_data = (int64_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    target_accm(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::UINTEGER: {
                uint32_t *target_vec_data = (uint32_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    target_accm(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::UBIGINT: {
                uint64_t *target_vec_data = (uint64_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    target_accm(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::FLOAT: {
                float *target_vec_data = (float *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    target_accm(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::DOUBLE: {
                double *target_vec_data = (double *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    target_accm(target_vec_data[j]);
                }
                break;
            }
        }
    }
}

} // namespace duckdb