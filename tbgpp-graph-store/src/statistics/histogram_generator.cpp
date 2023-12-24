#include "statistics/histogram_generator.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/types/data_chunk.hpp"

#include <boost/array.hpp>


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
    boost::array<double, 3> probs = {{0.25, 0.5, 0.75}}; // TODO need to define bin size
    _init_accumulators(universal_schema);
    
    // Get PropertySchema IDs
    PropertySchemaID_vector *ps_oids = partition_cat->GetPropertySchemaIDs();

    // Initialize ExtentIterators // TODO read only necessary columns
    vector<vector<idx_t>> scan_projection_mapping;
    vector<vector<LogicalType>> scan_types;

    for (auto i = 0; i < ps_oids->size(); i++) {
        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, ps_oids->at(i));
        scan_types.push_back(std::move(ps_cat->GetTypesWithCopy()));
    }

    auto initializeAPIResult =
		client->graph_store->InitializeScan(ext_its, ps_oids, scan_projection_mapping, scan_types);
    
    // Initialize DataChunk where data will be read
    DataChunk chunk;
    chunk.Initialize(universal_schema);
    
    StoreAPIResult res;

    while(true) {
        res = client->graph_store->doScan(ext_its, chunk, universal_schema);
        if (res == StoreAPIResult::DONE) { break; }

        _accumulate_data(chunk, universal_schema);
    }

    // store histogram info in the partition catalog
    idx_t_vector *offset_infos = partition_cat->GetOffsetInfos();
    idx_t_vector *boundary_values = partition_cat->GetBoundaryValues();

    for (auto i = 0; i < universal_schema.size(); i++) {
        offset_infos->push_back(probs.size());
        for (auto j = 0; j < probs.size(); j++) {
            boundary_values.push_back(extended_p_square(accms[i][j]));
        }
    }
}

void HistogramGenerator::_init_accumulators(vector<LogicalType> &universal_schema) {
    boost::array<double, 3> probs = {{0.25, 0.5, 0.75}}; // TODO need to define bin size

    for (auto i = 0; i < universal_schema.size(); i++) {
        if (universal_schema[i].IsNumeric()) {
            accumulator_set<double, stats<tag::extended_p_square_quantile>> *acc =
                new accumulator_set<double, stats<tag::extended_p_square_quantile>>(
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

void HistogramGenerator::_accumulate_data(DataChunk &chunk, vector<LogicalType> &universal_schema) {
    for (auto i = 0; i < target_cols.size(); i++) {
        auto &target_accm = *(accms[target_cols[i]]);
        auto &target_vec = chunk.data[target_cols[i]];

        switch(universal_schema[target_cols[i]].id()) {
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