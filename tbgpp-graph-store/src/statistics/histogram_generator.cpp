#include "statistics/histogram_generator.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/types/data_chunk.hpp"

#include <boost/array.hpp>
#include <boost/histogram.hpp>
#include <numeric>
#include <random>
#include <ctime>

using namespace boost::accumulators;
namespace duckdb {

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client)
{
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

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client, string &part_name)
{
    Catalog &cat_instance = client->db->GetCatalog();

    // Get partition catalog
    PartitionCatalogEntry *partition_cat =
        (PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, part_name);
    
    _create_histogram(client, partition_cat);
    // _create_histogram_test(client, partition_cat);
}

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client, idx_t partition_oid)
{
    Catalog &cat_instance = client->db->GetCatalog();

    // Get partition catalog
    PartitionCatalogEntry *partition_cat =
        (PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, partition_oid);
    
    _create_histogram(client, partition_cat);
    // _create_histogram_test(client, partition_cat);
}

void HistogramGenerator::_create_histogram(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat)
{
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

        // Initialize ExtentIterators // TODO read only necessary columns // TODO using sampling technique?
        auto initializeAPIResult =
		    client->graph_store->InitializeScan(ext_its, oids, scan_projection_mapping, scan_types);

        // Initialize DataChunk where data will be read
        DataChunk chunk;
        chunk.Initialize(scan_types[0]);
        
        StoreAPIResult res;

        while(true) {
            res = client->graph_store->doScan(ext_its, chunk, scan_types[0]);
            if (res == StoreAPIResult::DONE) { break; }

            _accumulate_data(chunk, universal_schema, target_cols_in_univ_schema);
        }
    }

    // store histogram info in the partition catalog
    idx_t_vector *offset_infos = partition_cat->GetOffsetInfos();
    idx_t_vector *boundary_values = partition_cat->GetBoundaryValues();
    offset_infos->clear();
    boundary_values->clear();
    uint64_t accumulated_offset = 0;

    // initialize variables for histogram data
    vector<uint64_t> num_buckets_for_each_column;
    vector<vector<uint64_t>> frequency_values_for_each_column;
    frequency_values_for_each_column.resize(universal_schema.size());

    for (auto i = 0; i < universal_schema.size(); i++) {
        accumulated_offset += probs.size(); // TODO skip if not numeric
        offset_infos->push_back(accumulated_offset);
        for (auto j = 0; j < probs.size(); j++) {
            auto boundary_value = quantile(*accms[i], quantile_probability = probs[j]);
            boundary_values->push_back(boundary_value);
        }
        num_buckets_for_each_column.push_back(probs.size() - 1);
    }

    // calculate histogram for each property schema // TODO optimize this process
    for (auto i = 0; i < ps_oids->size(); i++) {
        vector<idx_t> oids;
        vector<vector<LogicalType>> scan_types;
        vector<vector<idx_t>> scan_projection_mapping;
        vector<idx_t> target_cols_in_univ_schema;
        vector<boost::histogram::histogram<std::tuple<boost::histogram::axis::variable<>>>> histograms;

        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, ps_oids->at(i));
        idx_t_vector *freq_offset_infos = ps_cat->GetOffsetInfos();
        idx_t_vector *frequency_values = ps_cat->GetFrequencyValues();
        freq_offset_infos->clear();
        frequency_values->clear();
        
        auto *prop_key_ids = ps_cat->GetPropKeyIDs();
        for (auto j = 0; j < prop_key_ids->size(); j++) {
            target_cols_in_univ_schema.push_back(property_to_idx_map->at(prop_key_ids->at(j)));
        }
        for (auto i = 0; i < target_cols_in_univ_schema.size(); i++) {
            auto target_col_idx = target_cols_in_univ_schema[i];
            auto begin_offset = target_col_idx == 0 ? 0 : offset_infos->at(target_col_idx - 1);
            auto end_offset = offset_infos->at(target_col_idx);
            auto num_boundaries = target_col_idx == 0 ? offset_infos->at(0) : offset_infos->at(target_col_idx) - offset_infos->at(target_col_idx - 1);
            std::vector<idx_t> boundaries;
            for (auto j = begin_offset; j < end_offset; j++) {
                boundaries.push_back(boundary_values->at(j));
            }
            auto v = boost::histogram::axis::variable<>(boundaries.begin(), boundaries.end());
            auto h = boost::histogram::make_histogram(v);
            histograms.emplace_back(std::move(h));
        }
        
        oids.push_back(ps_oids->at(i));
        scan_types.push_back(std::move(ps_cat->GetTypesWithCopy()));
        scan_projection_mapping.push_back(vector<idx_t>(scan_types[0].size()));
        std::iota(std::begin(scan_projection_mapping[0]), std::end(scan_projection_mapping[0]), 1);

        // Initialize ExtentIterators // TODO read only necessary columns // TODO using sampling technique?
        auto initializeAPIResult =
		    client->graph_store->InitializeScan(ext_its, oids, scan_projection_mapping, scan_types);

        // Initialize DataChunk where data will be read
        DataChunk chunk;
        chunk.Initialize(scan_types[0]);
        
        StoreAPIResult res;

        while(true) {
            res = client->graph_store->doScan(ext_its, chunk, scan_types[0]);
            if (res == StoreAPIResult::DONE) { break; }

            _create_bucket(chunk, universal_schema, target_cols_in_univ_schema, histograms);
        }

        idx_t col_idx = 0;
        for (auto i = 0; i < target_cols_in_univ_schema.size(); i++) {
            auto &h = histograms[i];
            auto target_col_idx = target_cols_in_univ_schema[i];
            auto begin_offset = target_col_idx == 0 ? 0 : offset_infos->at(target_col_idx - 1);
            auto end_offset = offset_infos->at(target_col_idx);
            auto num_boundaries = target_col_idx == 0 ? offset_infos->at(0) : offset_infos->at(target_col_idx) - offset_infos->at(target_col_idx - 1);
            while (col_idx < target_col_idx) {
                for (auto j = 0; j < num_buckets_for_each_column[col_idx]; j++) {
                    frequency_values_for_each_column[col_idx].push_back(0);
                }
                col_idx++;
            }
            D_ASSERT(num_boundaries - 1 == num_buckets_for_each_column[col_idx]);
            
            freq_offset_infos->push_back(num_boundaries - 1);
            for (auto j = 0; j < num_boundaries - 1; j++) {
                // std::cout << "[" << boundary_values->at(begin_offset + j) << ", " << boundary_values->at(begin_offset + j + 1) << ") : " << h.at(j) << std::endl;
                frequency_values->push_back(h.at(j));
                frequency_values_for_each_column[col_idx].push_back(h.at(j));
            }
            col_idx++;
        }
        while (col_idx < universal_schema.size()) {
            for (auto j = 0; j < num_buckets_for_each_column[col_idx]; j++) {
                frequency_values_for_each_column[col_idx].push_back(0);
            }
            col_idx++;
        }
    }
    
    for (auto i = 0; i < frequency_values_for_each_column.size(); i++) {
        std::cout << i << "-th column freq values: ";
        for (auto j = 0; j < frequency_values_for_each_column[i].size(); j++) {
            std::cout << frequency_values_for_each_column[i][j] << " ";
        }
        std::cout << std::endl;
    }
}

void HistogramGenerator::_create_histogram_test(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat)
{
    Catalog &cat_instance = client->db->GetCatalog();

    // Get universal schema
    vector<LogicalType> universal_schema = std::move(partition_cat->GetTypes());

    // Initialize accumulators for histogram
    boost::array<double, 5> probs = {{0, 0.25, 0.5, 0.75, 1.00}}; // TODO need to define bin size
    // boost::array<double, 10> probs = { 0.990, 0.991, 0.992, 0.993, 0.994,
    //                            0.995, 0.996, 0.997, 0.998, 0.999 };
    accumulator_set<int64_t, stats<tag::extended_p_square_quantile>> acc(extended_p_square_probabilities = probs);
    
    // std::mt19937 mt(std::time(nullptr));

    // for (auto i = 0; i < 1000000; i++) {
    //     acc(mt() % 1000);
    // }

    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::normal_distribution d{100.0, 5.0};
    auto random_int = [&d, &gen] { return std::round(d(gen)); };

    for (auto i = 0; i < 1000000; i++) {
        acc(random_int());
    }

    for (auto j = 0; j < probs.size(); j++) {
        std::cout << j << ": " << quantile(acc, quantile_probability = probs[j]) << std::endl;
        // boundary_values.push_back(extended_p_square(accms[i][j]));
    }
}

void HistogramGenerator::_init_accumulators(vector<LogicalType> &universal_schema)
{
    boost::array<double, 5> probs = {{0, 0.25, 0.5, 0.75, 1.00}}; // TODO need to define bin size
    // boost::array<double, 10> probs = { 0.990, 0.991, 0.992, 0.993, 0.994,
    //                            0.995, 0.996, 0.997, 0.998, 0.999 };

    for (auto i = 0; i < universal_schema.size(); i++) {
        if (universal_schema[i].IsNumeric()) {
            // TODO separate implementation for each types
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

void HistogramGenerator::_accumulate_data(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema)
{
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

void HistogramGenerator::_create_bucket(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema,
    vector<boost::histogram::histogram<std::tuple<boost::histogram::axis::variable<>>>> &histograms)
{
    for (auto i = 0; i < target_cols_in_univ_schema.size(); i++) {
        auto &h = histograms[i];
        auto &target_vec = chunk.data[i];

        switch(universal_schema[target_cols_in_univ_schema[i]].id()) {
            case LogicalTypeId::INTEGER: {
                int32_t *target_vec_data = (int32_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    h(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::BIGINT: {
                int64_t *target_vec_data = (int64_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    h(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::UINTEGER: {
                uint32_t *target_vec_data = (uint32_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    h(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::UBIGINT: {
                uint64_t *target_vec_data = (uint64_t *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    h(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::FLOAT: {
                float *target_vec_data = (float *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    h(target_vec_data[j]);
                }
                break;
            }
            case LogicalTypeId::DOUBLE: {
                double *target_vec_data = (double *)target_vec.GetData();
                for (auto j = 0; j < chunk.size(); j++) {
                    h(target_vec_data[j]);
                }
                break;
            }
        }
    }
}

void _generate_group_info(PartitionCatalogEntry *partition_cat, PropertySchemaID_vector *ps_oids,
        vector<uint64_t> &num_buckets_for_each_column, vector<vector<uint64_t>> &frequency_values_for_each_column)
{
    auto *num_groups = partition_cat->GetNumberOfGroups();
    auto *group_info = partition_cat->GetGroupInfo();
    auto *multipliers = partition_cat->GetMultipliers();

    num_groups->clear();
    group_info->clear();
    multipliers->clear();

    // TODO how to cluster?
    for (auto i = 0; i < num_buckets_for_each_column.size(); i++) {
        num_groups->push_back(1);
    }

    uint64_t accmulated_multipliers = 1;
    multipliers->push_back(0);
    for (auto i = 0; i < num_groups->size() - 1; i++) {
        accmulated_multipliers *= num_groups->at(i);
        multipliers->push_back(accmulated_multipliers);
    }

    // group by column // group by ps_oid is better?
    for (auto i = 0; i < num_buckets_for_each_column.size(); i++) {
        for (auto j = 0; j < ps_oids->size(); j++) {
            group_info->push_back(0);
        }
    }
}

} // namespace duckdb