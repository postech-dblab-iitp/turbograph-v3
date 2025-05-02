#include <boost/array.hpp>
#include <boost/histogram.hpp>
#include <numeric>
#include <random>
#include <ctime>

#include "main/client_context.hpp"
#include "main/database.hpp"

#include "common/types/data_chunk.hpp"
#include "common/logger.hpp"

#include "storage/statistics/histogram_generator.hpp"
#include "storage/statistics/clustering/clique.hpp"
#include "storage/statistics/clustering/dummy.hpp"

using namespace boost::accumulators;
namespace duckdb {

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client)
{
    spdlog::debug("[CreateHistogram] Start creating histograms for all partitions");

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
    
    spdlog::debug("[CreateHistogram] Create Histogram for partition {}", partition_cat->GetName());
    
    _create_histogram(client, partition_cat);
}

void HistogramGenerator::CreateHistogram(std::shared_ptr<ClientContext> client, idx_t partition_oid)
{
    Catalog &cat_instance = client->db->GetCatalog();

    // Get partition catalog
    PartitionCatalogEntry *partition_cat =
        (PartitionCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, partition_oid);
    
    spdlog::debug("[CreateHistogram] Create Histogram for partition {}", partition_cat->GetName());

    if (partition_cat->GetName() == "vpart_NODE") {
        spdlog::debug("[CreateHistogram] Skip for vpart_NODE");
        return;
    }
    
    _create_histogram(client, partition_cat);
}

void HistogramGenerator::_create_histogram(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat)
{
    Catalog &cat_instance = client->db->GetCatalog();

    // Get universal schema
    vector<LogicalType> universal_schema = std::move(partition_cat->GetTypes());

    // Initialize accumulators for histogram
    std::vector<uint64_t> bin_sizes;
    std::vector<std::vector<double>> probs_per_column;
    _calculate_bin_sizes(client, partition_cat, bin_sizes);
    _calculate_bin_boundaries(probs_per_column, bin_sizes);
    _init_accumulators(universal_schema, probs_per_column);
    
    // Get PropertySchema IDs
    PropertySchemaID_vector *ps_oids = partition_cat->GetPropertySchemaIDs();
    PropertyToIdxUnorderedMap *property_to_idx_map = partition_cat->GetPropertyToIdxMap();

    if (ps_oids->size() > 1) {
        spdlog::info("[_create_histogram] Skip for multi-schema partition (to be supported)");
        return;
    }

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
		    client->graph_storage_wrapper->InitializeScan(ext_its, oids, scan_projection_mapping, scan_types);

        // Initialize DataChunk where data will be read
        DataChunk chunk;
        chunk.Initialize(scan_types[0]);
        
        StoreAPIResult res;
        std::vector<std::unordered_set<uint64_t>> ndv_counters(scan_types[0].size());
        size_t num_total_tuples = 0;

        while(true) {
            res = client->graph_storage_wrapper->doScan(ext_its, chunk, scan_types[0]);
            if (res == StoreAPIResult::DONE) { break; }

            _accumulate_data_for_hist(chunk, universal_schema, target_cols_in_univ_schema);
            _accumulate_data_for_ndv(chunk, scan_types[0], ndv_counters, num_total_tuples);
        }

        _store_ndv(ps_cat, scan_types[0], ndv_counters, num_total_tuples);
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
        auto& probs = probs_per_column[i];
        accumulated_offset += probs.size();
        offset_infos->push_back(accumulated_offset);
        for (auto j = 0; j < probs.size(); j++) {
            if (!universal_schema[i].IsNumeric()) {
                boundary_values->push_back(0);
            } else {
                auto boundary_value = quantile(*accms[i], quantile_probability = probs[j]);
                if (boundary_value <= 0.0) {
                    boundary_values->push_back(0);
                } else {
                    boundary_values->push_back(boundary_value);
                }
            }
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
                // TODO bug "input sequence must be strictly ascending" occur
                if (j == begin_offset) {
                    boundaries.push_back(boundary_values->at(j));
                } else {
                    if (boundaries.back() >= boundary_values->at(j)) {
                        boundaries.push_back(boundaries.back() + 1);
                    } else {
                        boundaries.push_back(boundary_values->at(j));
                    }
                }
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
		    client->graph_storage_wrapper->InitializeScan(ext_its, oids, scan_projection_mapping, scan_types);

        // Initialize DataChunk where data will be read
        DataChunk chunk;
        chunk.Initialize(scan_types[0]);
        
        StoreAPIResult res;

        while(true) {
            res = client->graph_storage_wrapper->doScan(ext_its, chunk, scan_types[0]);
            if (res == StoreAPIResult::DONE) { break; }

            _create_bucket(chunk, universal_schema, target_cols_in_univ_schema, histograms);
        }

        idx_t col_idx = 0;
        accumulated_offset = 0;
        for (auto i = 0; i < target_cols_in_univ_schema.size(); i++) {
            auto &h = histograms[i];
            auto target_col_idx = target_cols_in_univ_schema[i];
            auto begin_offset = target_col_idx == 0 ? 0 : offset_infos->at(target_col_idx - 1);
            auto end_offset = offset_infos->at(target_col_idx);
            auto num_boundaries = target_col_idx == 0 ? offset_infos->at(0) : offset_infos->at(target_col_idx) - offset_infos->at(target_col_idx - 1);
            // while (col_idx < target_col_idx) {
            //     for (auto j = 0; j < num_buckets_for_each_column[col_idx]; j++) {
            //         frequency_values_for_each_column[col_idx].push_back(0);
            //     }
            //     col_idx++;
            // }
            D_ASSERT(num_boundaries - 1 == num_buckets_for_each_column[col_idx]);
            
            accumulated_offset += (num_boundaries);
            freq_offset_infos->push_back(accumulated_offset);
            for (auto j = 0; j < num_boundaries - 1; j++) {
                frequency_values->push_back(h.at(j));
                frequency_values_for_each_column[target_col_idx].push_back(h.at(j));
            }
            // col_idx++;
        }
    }
}

void HistogramGenerator::_init_accumulators(vector<LogicalType> &universal_schema, std::vector<std::vector<double>>& probs_per_column) {
    _clear_accms();
    target_cols.clear();
    for (auto i = 0; i < universal_schema.size(); i++) {
        if (universal_schema[i].IsNumeric() || universal_schema[i] == LogicalType::DATE) {
            // Use the dynamic probs array
            accumulator_set<int64_t, stats<tag::extended_p_square_quantile>> *acc =
                new accumulator_set<int64_t, stats<tag::extended_p_square_quantile>>(
                    extended_p_square_probabilities = probs_per_column[i]
                );
            accms.push_back(acc);
            target_cols.push_back(i);
        } else if (universal_schema[i] == LogicalType::VARCHAR) {
            // singletone histogram type for varchar type
            accms.push_back(nullptr);
        } else {
            accms.push_back(nullptr);
        }
    }
}


void HistogramGenerator::_accumulate_data_for_hist(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema)
{
    for (auto i = 0; i < target_cols_in_univ_schema.size(); i++) {
        if (accms[target_cols_in_univ_schema[i]] == nullptr) continue;
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
            case LogicalTypeId::DATE: {
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
            case LogicalTypeId::DATE: {
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

void HistogramGenerator::_generate_group_info(PartitionCatalogEntry *partition_cat, PropertySchemaID_vector *ps_oids,
        vector<uint64_t> &num_buckets_for_each_column, vector<vector<uint64_t>> &frequency_values_for_each_column)
{
    auto *num_groups = partition_cat->GetNumberOfGroups();
    auto *group_info = partition_cat->GetGroupInfo();
    auto *multipliers = partition_cat->GetMultipliers();

    num_groups->clear();
    group_info->clear();
    multipliers->clear();

    // group by column // group by ps_oid is better?
    auto num_cols = num_buckets_for_each_column.size();
    group_info->resize(num_cols * ps_oids->size());
    for (auto i = 0; i < num_cols; i++) {
        uint64_t num_groups_for_this_column;
        vector<uint64_t> group_info_for_this_column;
        _cluster_column<CliqueClustering>(ps_oids->size(), num_buckets_for_each_column[i], frequency_values_for_each_column[i], num_groups_for_this_column, group_info_for_this_column);
        
        // print num_groups_for_this_column and group_info_for_this_column
        // std::cout << i << "-th column num_groups: " << num_groups_for_this_column << std::endl;
        // std::cout << i << "-th column group_info: ";
        // for (auto j = 0; j < group_info_for_this_column.size(); j++) {
        //     std::cout << group_info_for_this_column[j] << " ";
        // }
        // std::cout << std::endl;

        num_groups->push_back(num_groups_for_this_column);
        for (auto j = 0; j < group_info_for_this_column.size(); j++) {
            group_info->at(num_cols * j + i) = group_info_for_this_column[j];
        }
    }

    uint64_t accmulated_multipliers = 1;
    multipliers->push_back(1);
    for (auto i = 0; i < num_groups->size() - 1; i++) {
        accmulated_multipliers *= num_groups->at(i);
        multipliers->push_back(accmulated_multipliers);
    }
}

void HistogramGenerator::_calculate_bin_boundaries(std::vector<std::vector<double>>& probs_per_column, vector<uint64_t>& bin_sizes) {
    probs_per_column.resize(bin_sizes.size());
    for (int i = 0; i < bin_sizes.size(); ++i) {
        probs_per_column[i].resize(bin_sizes[i] + 1);
        for (int j = 0; j <= bin_sizes[i]; ++j) {
            probs_per_column[i][j] = static_cast<double>(j) / bin_sizes[i];
        }
    }
}

void HistogramGenerator::_calculate_bin_sizes(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat, vector<uint64_t>& bin_sizes) {
    Catalog &cat_instance = client->db->GetCatalog();
    auto num_properties = partition_cat->GetTypes().size();
    PropertySchemaID_vector *ps_oids = partition_cat->GetPropertySchemaIDs();
    PropertyToIdxUnorderedMap *property_to_idx_map = partition_cat->GetPropertyToIdxMap();

    // calculate accumulated number of rows for each column
    vector<uint64_t> acc_num_rows(num_properties, 0);
    bin_sizes.resize(num_properties);
    for (auto i = 0; i < ps_oids->size(); i++) {
        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)cat_instance.GetEntry(*client.get(), DEFAULT_SCHEMA, ps_oids->at(i));
        
        auto ps_size = ps_cat->GetNumberOfRowsApproximately();
        auto *prop_key_ids = ps_cat->GetPropKeyIDs();
        for (auto j = 0; j < prop_key_ids->size(); j++) {
            auto target_col_idx = property_to_idx_map->at(prop_key_ids->at(j));
            acc_num_rows[target_col_idx] += ps_size;
        }
    }

    // calculate bin size for each column
    for (auto i = 0; i < num_properties; i++) {
        bin_sizes[i] = _calculate_bin_size(acc_num_rows[i], BinningMethod::CONST);
    }
}


uint64_t HistogramGenerator::_calculate_bin_size(uint64_t num_rows, BinningMethod method) {
    // see https://glamp.github.io/blog/posts/histograms-in-postgres/
    // see How many bins should be put in a regular histogram
    // see https://en.wikipedia.org/wiki/Histogram#Number_of_bins_and_width
    // We only support methods without data scanning. Other methods requires standard deviation, which requires data scanning.
    switch (method) {
        case BinningMethod::SQRT:
            return static_cast<uint64_t>(std::sqrt(num_rows));
        case BinningMethod::STURGES: //log-2 of n + 1
            return static_cast<uint64_t>(std::log2(num_rows) + 1);
        case BinningMethod::RICE:
            return static_cast<uint64_t>(2 * std::pow(num_rows, 1.0 / 3.0));
        case BinningMethod::SCOTT:
        {
            auto min = 0.0;
            auto max = 0.0;
            auto mean = 0.0;
            auto std = 0.0;
            auto range = max - min;
            return static_cast<uint64_t>(range / (3.5 * std::pow(num_rows, 1.0 / 3.0)));
        }
        case BinningMethod::CONST:
            return 10;
        default:
            throw std::runtime_error("Unknown binning method");
    }
}


void HistogramGenerator::_accumulate_data_for_ndv(DataChunk& chunk, vector<LogicalType> types, std::vector<std::unordered_set<uint64_t>>& ndv_counters, size_t& num_total_tuples) {
    num_total_tuples += chunk.size();
    for (auto i = 0; i < chunk.size(); i++) {
        for (auto j = 0; j < chunk.ColumnCount(); j++) {
            auto &target_set = ndv_counters[j];
            auto &target_vec = chunk.data[j];
            auto target_value = target_vec.GetValue(i);
            switch(types[j].id()) {
                case LogicalTypeId::INTEGER:{
                    target_set.insert(target_value.GetValue<int32_t>());
                    break;
                }
                case LogicalTypeId::DATE:{
                    target_set.insert(target_value.GetValue<int32_t>());
                    break;
                }
                case LogicalTypeId::BIGINT:{
                    target_set.insert(target_value.GetValue<int64_t>());
                    break;
                }
                case LogicalTypeId::UINTEGER:{
                    target_set.insert(target_value.GetValue<uint32_t>());
                    break;
                }
                case LogicalTypeId::UBIGINT: {
                    target_set.insert(target_value.GetValue<uint64_t>());
                    break;
                }
            }
        }
    }
}

void HistogramGenerator::_store_ndv(PropertySchemaCatalogEntry *ps_cat, vector<LogicalType> types, std::vector<std::unordered_set<uint64_t>>& ndv_counters, size_t& num_total_tuples) {
    auto ndvs = ps_cat->GetNDVs();
    ndvs->clear();
    ndvs->push_back(num_total_tuples); // ID column
    for (auto i = 0; i < types.size(); i++) {
        auto &target_set = ndv_counters[i];
        switch(types[i].id()) {
            case LogicalTypeId::INTEGER:
            case LogicalTypeId::DATE:
            case LogicalTypeId::BIGINT:
            case LogicalTypeId::UINTEGER:
            case LogicalTypeId::UBIGINT: {
                ndvs->push_back(target_set.size());
                break;
            }
            default: {
                ndvs->push_back(num_total_tuples);
                break;
            }
        }
    }
    D_ASSERT(ndvs->size() == types.size() + 1);
}

} // namespace duckdb