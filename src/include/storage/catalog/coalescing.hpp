#pragma once

#include "storage/catalog/catalog.hpp"
#include "storage/catalog/catalog_entry/list.hpp"
#include "common/common.hpp"
#include "main/database.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "optimizer/mdprovider/MDProviderTBGPP.h"

namespace duckdb {

class Coalescing {
   public:
    static void do_coalescing(
        ClientContext &context, DatabaseInstance &db,
        vector<uint64_t> &property_key_ids, vector<idx_t> &table_oids,
        gpmd::MDProviderTBGPP *provider,
        vector<idx_t> &representative_table_oids,
        vector<vector<duckdb::idx_t>> &table_oids_in_group,
        vector<vector<uint64_t>> &property_location_in_representative,
        vector<bool> &is_each_group_has_temporary_table)
    {
        D_ASSERT(table_oids.size() > 0);
        // Get first property schema cat entry to get partition catalog
        // Currently, we do not support multi label (i.e. multi partition) case
        auto &catalog = db.GetCatalog();
        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, table_oids[0]);
        auto part_oid = ps_cat->GetPartitionOID();
        auto part_id = ps_cat->GetPartitionID();

        // Get partition catalog
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA,
                                                      part_oid);

        // Get property locations using property expressions & prop_to_idx map
        vector<idx_t> property_locations;
        auto *prop_to_idxmap = part_cat->GetPropertyToIdxMap();
        for (auto i = 0; i < property_key_ids.size(); i++) {
            D_ASSERT(prop_to_idxmap->find(property_key_ids[i]) !=
                     prop_to_idxmap->end());
            property_locations.push_back(
                prop_to_idxmap->at(property_key_ids[i]));
        }

        // TODO partition catalog should store tables groups for each column.
        // Tables in the same group have similar cardinality for the column
        idx_t_vector *num_groups_for_each_column =
            part_cat->GetNumberOfGroups();
        idx_t_vector *group_info_for_each_table = part_cat->GetGroupInfo();
        idx_t_vector *multipliers = part_cat->GetMultipliers();
        idx_t_vector *ps_oids = part_cat->GetPropertySchemaIDs();
        uint64_t num_cols = part_cat->GetNumberOfColumns();

        // // grouping similar (which has similar histogram) tables
        // _group_similar_tables_based_on_histogram(num_cols, property_locations,
        //                       num_groups_for_each_column,
        //                       group_info_for_each_table, multipliers, ps_oids,
        //                       table_oids, table_oids_in_group);

        _group_similar_tables_based_on_cardinality(context, db, table_oids,
                                                   table_oids_in_group);

        // create temporal catalog table
        // TODO check if we already built temporal table for the same groups
        _create_temporal_table_catalog(
            context, db, part_cat, provider, table_oids_in_group,
            representative_table_oids, part_id, part_oid, property_key_ids,
            property_location_in_representative,
            is_each_group_has_temporary_table);
    }

   private:
    enum class GroupingAlgorithm {
        DEFAULT,
        MERGEALL,
        SPLITALL
    };

    static GroupingAlgorithm grouping_algo;

    static void _group_similar_tables_based_on_histogram(
        uint64_t num_cols, vector<idx_t> &property_locations,
        idx_t_vector *num_groups_for_each_column,
        idx_t_vector *group_info_for_each_table, idx_t_vector *multipliers,
        idx_t_vector *ps_oids, vector<idx_t> &table_oids,
        std::vector<std::vector<duckdb::idx_t>> &table_oids_in_group)
    {
        // TODO we need to develop a better algorithm to group similar tables
        if (true) {
            table_oids_in_group.push_back(table_oids);
        }
        else {
            // refer to group_info_for_each_table, group similar tables
            unordered_map<idx_t, vector<idx_t>> unique_key_to_oids_group;

            D_ASSERT(num_cols == num_groups_for_each_column->size());
            // D_ASSERT(group_info_for_each_table->size() ==
            //          table_oids.size() * num_groups_for_each_column->size());

            idx_t idx = 0;
            for (auto i = 0; i < ps_oids->size(); i++) {
                if ((*ps_oids)[i] != table_oids[idx]) {
                    continue;
                }
                uint64_t unique_key = 0;
                idx_t base_offset = i * num_cols;
                for (auto j = 0; j < property_locations.size(); j++) {
                    idx_t col_idx = property_locations[j];
                    unique_key +=
                        group_info_for_each_table->at(base_offset + col_idx) *
                        multipliers->at(col_idx);
                }

                auto it = unique_key_to_oids_group.find(unique_key);
                if (it == unique_key_to_oids_group.end()) {
                    vector<idx_t> tmp_vec;
                    tmp_vec.push_back(table_oids[idx]);
                    unique_key_to_oids_group.insert(
                        {unique_key, std::move(tmp_vec)});
                }
                else {
                    it->second.push_back(table_oids[idx]);
                }
                idx++;
            }

            for (auto &it : unique_key_to_oids_group) {
                table_oids_in_group.push_back(std::move(it.second));
            }
        }
    }

    static void _group_similar_tables_based_on_cardinality(
        ClientContext &context, DatabaseInstance &db, vector<idx_t> &table_oids,
        std::vector<std::vector<duckdb::idx_t>> &table_oids_in_group)
    {
        // get cardinality for each graphlet
        auto &catalog = db.GetCatalog();
        vector<std::pair<idx_t, idx_t>> cardinality_for_each_gl;
        cardinality_for_each_gl.reserve(table_oids.size());
        for (auto i = 0; i < table_oids.size(); i++) {
            auto table_oid = table_oids[i];
            auto *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, table_oid);
            cardinality_for_each_gl.push_back(std::make_pair(
                table_oid, ps_cat->GetNumberOfRowsApproximately()));
        }

        // clustering according to the algorithm ?
        switch (grouping_algo) {
            case GroupingAlgorithm::MERGEALL:
            {
                table_oids_in_group.emplace_back();
                for (auto i = 0; i < table_oids.size(); i++) {
                    table_oids_in_group[0].push_back(table_oids[i]);
                }
                break;
            }
            case GroupingAlgorithm::SPLITALL:
            {
                for (auto i = 0; i < table_oids.size(); i++) {
                    table_oids_in_group.emplace_back();
                    table_oids_in_group[i].push_back(table_oids[i]);
                }
                break;
            }
            case GroupingAlgorithm::DEFAULT:
            default:
                grouping_default(cardinality_for_each_gl, table_oids_in_group);
                break;
        }
    }

    static void _create_temporal_table_catalog(
        ClientContext &context, DatabaseInstance &db,
        PartitionCatalogEntry *part_cat, gpmd::MDProviderTBGPP *provider,
        vector<vector<duckdb::idx_t>> &table_oids_in_group,
        vector<idx_t> &representative_table_oids, PartitionID part_id,
        idx_t part_oid, vector<uint64_t> &property_key_ids,
        vector<vector<uint64_t>> &property_location_in_representative,
        vector<bool> &is_each_group_has_temporary_table)
    {
        auto &catalog = db.GetCatalog();
        string part_name = part_cat->GetName();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(
            context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        property_location_in_representative.resize(table_oids_in_group.size());
        is_each_group_has_temporary_table.resize(table_oids_in_group.size(),
                                                 false);

        // create temporal table catalog for each group
        for (auto i = 0; i < table_oids_in_group.size(); i++) {
            auto &table_oids_to_be_merged = table_oids_in_group[i];
            if (table_oids_to_be_merged.size() == 1) {
                representative_table_oids.push_back(table_oids_to_be_merged[0]);
            }
            else {
                bool is_new_virtual_table = false;
                uint64_t virtual_table_oid = 0;
                PropertySchemaCatalogEntry *temporal_ps_cat = nullptr;
                vector<PropertyKeyID> merged_property_key_ids;

                if (provider != nullptr) {
                    is_new_virtual_table = !(provider->CheckVirtualTableExists(
                        table_oids_to_be_merged, virtual_table_oid));
                }

                if (is_new_virtual_table) {
                    string property_schema_name =
                        part_name + DEFAULT_TEMPORAL_INFIX +
                        std::to_string(
                            part_cat->GetNewTemporalID());  // TODO vpart -> vps
                    // std::cout << "temp schema: " << property_schema_name
                    //           << std::endl;
                    vector<LogicalType> merged_types;
                    
                    vector<string> key_names;

                    // Create new Property Schema Catalog Entry
                    CreatePropertySchemaInfo propertyschema_info(
                        DEFAULT_SCHEMA, property_schema_name.c_str(), part_id,
                        part_oid);
                    temporal_ps_cat =
                        (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
                            context, &propertyschema_info);

                    idx_t_vector *merged_offset_infos =
                        temporal_ps_cat->GetOffsetInfos();
                    idx_t_vector *merged_freq_values =
                        temporal_ps_cat->GetFrequencyValues();
                    uint64_t_vector *merged_ndvs = temporal_ps_cat->GetNDVs();
                    uint64_t merged_num_tuples = 0;

                    // merge histogram & schema
                    _merge_schemas_and_histograms(
                        context, db, table_oids_to_be_merged, merged_types,
                        merged_property_key_ids, merged_offset_infos,
                        merged_freq_values, merged_ndvs, merged_num_tuples);

                    // create physical id index catalog
                    CreateIndexInfo idx_info(DEFAULT_SCHEMA,
                                            property_schema_name + "_id",
                                            IndexType::PHYSICAL_ID, part_oid,
                                            temporal_ps_cat->GetOid(), 0, {-1});
                    IndexCatalogEntry *index_cat =
                        (IndexCatalogEntry *)catalog.CreateIndex(context,
                                                                &idx_info);

                    // for (auto j = 0; j < merged_property_key_ids.size(); j++) {
                    //     key_names.push_back("");
                    // }
                    gcat->GetPropertyNames(context, merged_property_key_ids,
                                        key_names);
                    temporal_ps_cat->SetFake();
                    temporal_ps_cat->SetSchema(context, key_names, merged_types,
                                            merged_property_key_ids);
                    temporal_ps_cat->SetPhysicalIDIndex(index_cat->GetOid());
                    temporal_ps_cat->SetNumberOfLastExtentNumTuples(
                        merged_num_tuples);

                    provider->AddVirtualTable(table_oids_to_be_merged,
                                              temporal_ps_cat->GetOid());
                } else {
                    temporal_ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(
                        context, DEFAULT_SCHEMA, virtual_table_oid);
                    D_ASSERT(temporal_ps_cat != nullptr);
                    D_ASSERT(temporal_ps_cat->IsFake());

                    auto *key_ids = temporal_ps_cat->GetKeyIDs();
                    merged_property_key_ids.reserve(key_ids->size());
                    for (auto i = 0; i < key_ids->size(); i++) {
                        merged_property_key_ids.push_back(key_ids->at(i));
                    }
                }

                representative_table_oids.push_back(temporal_ps_cat->GetOid());
                is_each_group_has_temporary_table[i] = true;

                // update property_location_in_representative - may be inefficient
                for (auto j = 0; j < property_key_ids.size(); j++) {
                    auto it = std::find(merged_property_key_ids.begin(),
                                        merged_property_key_ids.end(),
                                        property_key_ids[j]);
                    // D_ASSERT(it != merged_property_key_ids.end());
                    if (it != merged_property_key_ids.end()) {
                        auto idx =
                            std::distance(merged_property_key_ids.begin(), it);
                        property_location_in_representative[i].push_back(idx);
                    } else {
                        property_location_in_representative[i].push_back(
                            std::numeric_limits<uint64_t>::max()
                        );
                    }
                }
            }
        }
    }

    static void _create_temporal_partition_catalog(
        ClientContext &context, DatabaseInstance &db,
        vector<PartitionID> &part_ids, vector<idx_t> &part_oids,
        vector<idx_t> &table_oids,
        vector<vector<duckdb::idx_t>> &table_oids_in_group,
        vector<idx_t> &representative_table_oids,
        vector<uint64_t> &property_key_ids,
        vector<vector<uint64_t>> &property_location_in_representative)
    {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(
            context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        // Create new catalog
        string new_partition_name = DEFAULT_VERTEX_PARTITION_PREFIX;
        for (auto part_oid : part_oids) {
            PartitionCatalogEntry *part_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, part_oid);
            std::string part_name = part_cat->GetName();
            const std::string prefix = DEFAULT_VERTEX_PARTITION_PREFIX;
            if (part_name.compare(0, prefix.size(), prefix) == 0) {
                part_name = part_name.substr(prefix.size());
            }
            new_partition_name +=
                part_name + "_" + std::to_string(part_cat->GetNewTemporalID());
        }
        CreatePartitionInfo partition_info(DEFAULT_SCHEMA,
                                           new_partition_name.c_str());
        PartitionCatalogEntry *new_part_cat =
            (PartitionCatalogEntry *)catalog.CreatePartition(context,
                                                             &partition_info);
        PartitionID new_pid = gcat->GetNewPartitionID();
        auto new_part_oid = new_part_cat->GetOid();
        vector<string> new_label_set = {"TEMP_" + new_partition_name};
        gcat->AddVertexPartition(context, new_pid, new_part_cat->GetOid(),
                                 new_label_set);
        new_part_cat->SetPartitionID(new_pid);

        // Create merged infos
        D_ASSERT(table_oids_in_group.size() == 1);
        property_location_in_representative.resize(table_oids_in_group.size());
        for (auto i = 0; i < table_oids_in_group.size(); i++) {
            auto &table_oids_to_be_merged = table_oids_in_group[i];
            if (table_oids_to_be_merged.size() == 1) {
                representative_table_oids.push_back(table_oids_to_be_merged[0]);
            }
            else {
                string property_schema_name =
                    new_partition_name + DEFAULT_TEMPORAL_INFIX +
                    std::to_string(
                        new_part_cat->GetNewTemporalID());  // TODO vpart -> vps
                vector<LogicalType> merged_types;
                vector<PropertyKeyID> merged_property_key_ids;
                vector<string> key_names;

                // Create new Property Schema Catalog Entry
                CreatePropertySchemaInfo propertyschema_info(
                    DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid,
                    new_part_oid);
                PropertySchemaCatalogEntry *temporal_ps_cat =
                    (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
                        context, &propertyschema_info);

                // TODO optimize this function
                unordered_set<PropertyKeyID> merged_schema;
                unordered_map<PropertyKeyID, LogicalTypeId> type_info;
                uint64_t merged_num_tuples = 0;

                // merge schemas & histograms
                for (auto i = 0; i < table_oids_to_be_merged.size(); i++) {
                    idx_t table_oid = table_oids_to_be_merged[i];

                    PropertySchemaCatalogEntry *ps_cat =
                        (PropertySchemaCatalogEntry *)catalog.GetEntry(
                            context, DEFAULT_SCHEMA, table_oid);

                    auto *types = ps_cat->GetTypes();
                    auto *key_ids = ps_cat->GetKeyIDs();
                    merged_num_tuples += ps_cat->GetNumberOfRowsApproximately();

                    for (auto j = 0; j < key_ids->size(); j++) {
                        merged_schema.insert(key_ids->at(j));
                        if (type_info.find(key_ids->at(j)) == type_info.end()) {
                            type_info.insert({key_ids->at(j), types->at(j)});
                        }
                    }
                }

                merged_property_key_ids.reserve(merged_schema.size());
                for (auto it = merged_schema.begin(); it != merged_schema.end();
                     it++) {
                    merged_property_key_ids.push_back(*it);
                }

                // create physical id index catalog
                CreateIndexInfo idx_info(DEFAULT_SCHEMA,
                                         property_schema_name + "_id",
                                         IndexType::PHYSICAL_ID, new_part_oid,
                                         temporal_ps_cat->GetOid(), 0, {-1});
                IndexCatalogEntry *index_cat =
                    (IndexCatalogEntry *)catalog.CreateIndex(context,
                                                             &idx_info);
                auto *found_index_cat = (IndexCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, index_cat->GetOid());

                // TODO sort by key ids - always right?
                // std::sort(merged_property_key_ids.begin(), merged_property_key_ids.end());
                for (auto i = 0; i < merged_property_key_ids.size(); i++) {
                    idx_t prop_key_id = merged_property_key_ids[i];
                    merged_types.push_back(
                        LogicalType(type_info.at(prop_key_id)));
                }

                // for (auto j = 0; j < merged_property_key_ids.size(); j++) {
                //     key_names.push_back("");
                // }
                gcat->GetPropertyNames(context, merged_property_key_ids,
                                       key_names);
                new_part_cat->SetSchema(context, key_names, merged_types,
                                        merged_property_key_ids);
                new_part_cat->AddPropertySchema(context,
                                                temporal_ps_cat->GetOid(),
                                                merged_property_key_ids);
                temporal_ps_cat->SetFake();
                temporal_ps_cat->SetSchema(context, key_names, merged_types,
                                           merged_property_key_ids);
                temporal_ps_cat->SetPhysicalIDIndex(index_cat->GetOid());
                temporal_ps_cat->SetNumberOfLastExtentNumTuples(
                    merged_num_tuples);

                representative_table_oids.push_back(temporal_ps_cat->GetOid());

                // update property_location_in_representative - may be inefficient
                for (auto j = 0; j < property_key_ids.size(); j++) {
                    auto it = std::find(merged_property_key_ids.begin(),
                                        merged_property_key_ids.end(),
                                        property_key_ids[j]);
                    D_ASSERT(it != merged_property_key_ids.end());
                    auto idx =
                        std::distance(merged_property_key_ids.begin(), it);
                    property_location_in_representative[i].push_back(idx);
                }
            }
        }
    }

    static void _merge_schemas_and_histograms(
        ClientContext &context, DatabaseInstance &db,
        vector<idx_t> table_oids_to_be_merged,
        vector<LogicalType> &merged_types,
        vector<PropertyKeyID> &merged_property_key_ids,
        idx_t_vector *merged_offset_infos, idx_t_vector *merged_freq_values,
        uint64_t_vector *merged_ndvs, uint64_t &merged_num_tuples)
    {
        auto &catalog = db.GetCatalog();

        // TODO optimize this function
        unordered_set<PropertyKeyID> merged_schema;
        unordered_map<PropertyKeyID, LogicalTypeId> type_info;
        unordered_map<PropertyKeyID, vector<idx_t>>
            intermediate_merged_freq_values;
        unordered_map<PropertyKeyID, idx_t> accumulated_ndvs;
        idx_t accumulated_ndvs_for_physical_id_col = 0;

        bool has_histogram = true;

        // merge schemas & histograms
        for (auto i = 0; i < table_oids_to_be_merged.size(); i++) {
            idx_t table_oid = table_oids_to_be_merged[i];

            PropertySchemaCatalogEntry *ps_cat =
                (PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, table_oid);

            auto *types = ps_cat->GetTypes();
            auto *key_ids = ps_cat->GetKeyIDs();
            auto *ndvs = ps_cat->GetNDVs();
            accumulated_ndvs_for_physical_id_col +=
                ps_cat->GetNumberOfRowsApproximately();
            merged_num_tuples += ps_cat->GetNumberOfRowsApproximately();

            has_histogram = has_histogram && ndvs->size() > 0;

            /**
             * TODO: adding NDVs is seems wrong.
             * We have to do correctly (e.g., setting max NDV)
            */

            for (auto j = 0; j < key_ids->size(); j++) {
                merged_schema.insert(key_ids->at(j));
                if (type_info.find(key_ids->at(j)) == type_info.end()) {
                    type_info.insert({key_ids->at(j), types->at(j)});
                }
                if (has_histogram) {
                    if (accumulated_ndvs.find(key_ids->at(j)) ==
                        accumulated_ndvs.end()) {
                        accumulated_ndvs.insert({key_ids->at(j), ndvs->at(j)});
                    }
                    else {
                        accumulated_ndvs[key_ids->at(j)] += ndvs->at(j);
                    }
                }
            }

            if (!has_histogram)
                continue;

            auto *offset_infos = ps_cat->GetOffsetInfos();
            auto *freq_values = ps_cat->GetFrequencyValues();

            for (auto j = 0; j < key_ids->size(); j++) {
                auto it = intermediate_merged_freq_values.find(key_ids->at(j));
                if (it == intermediate_merged_freq_values.end()) {
                    vector<idx_t> tmp_vec;
                    auto begin_offset = j == 0 ? 0 : offset_infos->at(j - 1);
                    auto end_offset = offset_infos->at(j);
                    auto freq_begin_offset = j == 0 ? 0 : begin_offset - (j);
                    auto freq_end_offset = end_offset - (j + 1);
                    for (auto k = freq_begin_offset; k < freq_end_offset; k++) {
                        tmp_vec.push_back(freq_values->at(k));
                    }
                    intermediate_merged_freq_values.insert(
                        {key_ids->at(j), std::move(tmp_vec)});
                }
                else {
                    auto &freq_vec = it->second;
                    auto begin_offset = j == 0 ? 0 : offset_infos->at(j - 1);
                    auto end_offset = offset_infos->at(j);
                    auto freq_begin_offset = j == 0 ? 0 : begin_offset - (j);
                    auto freq_end_offset = end_offset - (j + 1);
                    for (auto k = freq_begin_offset; k < freq_end_offset; k++) {
                        freq_vec[k - freq_begin_offset] += freq_values->at(k);
                    }
                }
            }
        }

        merged_property_key_ids.reserve(merged_schema.size());
        if (has_histogram) {
            merged_ndvs->push_back(accumulated_ndvs_for_physical_id_col);
        }
        for (auto it = merged_schema.begin(); it != merged_schema.end(); it++) {
            merged_property_key_ids.push_back(*it);
        }
        // TODO sort by key ids - always right?
        // std::sort(merged_property_key_ids.begin(), merged_property_key_ids.end());
        size_t accumulated_offset = 0;
        for (auto i = 0; i < merged_property_key_ids.size(); i++) {
            idx_t prop_key_id = merged_property_key_ids[i];
            merged_types.push_back(LogicalType(type_info.at(prop_key_id)));
            if (!has_histogram)
                continue;
            auto &freq_vec = intermediate_merged_freq_values.at(prop_key_id);
            accumulated_offset += (freq_vec.size() + 1);
            merged_offset_infos->push_back(accumulated_offset);
            for (auto j = 0; j < freq_vec.size(); j++) {
                merged_freq_values->push_back(freq_vec[j]);
            }
            auto &ndv = accumulated_ndvs.at(prop_key_id);
            merged_ndvs->push_back(ndv);
        }
    }

    static void grouping_default(
        vector<std::pair<idx_t, idx_t>> &cardinality_for_each_gl,
        std::vector<std::vector<duckdb::idx_t>> &table_oids_in_group)
    {
        std::sort(cardinality_for_each_gl.begin(),
                  cardinality_for_each_gl.end(),
                  [&](const std::pair<idx_t, idx_t> &lhs,
                      const std::pair<idx_t, idx_t> &rhs) {
                      return lhs.second < rhs.second;
                  });

        idx_t group_idx = 0, begin_idx = 0;
        idx_t smallest_card = cardinality_for_each_gl[0].second;
        table_oids_in_group.resize(1);
        for (auto i = 0; i < cardinality_for_each_gl.size(); i++) {
            // std::cout << "card of " << cardinality_for_each_gl[i].first
            //     << " : " << cardinality_for_each_gl[i].second << std::endl;
            if (cardinality_for_each_gl[i].second > 1.3 * smallest_card) {
                // flush
                for (auto j = begin_idx; j < i; j++) {
                    table_oids_in_group[group_idx].push_back(
                        cardinality_for_each_gl[j].first);
                }
                begin_idx = i;
                smallest_card = cardinality_for_each_gl[i].second;
                table_oids_in_group.push_back(std::vector<idx_t>{});
                group_idx++;
            }
        }

        // flush remaining
        for (auto j = begin_idx; j < cardinality_for_each_gl.size(); j++) {
            table_oids_in_group[group_idx].push_back(
                cardinality_for_each_gl[j].first);
        }

        // debugging
        // for (auto i = 0; i < table_oids_in_group.size(); i++) {
        //     std::cout << "group " << i << " : ";
        //     for (auto j = 0; j < table_oids_in_group[i].size(); j++) {
        //         std::cout << table_oids_in_group[i][j] << ", ";
        //     }
        //     std::cout << std::endl;
        // }
    }
};

}  // namespace duckdb