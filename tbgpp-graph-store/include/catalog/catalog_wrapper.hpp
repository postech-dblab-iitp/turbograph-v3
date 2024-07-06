#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "function/function.hpp"

#include "icecream.hpp"

#include <tuple>
#include <unordered_map>

namespace duckdb {

class CatalogWrapper {

public:
    CatalogWrapper(DatabaseInstance &db) : db(db) {}
    ~CatalogWrapper() {}

    void GetEdgeAndConnectedSrcDstPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs,
        vector<uint64_t> &srcPartitionIDs, vector<uint64_t> &dstPartitionIDs, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));

        for (auto &pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            srcPartitionIDs.push_back(p_cat->GetSrcPartOid());
            dstPartitionIDs.push_back(p_cat->GetDstPartOid());
        }
    }

    void GetSchemas(ClientContext &context, unordered_map<idx_t, vector<PropertyKeyID>>& TableOidsSchemas, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        vector<string> labelset_names;
        gcat->GetVertexLabels(labelset_names);

        for (auto &labelset_name : labelset_names) {
            vector<uint64_t> partitionIDs;
            vector<idx_t> oids;
            vector<string> partition_labelset_names = {labelset_name};
            partitionIDs = std::move(gcat->LookupPartition(context, partition_labelset_names, g_type));
            for (auto &pid : partitionIDs) {
                PartitionCatalogEntry *p_cat =
                    (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
                p_cat->GetPropertySchemaIDs(oids);
                for (auto &oid : oids) {
                    PropertySchemaCatalogEntry *ps_cat =
                    (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, oid);
                    auto key_ids_ptr = ps_cat->GetKeyIDs();
                    vector<PropertyKeyID> key_ids(key_ids_ptr->begin(), key_ids_ptr->end());
                    TableOidsSchemas.insert({oid, key_ids});
                }
            }
        }
    }

    void GetPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));
    }

    void GetSubPartitionIDsFromPartitions(ClientContext &context, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, idx_t &univTableID, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();

        for (auto &pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(oids);
            univTableID = p_cat->GetUnivPSOid();
        }
    }

    void GetSubPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));

        for (auto &pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(oids);
        }
    }

    void GetConnectedEdgeSubPartitionIDs(ClientContext &context, vector<idx_t> &src_oids, vector<uint64_t> &partitionIDs, vector<uint64_t> &dstPartitionIDs) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        for (auto &src_oid : src_oids) {
            gcat->GetConnectedEdgeOids(context, src_oid, partitionIDs);
        }

        for (auto &edge_pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, edge_pid);
            dstPartitionIDs.push_back(p_cat->GetDstPartOid());
        }
    }

    void GetConnectedEdgeSubPartitionIDs(ClientContext &context, vector<idx_t> &src_oids, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, vector<uint64_t> &dstPartitionIDs) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        for (auto &src_oid : src_oids) {
            gcat->GetConnectedEdgeOids(context, src_oid, partitionIDs);
        }

        for (auto &edge_pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, edge_pid);
            p_cat->GetPropertySchemaIDs(oids);
            dstPartitionIDs.push_back(p_cat->GetDstPartOid());
        }
    }

    idx_t GetAggFuncMdId(ClientContext &context, string &func_name, vector<LogicalType> &arguments) {
        auto &catalog = db.GetCatalog();
        AggregateFunctionCatalogEntry *aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, func_name, true);
        
        if (aggfunc_cat == nullptr) { throw InvalidInputException("Unsupported agg func name: " + func_name); }

        auto &agg_funcset = aggfunc_cat->functions->functions;
        D_ASSERT(agg_funcset.size() <= FUNC_GROUP_SIZE);
        std::string error_msg;
        idx_t agg_funcset_idx =
            Function::BindFunction(func_name, agg_funcset, arguments, error_msg);
        
        if (agg_funcset_idx == DConstants::INVALID_INDEX) { throw InvalidInputException("Unsupported agg func"); }

        idx_t aggfunc_mdid = FUNCTION_BASE_ID + (aggfunc_cat->GetOid() * FUNC_GROUP_SIZE) + agg_funcset_idx;
        return aggfunc_mdid;
    }
    
    idx_t GetScalarFuncMdId(ClientContext &context, string &func_name, vector<LogicalType> &arguments) {
        auto &catalog = db.GetCatalog();
        ScalarFunctionCatalogEntry *scalarfunc_cat =
            (ScalarFunctionCatalogEntry *)catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, func_name, true);

        if (scalarfunc_cat == nullptr) { throw InvalidInputException("Unsupported scalar func name: " + func_name); }

        auto &scalar_funcset = scalarfunc_cat->functions->functions;
        D_ASSERT(scalar_funcset.size() <= FUNC_GROUP_SIZE);
        std::string error_msg;
        idx_t scalar_funcset_idx =
            Function::BindFunction(func_name, scalar_funcset, arguments, error_msg);
            
        if (scalar_funcset_idx == DConstants::INVALID_INDEX) { throw InvalidInputException("Unsupported scalar func"); }
        idx_t scalarfunc_mdid = FUNCTION_BASE_ID + (scalarfunc_cat->GetOid() * FUNC_GROUP_SIZE) + scalar_funcset_idx;
        return scalarfunc_mdid;
    }

    PartitionCatalogEntry *GetPartition(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat;
    }

    PropertySchemaCatalogEntry *RelationIdGetRelation(ClientContext &context, idx_t rel_oid) {
        auto &catalog = db.GetCatalog();
        PropertySchemaCatalogEntry *ps_cat =
            (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, rel_oid);
        return ps_cat;
    }

    idx_t GetRelationPhysicalIDIndex(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat->GetPhysicalIDIndexOid();
    }

    idx_t_vector *GetRelationAdjIndexes(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat->GetAdjIndexOidVec();
    }

    idx_t_vector *GetRelationPropertyIndexes(ClientContext &context, idx_t partition_oid) {
        auto &catalog = db.GetCatalog();
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, partition_oid);
        return part_cat->GetPropertyIndexOidVec();
    }

    IndexCatalogEntry *GetIndex(ClientContext &context, idx_t index_oid) {
        auto &catalog = db.GetCatalog();
        IndexCatalogEntry *index_cat =
            (IndexCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, index_oid);
        return index_cat;
    }

    AggregateFunctionCatalogEntry *GetAggFunc(ClientContext &context, idx_t aggfunc_oid) {
        idx_t aggfunc_oid_ = (aggfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        AggregateFunctionCatalogEntry *aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, aggfunc_oid_);
        return aggfunc_cat;
    }

    void GetAggFuncAndIdx(ClientContext &context, idx_t aggfunc_oid, AggregateFunctionCatalogEntry *&aggfunc_cat,
        idx_t &function_idx) {
        idx_t aggfunc_oid_ = (aggfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        function_idx = (aggfunc_oid - FUNCTION_BASE_ID) % FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, aggfunc_oid_);
    }

    ScalarFunctionCatalogEntry *GetScalarFunc(ClientContext &context, idx_t scalarfunc_oid) {
        idx_t scalarfunc_oid_ = (scalarfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        ScalarFunctionCatalogEntry *scalarfunc_cat =
            (ScalarFunctionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, scalarfunc_oid_);
        return scalarfunc_cat;
    }

    void GetScalarFuncAndIdx(ClientContext &context, idx_t scalarfunc_oid, ScalarFunctionCatalogEntry *&scalarfunc_cat,
        idx_t &function_idx) {
        idx_t scalarfunc_oid_ = (scalarfunc_oid - FUNCTION_BASE_ID) / FUNC_GROUP_SIZE;
        function_idx = (scalarfunc_oid - FUNCTION_BASE_ID) % FUNC_GROUP_SIZE;
        auto &catalog = db.GetCatalog();
        scalarfunc_cat =
            (ScalarFunctionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, scalarfunc_oid_);
    }

    void GetPropertyKeyToPropertySchemaMap(
        ClientContext &context, vector<idx_t> &oids,
        unordered_map<string,
                      std::vector<std::tuple<idx_t, idx_t, LogicalTypeId>>>
            &pkey_to_ps_map,
        vector<string> &universal_schema)
    {
        auto &catalog = db.GetCatalog();

        for (auto &oid : oids) {
            PropertySchemaCatalogEntry *ps_cat =
                (PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, oid);

            string_vector *property_keys = ps_cat->GetKeys();
            LogicalTypeId_vector *property_key_types = ps_cat->GetTypes();
            for (int i = 0; i < property_keys->size(); i++) {
                if ((*property_key_types)[i] == LogicalType::FORWARD_ADJLIST ||
                    (*property_key_types)[i] == LogicalType::BACKWARD_ADJLIST)
                    continue;
                string property_key = std::string((*property_keys)[i]);
                auto it = pkey_to_ps_map.find(property_key);
                if (it == pkey_to_ps_map.end()) {
                    universal_schema.push_back(property_key);
                    pkey_to_ps_map.emplace(
                        property_key,
                        std::vector<std::tuple<idx_t, idx_t, LogicalTypeId>>{
                            std::make_tuple(oid, i + 1,
                                            (*property_key_types)[i])});
                }
                else {
                    it->second.push_back(
                        std::make_tuple(oid, i + 1, (*property_key_types)[i]));
                }
            }
        }
    }

    void GetPropertyKeyToPropertySchemaMap(
        ClientContext &context, vector<idx_t> &oids,
        PropertyToPropertySchemaPairVecUnorderedMap **property_schema_index,
        string_vector **universal_schema,
        idx_t_vector **universal_schema_ids,
        LogicalTypeId_vector **universal_types_id,
        vector<idx_t> &part_oids)
    {
        auto &catalog = db.GetCatalog();
        const void_allocator void_alloc = catalog.catalog_segment->get_segment_manager();

        // Allocate
        *property_schema_index = new PropertyToPropertySchemaPairVecUnorderedMap(void_alloc);
        *universal_schema = new string_vector(void_alloc);
        *universal_schema_ids = new idx_t_vector(void_alloc);
        *universal_types_id = new LogicalTypeId_vector(void_alloc);

        // Concat all property keys and types
        for (auto &part_oid: part_oids) {
            PartitionCatalogEntry *part_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, part_oid);
        
            auto part_universal_schema = part_cat->GetUniversalPropertyKeyNames();
            auto part_universal_schema_ids = part_cat->GetUniversalPropertyKeyIds();
            auto part_universal_types_id = part_cat->GetUniversalPropertyTypeIds();
            auto part_property_schema_index = part_cat->GetPropertySchemaIndex();

            // Merge
            for (auto i = 0; i < part_universal_schema->size(); i++) {
                duckdb::idx_t property_key_id = part_universal_schema_ids->at(i);
                auto it = (*property_schema_index)->find(property_key_id);
                if (it == (*property_schema_index)->end()) {
                    (*universal_schema)->push_back(part_universal_schema->at(i));
                    (*universal_schema_ids)->push_back(part_universal_schema_ids->at(i));
                    (*universal_types_id)->push_back(part_universal_types_id->at(i));
                    (*property_schema_index)->emplace(
                        property_key_id,
                        part_property_schema_index->find(property_key_id)->second);
                }
                else {
                    D_ASSERT("Same Column Name for Different Table is Not Implemented Yet");
                }
            }
        }
    }

    string GetTypeName(idx_t type_id) {
        return LogicalTypeIdToString((LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES));
    }

    idx_t GetTypeSize(idx_t type_id) {
        LogicalTypeId type_id_ = (LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);
        uint16_t extra_info = ((type_id - LOGICAL_TYPE_BASE_ID) / NUM_MAX_LOGICAL_TYPES);
        if (type_id_ == LogicalTypeId::DECIMAL) {
            uint8_t width = (uint8_t)(extra_info >> 8);
            uint8_t scale = (uint8_t)(extra_info & 0xFF);
            LogicalType tmp_type = LogicalType::DECIMAL(width, scale);
            return GetTypeIdSize(tmp_type.InternalType());
        } else {
            LogicalType tmp_type(type_id_);
            return GetTypeIdSize(tmp_type.InternalType());
        }
    }

    bool isTypeFixedLength(idx_t type_id) {
        LogicalType tmp_type((LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES));
        return TypeIsConstantSize(tmp_type.InternalType());
    }

    idx_t GetAggregate(ClientContext &context, const char *aggname, idx_t type_id, int nargs) {
        auto &catalog = db.GetCatalog();
        auto *func = (AggregateFunctionCatalogEntry *)catalog.GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, aggname);
        return func->GetOid();
    }

    idx_t GetComparisonOperator(idx_t left_type_id, idx_t right_type_id, ExpressionType etype) {
        return OPERATOR_BASE_ID
            + (((idx_t) etype) * (256 * 256))
            + (((left_type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES) * 256)
            + ((right_type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);
    }

    inline ExpressionType GetComparisonType(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        return etype;
    }

    inline idx_t GetLeftTypeId(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) % (256 * 256)) / 256;
    }

    inline idx_t GetRightTypeId(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) % (256));
    }

    string GetOpName(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        return ExpressionTypeToOrcaString(etype);
    }

    void GetOpInputTypes(idx_t op_id, idx_t &left_type_id, idx_t &right_type_id) {
        left_type_id = ((op_id - OPERATOR_BASE_ID) % (256 * 256)) / 256 + LOGICAL_TYPE_BASE_ID;
        right_type_id = ((op_id - OPERATOR_BASE_ID) % (256)) + LOGICAL_TYPE_BASE_ID;
    }

    idx_t GetOpFunc(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) / (256 * 256)) + EXPRESSION_TYPE_BASE_ID;
    }

    idx_t GetCommutatorOp(idx_t op_id) {
        idx_t left_type_id = GetLeftTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        idx_t right_type_id = GetRightTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        ExpressionType etype = GetComparisonType(op_id);
        return GetComparisonOperator(right_type_id, left_type_id, etype);
    }

    idx_t GetInverseOp(idx_t op_id) {
        idx_t left_type_id = GetLeftTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        idx_t right_type_id = GetRightTypeId(op_id) + LOGICAL_TYPE_BASE_ID;
        ExpressionType etype = GetComparisonType(op_id);
        ExpressionType inv_etype; // TODO get inv_etype using NegateComparisionExpression API
        switch (etype) {
            case ExpressionType::COMPARE_EQUAL:
                inv_etype = ExpressionType::COMPARE_NOTEQUAL;
                break;
            case ExpressionType::COMPARE_NOTEQUAL:
                inv_etype = ExpressionType::COMPARE_EQUAL;
                break;
            case ExpressionType::COMPARE_LESSTHAN:
                inv_etype = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
                break;
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                inv_etype = ExpressionType::COMPARE_GREATERTHAN;
                break;
            case ExpressionType::COMPARE_GREATERTHAN:
                inv_etype = ExpressionType::COMPARE_LESSTHANOREQUALTO;
                break;
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                inv_etype = ExpressionType::COMPARE_LESSTHAN;
                break;
            default:
                throw NotImplementedException("InverseOp is not implemented yet");
        }
        return GetComparisonOperator(left_type_id, right_type_id, inv_etype);
    }

    idx_t GetOpFamiliesForScOp(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        LogicalTypeId left_type_id = (LogicalTypeId) (((op_id - OPERATOR_BASE_ID) % (256 * 256)) / 256);
        LogicalTypeId right_type_id = (LogicalTypeId) ((op_id - OPERATOR_BASE_ID) % (256));
        idx_t left_type_physical_id = (idx_t) LogicalType(left_type_id).InternalType();
        idx_t right_type_physical_id = (idx_t) LogicalType(right_type_id).InternalType();

        return OPERATOR_FAMILY_BASE_ID
            + (((idx_t) etype) * (256 * 256))
            + (left_type_physical_id * 256)
            + (right_type_physical_id);
    }

    void _group_similar_tables(
        uint64_t num_cols, vector<idx_t> &property_locations,
        idx_t_vector *num_groups_for_each_column,
        idx_t_vector *group_info_for_each_table, idx_t_vector *multipliers,
        idx_t_vector *ps_oids, vector<idx_t> &table_oids,
        std::vector<std::vector<duckdb::idx_t>> &table_oids_in_group)
    {
        // TODO we need to develop a better algorithm to group similar tables
        if (true) {
            table_oids_in_group.push_back(table_oids);
        } else {
            // refer to group_info_for_each_table, group similar tables
            unordered_map<idx_t, vector<idx_t>> unique_key_to_oids_group;

            D_ASSERT(num_cols == num_groups_for_each_column->size());
            // D_ASSERT(group_info_for_each_table->size() ==
            //          table_oids.size() * num_groups_for_each_column->size());

            idx_t idx = 0;
            for (auto i = 0; i < ps_oids->size(); i++) {
                if ((*ps_oids)[i] != table_oids[idx]) { continue; }
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

    void _create_temporal_table_catalog(
        ClientContext &context, PartitionCatalogEntry *part_cat,
        vector<vector<duckdb::idx_t>> &table_oids_in_group,
        vector<idx_t> &representative_table_oids, PartitionID part_id,
        idx_t part_oid, vector<uint64_t> &property_key_ids,
        vector<vector<uint64_t>> &property_location_in_representative)
    {
        auto &catalog = db.GetCatalog();
        string part_name = part_cat->GetName();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(
            context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);

        property_location_in_representative.resize(table_oids_in_group.size());

        // create temporal table catalog for each group
        for (auto i = 0; i < table_oids_in_group.size(); i++) {
            auto &table_oids_to_be_merged = table_oids_in_group[i];
            if (table_oids_to_be_merged.size() == 1) {
                representative_table_oids.push_back(table_oids_to_be_merged[0]);
            }
            else {
                string property_schema_name =
                    part_name + DEFAULT_TEMPORAL_INFIX +
                    std::to_string(
                        part_cat->GetNewTemporalID());  // TODO vpart -> vps
                // std::cout << "temp schema: " << property_schema_name
                //           << std::endl;
                vector<LogicalType> merged_types;
                vector<PropertyKeyID> merged_property_key_ids;
                vector<string> key_names;

                // Create new Property Schema Catalog Entry
                CreatePropertySchemaInfo propertyschema_info(
                    DEFAULT_SCHEMA, property_schema_name.c_str(), part_id,
                    part_oid);
                PropertySchemaCatalogEntry *temporal_ps_cat =
                    (PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
                        context, &propertyschema_info);

                idx_t_vector *merged_offset_infos =
                    temporal_ps_cat->GetOffsetInfos();
                idx_t_vector *merged_freq_values =
                    temporal_ps_cat->GetFrequencyValues();
                uint64_t_vector *merged_ndvs =
                    temporal_ps_cat->GetNDVs();
                uint64_t merged_num_tuples = 0;

                // merge histogram & schema
                _merge_schemas_and_histograms(
                    context, table_oids_to_be_merged, merged_types,
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
                temporal_ps_cat->SetNumberOfLastExtentNumTuples(merged_num_tuples);

                representative_table_oids.push_back(temporal_ps_cat->GetOid());

                // update property_location_in_representative - may be inefficient
                for (auto j = 0; j < property_key_ids.size(); j++) {
                    auto it = std::find(merged_property_key_ids.begin(),
                                        merged_property_key_ids.end(),
                                        property_key_ids[j]);
                    D_ASSERT(it != merged_property_key_ids.end());
                    auto idx = std::distance(merged_property_key_ids.begin(), it);
                    property_location_in_representative[i].push_back(idx);
                }
            }
        }
    }

    void _merge_schemas_and_histograms(
        ClientContext &context, vector<idx_t> table_oids_to_be_merged,
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
            merged_num_tuples +=
                ps_cat->GetNumberOfRowsApproximately();

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

            if (!has_histogram) continue;

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
            merged_types.push_back(
                LogicalType(type_info.at(prop_key_id)));
            if (!has_histogram) continue;
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

    void ConvertTableOidsIntoRepresentativeOids(
        ClientContext &context, vector<uint64_t> &property_key_ids,
        vector<idx_t> &table_oids, vector<idx_t> &representative_table_oids,
        vector<vector<duckdb::idx_t>> &table_oids_in_group,
        vector<vector<uint64_t>> &property_location_in_representative)
    {
        // TODO currently, prop_exprs contains all properties for the specific label
        // we cannot prune table_oids that contains used_columns only in this phase
        // we should fix this..

        D_ASSERT(table_oids.size() > 0);

        // Get first property schema cat entry to find partition catalog
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

        // grouping similar (which has similar histogram) tables
        _group_similar_tables(num_cols, property_locations,
                              num_groups_for_each_column,
                              group_info_for_each_table, multipliers, ps_oids,
                              table_oids, table_oids_in_group);

        // create temporal catalog table
        // TODO check if we already built temporal table for the same groups
        _create_temporal_table_catalog(context, part_cat, table_oids_in_group,
                                       representative_table_oids, part_id,
                                       part_oid, property_key_ids,
                                       property_location_in_representative);
    }

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb