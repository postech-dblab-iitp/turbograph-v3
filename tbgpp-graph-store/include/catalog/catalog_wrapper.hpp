#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "function/aggregate/distributive_functions.hpp"
#include "function/function.hpp"

#include "icecream.hpp"

#include <tuple>

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

    void GetPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<uint64_t> &partitionIDs, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat =
            (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        partitionIDs = std::move(gcat->LookupPartition(context, labelset_names, g_type));
    }

    void GetSubPartitionIDsFromPartitions(ClientContext &context, vector<uint64_t> &partitionIDs, vector<idx_t> &oids, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();

        for (auto &pid : partitionIDs) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(oids);
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

    void GetPropertyKeyToPropertySchemaMap(ClientContext &context, vector<idx_t> &oids, unordered_map<string, std::vector<std::tuple<idx_t, idx_t, LogicalTypeId> >> &pkey_to_ps_map, vector<string> &universal_schema) {
        auto &catalog = db.GetCatalog();
        for (auto &oid : oids) {
            PropertySchemaCatalogEntry *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, oid);

            string_vector *property_keys = ps_cat->GetKeys();
            LogicalTypeId_vector *property_key_types = ps_cat->GetTypes();
            for (int i = 0; i < property_keys->size(); i++) {
                if ((*property_key_types)[i] == LogicalType::FORWARD_ADJLIST || 
                    (*property_key_types)[i] == LogicalType::BACKWARD_ADJLIST) continue;
                string property_key = std::string((*property_keys)[i]);
                auto it = pkey_to_ps_map.find(property_key);
                if (it == pkey_to_ps_map.end()) {
                    universal_schema.push_back(property_key);
                    pkey_to_ps_map.emplace(property_key, std::vector<std::tuple<idx_t, idx_t, LogicalTypeId>>{std::make_tuple(oid, i + 1, (*property_key_types)[i])} );
                } else {
                    it->second.push_back(std::make_tuple(oid, i + 1, (*property_key_types)[i]));
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
        return ExpressionTypeToString(etype);
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

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb