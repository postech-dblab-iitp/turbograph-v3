#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "function/aggregate/distributive_functions.hpp"

#include "icecream.hpp"

#include <tuple>

namespace duckdb {

class CatalogWrapper {

public:
    CatalogWrapper(DatabaseInstance &db) : db(db) {}
    ~CatalogWrapper() {}

    void GetSubPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<idx_t> &oids, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, "graph1");
        vector<idx_t> pids = std::move(gcat->LookupPartition(context, labelset_names, g_type));

        for (auto &pid : pids) {
            PartitionCatalogEntry *p_cat =
                (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(oids);
        }
    }

    idx_t GetAggFuncMdId(ClientContext &context, string &func_name, vector<LogicalType> &arguments) {
        auto &catalog = db.GetCatalog();
        AggregateFunctionCatalogEntry *aggfunc_cat =
            (AggregateFunctionCatalogEntry *)catalog.GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, DEFAULT_SCHEMA, func_name, true);
        
        if (aggfunc_cat == nullptr) { throw InvalidInputException("Unsupported agg func"); }

        auto &agg_funcset = aggfunc_cat->functions->functions;
        D_ASSERT(agg_funcset.size() <= FUNC_GROUP_SIZE);
        idx_t agg_funcset_idx = 0; bool found = false;
        for (int idx = 0; idx < agg_funcset.size(); idx++) {
            if (arguments.size() != agg_funcset[idx].arguments.size()) continue;
            if (arguments == agg_funcset[idx].arguments) {
                found = true;
                agg_funcset_idx = idx;
                break;
            }
        }
        if (!found) { throw InvalidInputException("Unsupported agg func"); }

        idx_t aggfunc_mdid = FUNCTION_BASE_ID + (aggfunc_cat->GetOid() * FUNC_GROUP_SIZE) + agg_funcset_idx;
        return aggfunc_mdid;
    }
    
    idx_t GetScalarFuncMdId(ClientContext &context, string &func_name, vector<LogicalType> &arguments) {

        return 0;
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
        return LogicalTypeIdToString((LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID));
    }

    idx_t GetTypeSize(idx_t type_id) {
        LogicalType tmp_type((LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID));
        return GetTypeIdSize(tmp_type.InternalType());
    }

    bool isTypeFixedLength(idx_t type_id) {
        LogicalType tmp_type((LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID));
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
            + ((left_type_id - LOGICAL_TYPE_BASE_ID) * 256)
            + ((right_type_id - LOGICAL_TYPE_BASE_ID));
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