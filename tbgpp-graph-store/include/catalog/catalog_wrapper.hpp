#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "function/aggregate/distributive_functions.hpp"

#include "icecream.hpp"

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
            PartitionCatalogEntry *p_cat = (PartitionCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, pid);
            p_cat->GetPropertySchemaIDs(oids);
        }
    }

    PropertySchemaCatalogEntry *RelationIdGetRelation(ClientContext &context, idx_t rel_oid) {
        auto &catalog = db.GetCatalog();
        PropertySchemaCatalogEntry *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(context, DEFAULT_SCHEMA, rel_oid);
        return ps_cat;
    }

    void GetPropertyKeyToPropertySchemaMap(ClientContext &context, vector<idx_t> &oids, unordered_map<string, vector<pair<idx_t, idx_t>>> &pkey_to_ps_map) {
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
                    pkey_to_ps_map.emplace(property_key, std::vector<pair<idx_t, idx_t>> {std::make_pair(oid, i + 1)});
                } else {
                    it->second.push_back(std::make_pair(oid, i + 1));
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

    ExpressionType GetComparisonType(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        return etype;
    }

    string GetOpName(idx_t op_id) {
        ExpressionType etype = (ExpressionType) ((op_id - OPERATOR_BASE_ID) / (256 * 256));
        return ExpressionTypeToString(etype);
    }

    void GetOpInputTypes(idx_t op_oid, idx_t &left_type_id, idx_t &right_type_id) {
        left_type_id = ((op_oid - OPERATOR_BASE_ID) % (256 * 256)) / 256;
        right_type_id = ((op_oid - OPERATOR_BASE_ID) % (256));
    }

    idx_t GetOpFunc(idx_t op_id) {
        return ((op_id - OPERATOR_BASE_ID) / (256 * 256)) + EXPRESSION_TYPE_BASE_ID;
    }

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb