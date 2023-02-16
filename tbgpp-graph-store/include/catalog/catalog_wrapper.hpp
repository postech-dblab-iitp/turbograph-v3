#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"

#include "icecream.hpp"

namespace duckdb {

class CatalogWrapper {

public:
    CatalogWrapper(DatabaseInstance &db) : db(db) {}
    ~CatalogWrapper() {}

    void GetSubPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<idx_t> &oids, GraphComponentType g_type) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, "main", "graph1");
        vector<idx_t> pids = std::move(gcat->LookupPartition(context, labelset_names, g_type));

        for (auto &pid : pids) {
            PartitionCatalogEntry *p_cat = (PartitionCatalogEntry *)catalog.GetEntry(context, "main", pid); // TODO main -> DefaultSchema
            p_cat->GetPropertySchemaIDs(oids);
        }
    }

    PropertySchemaCatalogEntry *RelationIdGetRelation(ClientContext &context, idx_t rel_oid) {
        auto &catalog = db.GetCatalog();
        PropertySchemaCatalogEntry *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(context, "main", rel_oid);
        return ps_cat;
    }

    void GetPropertyKeyToPropertySchemaMap(ClientContext &context, vector<idx_t> &oids, unordered_map<string, vector<idx_t>> &pkey_to_ps_map) {
        auto &catalog = db.GetCatalog();
        for (auto &oid : oids) {
            PropertySchemaCatalogEntry *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(context, "main", oid);

            string_vector *property_keys = ps_cat->GetKeys();
            LogicalTypeId_vector *property_key_types = ps_cat->GetTypes();
            for (int i = 0; i < property_keys->size(); i++) {
                if ((*property_key_types)[i] == LogicalType::FORWARD_ADJLIST || 
                    (*property_key_types)[i] == LogicalType::BACKWARD_ADJLIST) continue;
                string property_key = std::string((*property_keys)[i]);
                auto it = pkey_to_ps_map.find(property_key);
                if (it == pkey_to_ps_map.end()) {
                    pkey_to_ps_map.emplace(property_key, std::vector<idx_t> {oid});
                } else {
                    it->second.push_back(oid);
                }
            }
        }
    }

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb