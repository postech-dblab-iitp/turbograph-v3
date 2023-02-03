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

    void GetSubPartitionIDs(ClientContext &context, vector<string> labelset_names, vector<idx_t> &oids) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(context, CatalogType::GRAPH_ENTRY, "main", "graph1");
        vector<idx_t> pids = gcat->LookupPartition(context, labelset_names, GraphComponentType::VERTEX); // TODO type?

        for (auto &pid : pids) {
            PartitionCatalogEntry *p_cat = (PartitionCatalogEntry *)catalog.GetEntry(context, "main", pid);
            p_cat->GetPropertySchemaIDs(oids);
        }
    }

    PropertySchemaCatalogEntry *RelationIdGetRelation(ClientContext &context, idx_t rel_oid) {
        auto &catalog = db.GetCatalog();
        PropertySchemaCatalogEntry *ps_cat = (PropertySchemaCatalogEntry *)catalog.GetEntry(context, "main", rel_oid);
        return ps_cat;
    }

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb