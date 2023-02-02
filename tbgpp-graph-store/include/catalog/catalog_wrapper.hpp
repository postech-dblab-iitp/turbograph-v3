#pragma once

#include "main/database.hpp"
#include "common/common.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"

namespace duckdb {

class CatalogWrapper {

public:
    CatalogWrapper(DatabaseInstance &db) : db(db) {}
    ~CatalogWrapper() {}

    void GetSubPartitionIDs(ClientContext &context, vector<string> labelset_names) {
        auto &catalog = db.GetCatalog();
        GraphCatalogEntry *gcat = (GraphCatalogEntry *)catalog.GetEntry(context, "main", "graph1");
        vector<idx_t> oids = gcat->LookupPartition(context, labelset_names, GraphComponentType::VERTEX); // TODO type?

        for (auto &oid : oids) {
            PartitionCatalogEntry *p_cat = (PartitionCatalogEntry *)catalog.GetEntry(context, "main", oid);
        }
        // for (auto &labelset : labelset_names) {
        //     PartitionCatalogEntry *p_cat = (PartitionCatalogEntry *)catalog.GetEntry(context, "main", "");
        // }
    }

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb