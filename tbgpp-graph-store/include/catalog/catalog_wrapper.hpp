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
        icecream::ic.enable(); IC(); icecream::ic.disable();
        oids = gcat->LookupPartition(context, labelset_names, GraphComponentType::VERTEX); // TODO type?
        icecream::ic.enable(); IC(); icecream::ic.disable();

        // for (auto &oid : oids) {
        //     PartitionCatalogEntry *p_cat = (PartitionCatalogEntry *)catalog.GetEntry(context, "main", oid);
        // }
    }

private:
    //! Reference to the database
	DatabaseInstance &db;
};

} // namespace duckdb