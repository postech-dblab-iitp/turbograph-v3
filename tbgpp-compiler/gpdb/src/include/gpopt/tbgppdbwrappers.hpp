#ifndef TBGPPDB_gdbwrappers_H
#define TBGPPDB_gdbwrappers_H

extern "C" {
#include "postgres.h"

// #include "access/attnum.h"
// #include "nodes/plannodes.h"
// #include "parser/parse_coerce.h"
// #include "utils/faultinjector.h"
#include "utils/lsyscache.h"
}

#include "enums/index_type.hpp"
#include "catalog/catalog_entry/list.hpp"

namespace duckdb {

// return the logical index type for a given logical index oid
IndexType GetLogicalIndexType(Oid index_oid);

// get relation with given oid
PropertySchemaCatalogEntry* GetRelation(idx_t rel_oid);

} // duckdb


#endif // !TBGPPDB_gdbwrappers_H

// EOF
