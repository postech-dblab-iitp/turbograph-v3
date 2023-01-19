#ifndef TBGPPDB_gdbwrappers_H
#define TBGPPDB_gdbwrappers_H

#include "enums/index_type.hpp"

typedef unsigned int Oid; // TODO temporary

namespace duckdb {

// return the logical index type for a given logical index oid
IndexType GetLogicalIndexType(Oid index_oid);

} // duckdb


#endif // !TBGPPDB_gdbwrappers_H

// EOF
