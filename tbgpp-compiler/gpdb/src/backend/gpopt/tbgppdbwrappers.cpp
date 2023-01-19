#include "tbgppdbwrappers.h"

using namespace duckdb;

IndexType
duckdb::GetLogicalIndexType(Oid index_oid)
{
    // TODO we don't have index catalog yet
	// GP_WRAP_START;
	// {
	// 	/* catalog tables: pg_index */
	// 	return logicalIndexTypeForIndexOid(index_oid);
	// }
	// GP_WRAP_END;
	return IndexType::ART;
}