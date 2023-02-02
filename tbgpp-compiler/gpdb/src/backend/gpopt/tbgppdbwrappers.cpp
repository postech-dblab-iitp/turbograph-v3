#include "tbgppdbwrappers.hpp"
#include "utils/tbgppcache.hpp"
// #include <setjmp.h>

// #define GP_WRAP_START                                            \
// 	sigjmp_buf local_sigjmp_buf;                                 \
// 	{                                                            \
// 		CAutoExceptionStack aes((void **) &PG_exception_stack,   \
// 								(void **) &error_context_stack); \
// 		if (0 == sigsetjmp(local_sigjmp_buf, 0))                 \
// 		{                                                        \
// 			aes.SetLocalJmp(&local_sigjmp_buf)

// #define GP_WRAP_END                                        \
// 	}                                                      \
// 	else                                                   \
// 	{                                                      \
// 		GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError); \
// 	}                                                      \
// 	}

using namespace duckdb;

bool assert_enabled = true; // TODO

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

duckdb::PropertySchemaCatalogEntry*
duckdb::GetRelation(duckdb::idx_t rel_oid)
{
	// GP_WRAP_START;
	{
		/* catalog tables: relcache */
		return RelationIdGetRelation(rel_oid);
	}
	// GP_WRAP_END;
	return NULL;
}