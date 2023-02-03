#include "tbgppdbwrappers.hpp"
#include "utils/tbgppcache.hpp"
#include "client_context.hpp"
#include "catalog/catalog_wrapper.hpp"
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

void
duckdb::SetClientWrapper(shared_ptr<ClientContext> client_, shared_ptr<CatalogWrapper> catalog_wrapper_) {
	client_wrapper = client_;
	catalog_wrapper = catalog_wrapper_;
}

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
		return catalog_wrapper->RelationIdGetRelation(*client_wrapper.get(), rel_oid);
	}
	// GP_WRAP_END;
	return NULL;
}