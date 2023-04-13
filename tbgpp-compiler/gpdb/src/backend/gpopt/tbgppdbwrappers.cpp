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

duckdb::PartitionCatalogEntry*
duckdb::GetPartition(idx_t partition_oid) {
	return catalog_wrapper->GetPartition(*client_wrapper.get(), partition_oid);
}

duckdb::PropertySchemaCatalogEntry*
duckdb::GetRelation(idx_t rel_oid) {
	// GP_WRAP_START;
	{
		/* catalog tables: relcache */
		return catalog_wrapper->RelationIdGetRelation(*client_wrapper.get(), rel_oid);
	}
	// GP_WRAP_END;
	return NULL;
}

duckdb::AggregateFunctionCatalogEntry*
duckdb::GetAggFunc(idx_t aggfunc_oid) {
	return catalog_wrapper->GetAggFunc(*client_wrapper.get(), aggfunc_oid);
}

idx_t
duckdb::GetAggFuncIndex(idx_t aggfunc_oid) {
	return (aggfunc_oid - FUNCTION_BASE_ID) % 65536;
}

idx_t
duckdb::GetRelationPhysicalIDIndex(idx_t partition_oid) {
	return catalog_wrapper->GetRelationPhysicalIDIndex(*client_wrapper.get(), partition_oid);
}

idx_t_vector*
duckdb::GetRelationAdjIndexes(idx_t partition_oid) {
	return catalog_wrapper->GetRelationAdjIndexes(*client_wrapper.get(), partition_oid);
}

idx_t_vector*
duckdb::GetRelationPropertyIndexes(idx_t partition_oid) {
	return catalog_wrapper->GetRelationPropertyIndexes(*client_wrapper.get(), partition_oid);
}

IndexCatalogEntry*
duckdb::GetIndex(idx_t index_oid) {
	return catalog_wrapper->GetIndex(*client_wrapper.get(), index_oid);
}

string
duckdb::GetTypeName(idx_t type_id) {
	return catalog_wrapper->GetTypeName(type_id);
}

idx_t
duckdb::GetTypeSize(idx_t type_id) {
	return catalog_wrapper->GetTypeSize(type_id);
}

bool
duckdb::isTypeFixedLength(idx_t type_id) {
	return catalog_wrapper->isTypeFixedLength(type_id);
}

idx_t
duckdb::GetAggregate(const char *aggname, idx_t type_id, int nargs) {
	return catalog_wrapper->GetAggregate(*client_wrapper.get(), aggname, type_id, nargs);
}

idx_t 
duckdb::GetComparisonOperator(idx_t left_type_id, idx_t right_type_id, CmpType cmpt) {
	switch(cmpt) {
		case CmptEq:
			return catalog_wrapper->GetComparisonOperator(left_type_id, right_type_id, ExpressionType::COMPARE_EQUAL);
		case CmptNEq:
			return catalog_wrapper->GetComparisonOperator(left_type_id, right_type_id, ExpressionType::COMPARE_NOTEQUAL);
		case CmptLT:
			return catalog_wrapper->GetComparisonOperator(left_type_id, right_type_id, ExpressionType::COMPARE_LESSTHAN);
		case CmptLEq:
			return catalog_wrapper->GetComparisonOperator(left_type_id, right_type_id, ExpressionType::COMPARE_LESSTHANOREQUALTO);
		case CmptGT:
			return catalog_wrapper->GetComparisonOperator(left_type_id, right_type_id, ExpressionType::COMPARE_GREATERTHAN);
		case CmptGEq:
			return catalog_wrapper->GetComparisonOperator(left_type_id, right_type_id, ExpressionType::COMPARE_GREATERTHANOREQUALTO);
		default:
			// TODO throw invalid type exception
			break;
	}
	return InvalidOid;
}

CmpType 
duckdb::GetComparisonType(idx_t op_id) {
	ExpressionType etype = catalog_wrapper->GetComparisonType(op_id);
	switch (etype) {
		case ExpressionType::COMPARE_EQUAL:
			return CmptEq;
		case ExpressionType::COMPARE_NOTEQUAL:
			return CmptNEq;
		case ExpressionType::COMPARE_LESSTHAN:
			return CmptLT;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return CmptLEq;
		case ExpressionType::COMPARE_GREATERTHAN:
			return CmptGT;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return CmptGEq;
		default:
			D_ASSERT(false);
			break;
	}
	return CmptOther; // TODO invalid
}

void 
duckdb::GetOpInputTypes(idx_t op_oid, uint32_t *left_type_id, uint32_t *right_type_id) {
	idx_t left_tid, right_tid;

	catalog_wrapper->GetOpInputTypes(op_oid, left_tid, right_tid);
	D_ASSERT(left_tid <= std::numeric_limits<uint32_t>::max());
	D_ASSERT(right_tid <= std::numeric_limits<uint32_t>::max());

	*left_type_id = (uint32_t) left_tid;
	*right_type_id = (uint32_t) right_tid;
}

string
duckdb::GetOpName(idx_t op_id) {
	return catalog_wrapper->GetOpName(op_id);
}

idx_t
duckdb::GetOpFunc(idx_t op_id) {
	return catalog_wrapper->GetOpFunc(op_id);
}

idx_t 
duckdb::GetCommutatorOp(idx_t op_id) {
	return catalog_wrapper->GetCommutatorOp(op_id);
}

idx_t
duckdb::GetInverseOp(idx_t op_id) {
	return catalog_wrapper->GetInverseOp(op_id);
}

idx_t
duckdb::GetOpFamiliesForScOp(idx_t op_id) {
	return catalog_wrapper->GetOpFamiliesForScOp(op_id);
}