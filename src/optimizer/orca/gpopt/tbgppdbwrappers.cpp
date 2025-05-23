#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "main/client_context.hpp"
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

bool assert_enabled = true;  // TODO

void duckdb::SetClientWrapper(shared_ptr<ClientContext> client_,
                              shared_ptr<CatalogWrapper> catalog_wrapper_)
{
    client_wrapper = client_;
    catalog_wrapper = catalog_wrapper_;
}

void duckdb::ReleaseClientWrapper()
{
    client_wrapper.reset();
    catalog_wrapper.reset();
}

IndexType duckdb::GetLogicalIndexType(Oid index_oid)
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

duckdb::PartitionCatalogEntry *duckdb::GetPartition(idx_t partition_oid)
{
    return catalog_wrapper->GetPartition(*client_wrapper.get(), partition_oid);
}

duckdb::PropertySchemaCatalogEntry *duckdb::GetRelation(idx_t rel_oid)
{
    // GP_WRAP_START;
    {
        /* catalog tables: relcache */
        return catalog_wrapper->RelationIdGetRelation(*client_wrapper.get(),
                                                      rel_oid);
    }
    // GP_WRAP_END;
    return NULL;
}

duckdb::AggregateFunctionCatalogEntry *duckdb::GetAggFunc(idx_t aggfunc_oid)
{
    return catalog_wrapper->GetAggFunc(*client_wrapper.get(), aggfunc_oid);
}

duckdb::ScalarFunctionCatalogEntry *duckdb::GetScalarFunc(idx_t scalarfunc_oid)
{
    return catalog_wrapper->GetScalarFunc(*client_wrapper.get(),
                                          scalarfunc_oid);
}

idx_t duckdb::GetAggFuncIndex(idx_t aggfunc_oid)
{
    return (aggfunc_oid - FUNCTION_BASE_ID) % 65536;
}

idx_t duckdb::GetScalarFuncIndex(idx_t scalarfunc_oid)
{
    return (scalarfunc_oid - FUNCTION_BASE_ID) % 65536;
}

void duckdb::GetHistogramInfo(PropertySchemaCatalogEntry *rel, int16_t attno,
                              AttStatsSlot *hist_slot)
{
    auto *offset_infos = rel->GetOffsetInfos();
    auto *frequency_values = rel->GetFrequencyValues();
    auto type = LogicalType((*rel->GetTypes())[attno - 1]);
    // if (!type.IsNumeric()) {
    // TODO type checking
    if (!(type == LogicalType::INTEGER || type == LogicalType::BIGINT ||
          type == LogicalType::UINTEGER || type == LogicalType::UBIGINT ||
          type == LogicalType::FLOAT || type == LogicalType::DOUBLE ||
          type == LogicalType::DATE)) {
        // we have no histogram for non-numeric types
        hist_slot->valuetype = InvalidOid;
        return;
    }
    if (offset_infos->size() == 0) {
        // there is no histogram
        hist_slot->valuetype = InvalidOid;
        return;
    }
    else {
        idx_t num_buckets = attno == 1 ? (*offset_infos)[0] - 1
                                       : (*offset_infos)[attno - 1] -
                                             (*offset_infos)[attno - 2] - 1;
        idx_t begin_offset = attno == 1 ? 0 : (*offset_infos)[attno - 2];
        idx_t end_offset = (*offset_infos)[attno - 1];
        idx_t freq_begin_offset = attno == 1 ? 0 : begin_offset - (attno - 1);
        idx_t freq_end_offset = end_offset - attno;

        if (num_buckets == 0) {
            // there is no histogram for this column
            hist_slot->valuetype = InvalidOid;
            return;
        }

        PartitionCatalogEntry *part_cat = GetPartition(rel->GetPartitionOID());
        auto *boundary_values = part_cat->GetBoundaryValues();

        // get valuetype
        hist_slot->valuetype =
            (Oid)((*rel->GetTypes())[attno - 1]) + LOGICAL_TYPE_BASE_ID;

        // get nvalues
        hist_slot->nvalues = num_buckets;

        // get histogram boundary values
        hist_slot->values = new Datum[num_buckets + 1];
        for (auto i = begin_offset; i < end_offset; i++) {
            hist_slot->values[i - begin_offset] = (Datum)(*boundary_values)[i];
        }

        // get histogram frequencies
        hist_slot->freq_values = new Datum[num_buckets];
        for (auto i = freq_begin_offset; i < freq_end_offset; i++) {
            hist_slot->freq_values[i - freq_begin_offset] =
                (Datum)(*frequency_values)[i];
        }
    }
}

double duckdb::GetNDV(PropertySchemaCatalogEntry *rel, int16_t attno)
{
    auto *ndvs = rel->GetNDVs();
    if (ndvs->size() == 0) {
        return 0.0;
    }
    if (attno < 0) {
        // load ID col ndv
        return (*ndvs)[0];
    }
    else {
        // load other col ndv
        return (*ndvs)[attno];
    }
}

idx_t duckdb::GetRelationPhysicalIDIndex(idx_t partition_oid)
{
    return catalog_wrapper->GetRelationPhysicalIDIndex(*client_wrapper.get(),
                                                       partition_oid);
}

idx_t_vector *duckdb::GetRelationAdjIndexes(idx_t partition_oid)
{
    return catalog_wrapper->GetRelationAdjIndexes(*client_wrapper.get(),
                                                  partition_oid);
}

idx_t_vector *duckdb::GetRelationPropertyIndexes(idx_t partition_oid)
{
    return catalog_wrapper->GetRelationPropertyIndexes(*client_wrapper.get(),
                                                       partition_oid);
}

IndexCatalogEntry *duckdb::GetIndex(idx_t index_oid)
{
    return catalog_wrapper->GetIndex(*client_wrapper.get(), index_oid);
}

string duckdb::GetTypeName(idx_t type_id)
{
    return catalog_wrapper->GetTypeName(type_id);
}

idx_t duckdb::GetTypeSize(idx_t type_id)
{
    return catalog_wrapper->GetTypeSize(type_id);
}

bool duckdb::isTypeFixedLength(idx_t type_id)
{
    return catalog_wrapper->isTypeFixedLength(type_id);
}

idx_t duckdb::GetAggregate(const char *aggname, idx_t type_id, int nargs)
{
    return catalog_wrapper->GetAggregate(*client_wrapper.get(), aggname,
                                         type_id, nargs);
}

idx_t duckdb::GetComparisonOperator(idx_t left_type_id, idx_t right_type_id,
                                    CmpType cmpt)
{
    switch (cmpt) {
        case CmptEq:
            return catalog_wrapper->GetComparisonOperator(
                left_type_id, right_type_id, ExpressionType::COMPARE_EQUAL);
        case CmptNEq:
            return catalog_wrapper->GetComparisonOperator(
                left_type_id, right_type_id, ExpressionType::COMPARE_NOTEQUAL);
        case CmptLT:
            return catalog_wrapper->GetComparisonOperator(
                left_type_id, right_type_id, ExpressionType::COMPARE_LESSTHAN);
        case CmptLEq:
            return catalog_wrapper->GetComparisonOperator(
                left_type_id, right_type_id,
                ExpressionType::COMPARE_LESSTHANOREQUALTO);
        case CmptGT:
            return catalog_wrapper->GetComparisonOperator(
                left_type_id, right_type_id,
                ExpressionType::COMPARE_GREATERTHAN);
        case CmptGEq:
            return catalog_wrapper->GetComparisonOperator(
                left_type_id, right_type_id,
                ExpressionType::COMPARE_GREATERTHANOREQUALTO);
        default:
            // TODO throw invalid type exception
            break;
    }
    return InvalidOid;
}

CmpType duckdb::GetComparisonType(idx_t op_id)
{
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
    return CmptOther;  // TODO invalid
}

void duckdb::GetOpInputTypes(idx_t op_oid, uint32_t *left_type_id,
                             uint32_t *right_type_id)
{
    idx_t left_tid, right_tid;

    catalog_wrapper->GetOpInputTypes(op_oid, left_tid, right_tid);
    D_ASSERT(left_tid <= std::numeric_limits<uint32_t>::max());
    D_ASSERT(right_tid <= std::numeric_limits<uint32_t>::max());

    *left_type_id = (uint32_t)left_tid;
    *right_type_id = (uint32_t)right_tid;
}

string duckdb::GetOpName(idx_t op_id)
{
    return catalog_wrapper->GetOpName(op_id);
}

idx_t duckdb::GetOpFunc(idx_t op_id)
{
    return catalog_wrapper->GetOpFunc(op_id);
}

idx_t duckdb::GetCommutatorOp(idx_t op_id)
{
    return catalog_wrapper->GetCommutatorOp(op_id);
}

idx_t duckdb::GetInverseOp(idx_t op_id)
{
    return catalog_wrapper->GetInverseOp(op_id);
}

idx_t duckdb::GetOpFamiliesForScOp(idx_t op_id)
{
    return catalog_wrapper->GetOpFamiliesForScOp(op_id);
}

idx_t duckdb::AddVirtualTable(uint32_t original_vtbl_oid, uint32_t *oid_array,
                              idx_t size)
{
    return catalog_wrapper->AddVirtualTable(*client_wrapper.get(),
                                            original_vtbl_oid, oid_array, size);
}
