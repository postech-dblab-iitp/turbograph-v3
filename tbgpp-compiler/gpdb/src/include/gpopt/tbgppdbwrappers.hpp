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

class ClientContext;
class CatalogWrapper;

void SetClientWrapper(shared_ptr<ClientContext> client_, shared_ptr<CatalogWrapper> catalog_wrapper_);

// return the logical index type for a given logical index oid
IndexType GetLogicalIndexType(Oid index_oid);

// get catalog informations with given oid
PartitionCatalogEntry *GetPartition(idx_t partition_oid);
PropertySchemaCatalogEntry *GetRelation(idx_t rel_oid);
idx_t GetRelationPhysicalIDIndex(idx_t partition_oid);
idx_t_vector *GetRelationAdjIndexes(idx_t partition_oid);
idx_t_vector *GetRelationPropertyIndexes(idx_t partition_oid);
IndexCatalogEntry *GetIndex(idx_t index_oid);

// get type related informations
string GetTypeName(idx_t type_id);
idx_t GetTypeSize(idx_t type_id);
bool isTypeFixedLength(idx_t type_id);
idx_t GetAggregate(const char *aggname, idx_t type_id, int nargs);
idx_t GetComparisonOperator(idx_t left_type_id, idx_t right_type_id, CmpType cmpt);
CmpType GetComparisonType(idx_t op_id);
string GetOpName(idx_t op_id);
void GetOpInputTypes(idx_t op_oid, uint32_t *left_type_id, uint32_t *right_type_id);
idx_t GetOpFunc(idx_t op_id);
idx_t GetCommutatorOp(idx_t op_id);
idx_t GetInverseOp(idx_t op_id);

static shared_ptr<ClientContext> client_wrapper;
static shared_ptr<CatalogWrapper> catalog_wrapper;

} // duckdb


#endif // !TBGPPDB_gdbwrappers_H

// EOF
