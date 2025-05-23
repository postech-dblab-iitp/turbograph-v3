//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorTBGPPToDXL.cpp
//
//	@doc:
//		Class translating relcache entries into DXL objects
//
//	@test:
//
//
//---------------------------------------------------------------------------

extern "C" {
// #include "optimizer/orca/postgres.h"

// #include "access/heapam.h"
// #include "catalog/namespace.h"
// #include "catalog/pg_exttable.h"
// #include "catalog/pg_proc.h"
// #include "catalog/pg_statistic.h"
// #include "cdb/cdbhash.h"
// #include "optimizer/orca/cdb/cdbpartition.h"
// #include "utils/array.h"
// #include "utils/datum.h"
// #include "optimizer/orca/utils/elog.h"
// #include "utils/guc.h"
// #include "optimizer/orca/utils/lsyscache.h"
// #include "utils/relcache.h"
// #include "optimizer/orca/utils/syscache.h"
#include "optimizer/orca/utils/typcache.h"
}
#include "optimizer/orca/utils/tbgpp_rel.hpp"

#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/io/COstreamString.h"

#include "gpopt/base/CUtils.h"
// #include "optimizer/orca/gpopt/gpdbwrappers.h"
#include "gpopt/mdcache/CMDAccessor.h"
// #include "optimizer/orca/gpopt/translate/CTranslatorTBGPPToDXL.h"
#include "optimizer/orca/gpopt/translate/CTranslatorScalarToDXL.h"
#include "optimizer/orca/gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/exception.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/md/CMDCastGPDB.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDScCmpGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"

#include "naucrates/base/IDatumGeneric.h"
#include "naucrates/base/CDatumGenericGPDB.h"

// TBGPP related classes
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "catalog/catalog.hpp"
#include "translate/CTranslatorTBGPPToDXL.hpp"

using namespace gpdxl;
using namespace gpopt;
using namespace duckdb;

// Temporary defines..
#define NameStr(name)	((name).data) // From src/include/c.h
#define GpSegmentIdAttributeNumber			    (-8)    /*CDB*/ // From src/include/access/sysattr.h
#define TableOidAttributeNumber					(-7) // From src/include/access/sysattr.h
#define SelfItemPointerAttributeNumber			(-1) // From src/include/access/sysattr.h
#define RelationIsForeign(relation) \
	((bool)((relation)->rd_rel->relstorage == RELSTORAGE_FOREIGN)) // From src/include/utils/rel.h


static const ULONG cmp_type_mappings[][2] = {
	{IMDType::EcmptEq, CmptEq},	  {IMDType::EcmptNEq, CmptNEq},
	{IMDType::EcmptL, CmptLT},	  {IMDType::EcmptG, CmptGT},
	{IMDType::EcmptGEq, CmptGEq}, {IMDType::EcmptLEq, CmptLEq}};

//---------------------------------------------------------------------------
//	@function:
//		GetIndexTypeFromOid
//
//	@doc:
//		Retrieve the type of physical index structure
//
//---------------------------------------------------------------------------
static IMDIndex::EmdindexType
GetIndexTypeFromOid(OID index_oid)
{
	IndexType indexType = GetLogicalIndexType(index_oid);
	switch (indexType)
	{
		case IndexType::ART:
			return IMDIndex::EmdindBtree;
		// TODO we do not have below index types
		// case INDTYPE_BITMAP:
		// 	return IMDIndex::EmdindBitmap;
		// case INDTYPE_GIST:
		// 	return IMDIndex::EmdindGist;
		// case INDTYPE_GIN:
		// 	return IMDIndex::EmdindGin;
	}
	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
			   GPOS_WSZ_LIT("Query references unknown index type"));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveObject
//
//	@doc:
//		Retrieve a metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorTBGPPToDXL::RetrieveObject(CMemoryPool *mp,
										 CMDAccessor *md_accessor, IMDId *mdid,
										 IMDCacheObject::Emdtype mdtype)
{
	IMDCacheObject *md_obj = NULL;
	GPOS_ASSERT(NULL != md_accessor);

// #ifdef FAULT_INJECTOR
// 	gpdb::InjectFaultInOptTasks("opt_relcache_translator_catalog_access");
// #endif	// FAULT_INJECTOR

	switch (mdid->MdidType())
	{
		case IMDId::EmdidGeneral:
			md_obj = RetrieveObjectGPDB(mp, mdid, mdtype);
			break;

		case IMDId::EmdidRelStats:
			md_obj = RetrieveRelStats(mp, mdid);
			break;

		case IMDId::EmdidColStats:
			md_obj = RetrieveColStats(mp, md_accessor, mdid);
			break;

		case IMDId::EmdidCastFunc:
			md_obj = RetrieveCast(mp, mdid);
			break;

		case IMDId::EmdidScCmp:
			md_obj = RetrieveScCmp(mp, mdid);
			break;

		case IMDId::EmdidRel:
			md_obj = RetrieveRel(mp, md_accessor, mdid);
			break;

		case IMDId::EmdidInd:
			md_obj = RetrieveIndex(mp, md_accessor, mdid);
			break;

		case IMDId::EmdidCheckConstraint:
			md_obj = RetrieveCheckConstraints(mp, md_accessor, mdid);
			break;

		default:
			break;
	}

	if (NULL == md_obj)
	{
		// no match found
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	return md_obj;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveMDObjGPDB
//
//	@doc:
//		Retrieve a GPDB metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorTBGPPToDXL::RetrieveObjectGPDB(CMemoryPool *mp, IMDId *mdid,
											 IMDCacheObject::Emdtype mdtype)
{
	GPOS_ASSERT(mdid->MdidType() == CMDIdGPDB::EmdidGeneral);

	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_RTL_ASSERT(0 != oid);

	switch (mdtype)
	{
		case IMDCacheObject::EmdtType:
			return RetrieveType(mp, mdid);

		case IMDCacheObject::EmdtOp:
			return RetrieveScOp(mp, mdid);

		case IMDCacheObject::EmdtAgg:
			return RetrieveAgg(mp, mdid);

		case IMDCacheObject::EmdtFunc:
			return RetrieveFunc(mp, mdid);

		case IMDCacheObject::EmdtTrigger:
			return RetrieveTrigger(mp, mdid);

		case IMDCacheObject::EmdtSentinel:
			// // for window function lookup
			// if (gpdb::AggregateExists(oid))
			// {
			// 	return RetrieveAgg(mp, mdid);
			// }
			// else if (gpdb::FunctionExists(oid))
			// {
			// 	return RetrieveFunc(mp, mdid);
			// }
			// // no match found
			return NULL;

		default:
			GPOS_RTL_ASSERT_MSG(false, "Unexpected MD type.");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::GetRelName
//
//	@doc:
//		Return a relation name
//
//---------------------------------------------------------------------------
CMDName *
CTranslatorTBGPPToDXL::GetRelName(CMemoryPool *mp, duckdb::PropertySchemaCatalogEntry *rel)
{
	GPOS_ASSERT(NULL != rel);
	CHAR *relname = std::strcpy(new char[rel->GetName().length() + 1], rel->GetName().c_str());
	// CHAR *relname = const_cast<CHAR *>(rel->GetName().c_str());
	CWStringDynamic *relname_str =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, relname);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
	GPOS_DELETE(relname_str);
	return mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelIndexInfo
//
//	@doc:
//		Return the indexes defined on the given relation
//
//---------------------------------------------------------------------------
CMDIndexInfoArray *
CTranslatorTBGPPToDXL::RetrieveRelIndexInfo(CMemoryPool *mp, PropertySchemaCatalogEntry *rel)
{
	GPOS_ASSERT(NULL != rel);
	// if (gpdb::RelPartIsNone(rel->rd_id) || gpdb::IsLeafPartition(rel->rd_id))
	// {
		return RetrieveRelIndexInfoForNonPartTable(mp, rel);
	// }
	// else if (gpdb::RelPartIsRoot(rel->rd_id))
	// {
	// 	return RetrieveRelIndexInfoForPartTable(mp, rel);
	// }
	// else
	// {
		// interior partition: do not consider indexes
		CMDIndexInfoArray *md_index_info_array =
			GPOS_NEW(mp) CMDIndexInfoArray(mp);
		return md_index_info_array;
	// }
}

// return index info list of indexes defined on a partitioned table
CMDIndexInfoArray *
CTranslatorTBGPPToDXL::RetrieveRelIndexInfoForPartTable(CMemoryPool *mp,
														   Relation root_rel)
{
	GPOS_ASSERT(true); // TODO don't have index catalog yet..
	// CMDIndexInfoArray *md_index_info_array = GPOS_NEW(mp) CMDIndexInfoArray(mp);

	// // root of partitioned table: aggregate index information across different parts
	// List *plLogicalIndexInfo = RetrievePartTableIndexInfo(root_rel);

	// ListCell *lc = NULL;

	// ForEach(lc, plLogicalIndexInfo)
	// {
	// 	LogicalIndexInfo *logicalIndexInfo = (LogicalIndexInfo *) lfirst(lc);
	// 	OID index_oid = logicalIndexInfo->logicalIndexOid;

	// 	// only add supported indexes
	// 	Relation index_rel = gpdb::GetRelation(index_oid);

	// 	if (NULL == index_rel)
	// 	{
	// 		WCHAR wstr[1024];
	// 		CWStringStatic str(wstr, 1024);
	// 		COstreamString oss(&str);
	// 		oss << (ULONG) index_oid;
	// 		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
	// 				   str.GetBuffer());
	// 	}

	// 	GPOS_ASSERT(NULL != index_rel->rd_indextuple);

	// 	GPOS_TRY
	// 	{
	// 		if (IsIndexSupported(index_rel))
	// 		{
	// 			CMDIdGPDB *mdid_index =
	// 				GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidInd, index_oid);
	// 			BOOL is_partial = (NULL != logicalIndexInfo->partCons) ||
	// 							  (NIL != logicalIndexInfo->defaultLevels);
	// 			CMDIndexInfo *md_index_info =
	// 				GPOS_NEW(mp) CMDIndexInfo(mdid_index, is_partial);
	// 			md_index_info_array->Append(md_index_info);
	// 		}

	// 		gpdb::CloseRelation(index_rel);
	// 	}
	// 	GPOS_CATCH_EX(ex)
	// 	{
	// 		gpdb::CloseRelation(index_rel);
	// 		GPOS_RETHROW(ex);
	// 	}
	// 	GPOS_CATCH_END;
	// }
	// return md_index_info_array;
}

// return index info list of indexes defined on regular, external tables or leaf partitions
CMDIndexInfoArray *
CTranslatorTBGPPToDXL::RetrieveRelIndexInfoForNonPartTable(CMemoryPool *mp,
															  PropertySchemaCatalogEntry *rel)
{
	CMDIndexInfoArray *md_index_info_array = GPOS_NEW(mp) CMDIndexInfoArray(mp);

	auto append_index_md = [&](idx_t index_oid) {
		IndexCatalogEntry *index_cat = duckdb::GetIndex(index_oid);
	
		if (NULL == index_cat)
		{
			WCHAR wstr[1024];
			CWStringStatic str(wstr, 1024);
			COstreamString oss(&str);
			oss << (ULONG) index_oid;
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
					   str.GetBuffer());
		}

		// GPOS_ASSERT(NULL != index_rel->rd_indextuple);

		GPOS_TRY
		{
			if (IsIndexSupported(index_cat))
			{
				CMDIdGPDB *mdid_index =
					GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidInd, index_oid);
				// for a regular table, external table or leaf partition, an index is always complete
				CMDIndexInfo *md_index_info = GPOS_NEW(mp)
					CMDIndexInfo(mdid_index, false /* is_partial */);
				md_index_info_array->Append(md_index_info);
			}

			// gpdb::CloseRelation(index_rel);
		}
		GPOS_CATCH_EX(ex)
		{
			// gpdb::CloseRelation(index_rel);
			GPOS_RETHROW(ex);
		}
		GPOS_CATCH_END;
	};

	// not a partitioned table: obtain indexes directly from the catalog
	idx_t partition_oid = rel->GetPartitionOID();
	PartitionCatalogEntry *part_cat = duckdb::GetPartition(partition_oid);

	// Get PhysicalID Index
	// idx_t physical_id_index_oid = part_cat->GetPhysicalIDIndexOid(); // TODO 240115 tslee change this to ps_cat
	idx_t physical_id_index_oid = rel->GetPhysicalIDIndex();
	append_index_md(physical_id_index_oid);

	// Get AdjList Indexes
	idx_t_vector *adj_index_oids = part_cat->GetAdjIndexOidVec();
	for (idx_t i = 0; i < adj_index_oids->size(); i++) {
		idx_t index_oid = (*adj_index_oids)[i];
		append_index_md(index_oid);
	}

	// Get Property Indexes
	idx_t_vector *property_index_oids = part_cat->GetPropertyIndexOidVec();
	for (idx_t i = 0; i < property_index_oids->size(); i++) {
		idx_t index_oid = (*property_index_oids)[i];
		append_index_md(index_oid);
	}

	return md_index_info_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrievePartTableIndexInfo
//
//	@doc:
//		Return the index info list of on a partitioned table
//
//---------------------------------------------------------------------------
// List *
// CTranslatorTBGPPToDXL::RetrievePartTableIndexInfo(Relation rel)
// {
// 	GPOS_ASSERT(true); // TODO don't have index catalog yet..
// 	List *index_info_list = NIL;

	// LogicalIndexes *logical_indexes = gpdb::GetLogicalPartIndexes(rel->rd_id);

	// if (NULL == logical_indexes)
	// {
	// 	return NIL;
	// }
	// GPOS_ASSERT(NULL != logical_indexes);
	// GPOS_ASSERT(0 <= logical_indexes->numLogicalIndexes);

	// const ULONG num_indexes = (ULONG) logical_indexes->numLogicalIndexes;
	// for (ULONG ul = 0; ul < num_indexes; ul++)
	// {
	// 	LogicalIndexInfo *index_info = (logical_indexes->logicalIndexInfo)[ul];
	// 	index_info_list = gpdb::LAppend(index_info_list, index_info);
	// }

	// gpdb::GPDBFree(logical_indexes);

// 	return index_info_list;
// }

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelTriggers
//
//	@doc:
//		Return the triggers defined on the given relation
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorTBGPPToDXL::RetrieveRelTriggers(CMemoryPool *mp, PropertySchemaCatalogEntry *rel)
{
	GPOS_ASSERT(NULL != rel);
	// if (rel->rd_rel->relhastriggers && NULL == rel->trigdesc)
	// {
	// 	gpdb::BuildRelationTriggers(rel);
	// 	if (NULL == rel->trigdesc)
	// 	{
	// 		rel->rd_rel->relhastriggers = false;
	// 	}
	// }

	IMdIdArray *mdid_triggers_array = GPOS_NEW(mp) IMdIdArray(mp);
	// if (rel->rd_rel->relhastriggers)
	// {
	// 	const ULONG ulTriggers = rel->trigdesc->numtriggers;

	// 	for (ULONG ul = 0; ul < ulTriggers; ul++)
	// 	{
	// 		Trigger trigger = rel->trigdesc->triggers[ul];
	// 		OID trigger_oid = trigger.tgoid;
	// 		CMDIdGPDB *mdid_trigger =
	// 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, trigger_oid);
	// 		mdid_triggers_array->Append(mdid_trigger);
	// 	}
	// }

	return mdid_triggers_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelCheckConstraints
//
//	@doc:
//		Return the check constraints defined on the relation with the given oid
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorTBGPPToDXL::RetrieveRelCheckConstraints(CMemoryPool *mp, OID oid)
{
	IMdIdArray *check_constraint_mdids = GPOS_NEW(mp) IMdIdArray(mp);
	// List *check_constraints = gpdb::GetCheckConstraintOids(oid);

	// ListCell *lc = NULL;
	// ForEach(lc, check_constraints)
	// {
	// 	OID check_constraint_oid = lfirst_oid(lc);
	// 	GPOS_ASSERT(0 != check_constraint_oid);
	// 	CMDIdGPDB *mdid_check_constraint = GPOS_NEW(mp)
	// 		CMDIdGPDB(IMDId::EmdidCheckConstraint, check_constraint_oid);
	// 	check_constraint_mdids->Append(mdid_check_constraint);
	// }

	return check_constraint_mdids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::CheckUnsupportedRelation
//
//	@doc:
//		Check and fall back to planner for unsupported relations
//
//---------------------------------------------------------------------------
void
CTranslatorTBGPPToDXL::CheckUnsupportedRelation(OID rel_oid)
{
	GPOS_ASSERT(true); // TODO don't have this yet..
	// if (gpdb::RelPartIsInterior(rel_oid))
	// {
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
	// 			   GPOS_WSZ_LIT("Query on intermediate partition"));
	// }

	// List *part_keys = gpdb::GetPartitionAttrs(rel_oid);
	// ULONG num_of_levels = gpdb::ListLength(part_keys);

	// if (0 == num_of_levels && gpdb::HasSubclassSlow(rel_oid))
	// {
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
	// 			   GPOS_WSZ_LIT("Inherited tables"));
	// }

	// if (1 < num_of_levels)
	// {
	// 	if (!optimizer_multilevel_partitioning)
	// 	{
	// 		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
	// 				   GPOS_WSZ_LIT("Multi-level partitioned tables"));
	// 	}

	// 	if (!gpdb::IsMultilevelPartitionUniform(rel_oid))
	// 	{
	// 		GPOS_RAISE(
	// 			gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
	// 			GPOS_WSZ_LIT(
	// 				"Multi-level partitioned tables with non-uniform partitioning structure"));
	// 	}
	// }
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRel
//
//	@doc:
//		Retrieve a relation from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDRelation *
CTranslatorTBGPPToDXL::RetrieveRel(CMemoryPool *mp, CMDAccessor *md_accessor,
									  IMDId *mdid)
{
	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid(); // TODO check how this works
	GPOS_ASSERT(InvalidOid != oid);

	CheckUnsupportedRelation(oid);

	duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(oid);

	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	// if (RelationIsForeign(rel))
	// {
	// 	// GPORCA does not support foreign data wrappers
	// 	gpdb::CloseRelation(rel);
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
	// 			   GPOS_WSZ_LIT("Foreign Data"));
	// }

	// if (NULL != rel->rd_cdbpolicy &&
	// 	gpdb::GetGPSegmentCount() != rel->rd_cdbpolicy->numsegments)
	// {
	// 	// GPORCA does not support partially distributed tables yet
	// 	gpdb::CloseRelation(rel);
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiDXLInvalidAttributeValue,
	// 			   GPOS_WSZ_LIT("Partially Distributed Data"));
	// }

	CMDName *mdname = NULL;
	IMDRelation::Erelstoragetype rel_storage_type =
		IMDRelation::ErelstorageSentinel;
	CMDColumnArray *mdcol_array = NULL;
	IMDRelation::Ereldistrpolicy dist = IMDRelation::EreldistrSentinel;
	ULongPtrArray *distr_cols = NULL;
	IMdIdArray *distr_op_families = NULL;
	CMDIndexInfoArray *md_index_info_array = NULL;
	IMdIdArray *mdid_triggers_array = NULL;
	ULongPtrArray *part_keys = NULL;
	CharPtrArray *part_types = NULL;
	ULONG num_leaf_partitions = 0;
	BOOL convert_hash_to_random = false;
	ULongPtr2dArray *keyset_array = NULL;
	IMdIdArray *check_constraint_mdids = NULL;
	BOOL is_temporary = false;
	BOOL has_oids = false;
	BOOL is_partitioned = false;
	IMDRelation *md_rel = NULL;
	IMdIdArray *external_partitions = NULL;


	GPOS_TRY
	{
		// get rel name
		mdname = GetRelName(mp, rel);

		// get storage type
		// rel_storage_type = RetrieveRelStorageType(rel->rd_rel->relstorage);
		rel_storage_type = RetrieveRelStorageType('c'); // temporary

		// get relation columns
		mdcol_array =
			RetrieveRelColumns(mp, md_accessor, rel, rel_storage_type);
		const ULONG max_cols =
			GPDXL_SYSTEM_COLUMNS + (ULONG) rel->GetNumberOfColumns() + 1;
		ULONG *attno_mapping = ConstructAttnoMapping(mp, mdcol_array, max_cols);

		// TODO we do not support distributed environment now
		// get distribution policy
		dist = IMDRelation::EreldistrMasterOnly;

		// get distribution columns
		// if (IMDRelation::EreldistrHash == dist)
		// {
		// 	distr_cols = RetrieveRelDistributionCols(mp, gp_policy, mdcol_array,
		// 											 max_cols);
		// 	distr_op_families =
		// 		RetrieveRelDistributionOpFamilies(mp, gp_policy);
		// }

		// convert_hash_to_random = gpdb::IsChildPartDistributionMismatched(rel);

		// collect relation indexes
		md_index_info_array = RetrieveRelIndexInfo(mp, rel);

		// collect relation triggers // TODO we don't need this know
		mdid_triggers_array = RetrieveRelTriggers(mp, rel);

		// get partition keys
		// if (IMDRelation::ErelstorageExternal != rel_storage_type)
		// {
		// 	RetrievePartKeysAndTypes(mp, rel, oid, &part_keys, &part_types);
		// }
		// is_partitioned = (NULL != part_keys && 0 < part_keys->Size());

		// get number of leaf partitions
		// if (gpdb::RelPartIsRoot(oid))
		// {
		// 	num_leaf_partitions = gpdb::CountLeafPartTables(oid);
		// }

		// TODO implement
		// get key sets
		BOOL should_add_default_keys = true;
			// RelHasSystemColumns(''); // TODO
		keyset_array = RetrieveRelKeysets(mp, oid, should_add_default_keys,
										  is_partitioned, attno_mapping);

		// collect all check constraints
		check_constraint_mdids = RetrieveRelCheckConstraints(mp, oid);

		// is_temporary = (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP);
		// TODO implement
		// has_oids = rel->rd_rel->relhasoids;

		// if (gpdb::HasExternalPartition(oid))
		// {
		// 	GPOS_ASSERT(GPOS_FTRACE(EopttraceEnableExternalPartitionedTables));
		// 	external_partitions = RetrieveRelExternalPartitions(mp, oid);
		// }

		// GPOS_DELETE_ARRAY(attno_mapping);
		// gpdb::CloseRelation(rel);
	}
	GPOS_CATCH_EX(ex)
	{
		// gpdb::CloseRelation(rel);
		// GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	GPOS_ASSERT(IMDRelation::ErelstorageSentinel != rel_storage_type);
	GPOS_ASSERT(IMDRelation::EreldistrSentinel != dist);

	mdid->AddRef();

	// Retrieve full part constraints partitioned tables with indexes or external partitions;
	// returns NULL for non-partitioned tables
	BOOL construct_full_partcnstr_expr = false;
		// (md_index_info_array->Size() > 0 ||
		//  (external_partitions != NULL && external_partitions->Size() > 0) ||
		//  IMDRelation::ErelstorageExternal == rel_storage_type);

	CMDPartConstraintGPDB *mdpart_constraint = NULL; // = RetrievePartConstraintForRel(
		// mp, md_accessor, oid, mdcol_array, construct_full_partcnstr_expr);

	if (IMDRelation::ErelstorageExternal == rel_storage_type)
	{
		// ExtTableEntry *extentry = gpdb::GetExternalTableEntry(oid);

		// md_rel = GPOS_NEW(mp) CMDRelationExternalGPDB(
		// 	mp, mdid, mdname, dist, mdcol_array, distr_cols, distr_op_families,
		// 	convert_hash_to_random, keyset_array, md_index_info_array,
		// 	mdid_triggers_array, check_constraint_mdids, mdpart_constraint,
		// 	extentry->rejectlimit, ('r' == extentry->rejectlimittype),
		// 	NULL /* it's sufficient to pass NULL here since ORCA
		// 						doesn't really make use of the logerrors value.
		// 						In case of converting the DXL returned from to
		// 						PlanStmt, currently the code looks up the information
		// 						from catalog and fill in the required values into the ExternalScan */
		// );
	}
	else
	{
		md_rel = GPOS_NEW(mp) CMDRelationGPDB(
			mp, mdid, mdname, is_temporary, rel_storage_type, dist, mdcol_array,
			distr_cols, distr_op_families, part_keys, part_types,
			num_leaf_partitions, convert_hash_to_random, keyset_array,
			md_index_info_array, mdid_triggers_array, check_constraint_mdids,
			mdpart_constraint, has_oids, external_partitions);
	}

	return md_rel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelColumns
//
//	@doc:
//		Get relation columns
//
//---------------------------------------------------------------------------
CMDColumnArray *
CTranslatorTBGPPToDXL::RetrieveRelColumns(
	CMemoryPool *mp, CMDAccessor *md_accessor, duckdb::PropertySchemaCatalogEntry *rel,
	IMDRelation::Erelstoragetype rel_storage_type)
{
	CMDColumnArray *mdcol_array = GPOS_NEW(mp) CMDColumnArray(mp);

	// Insert physical id column. Attno = -1
	{
		CMDName *md_colname =
			CDXLUtils::CreateMDNameFromCharArray(mp, "_id");

		// translate the default column value
		CDXLNode *dxl_default_col_val = NULL;

		ULONG col_len = sizeof(uint64_t);
		CMDIdGPDB *mdid_col =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, (OID) duckdb::LogicalTypeId::ID + LOGICAL_TYPE_BASE_ID);
		
		CMDColumn *md_col = GPOS_NEW(mp)
			CMDColumn(md_colname, -1/*att->attnum*/, mdid_col, -1/*att->atttypmod*/,
					  true /*!att->attnotnull, is_nullable*/, false /*att->attisdropped*/,
					  dxl_default_col_val /* default value */, 0, col_len);

		mdcol_array->Append(md_col);
	}

	ULONG attnum = 1; // start from 1 - refer pg_attribute.h
	
	for (ULONG ul = 0; ul < (ULONG) rel->GetNumberOfColumns(); ul++)
	{
		if (rel->GetType(ul) == duckdb::LogicalType::FORWARD_ADJLIST ||
			rel->GetType(ul) == duckdb::LogicalType::BACKWARD_ADJLIST) continue;
		// Form_pg_attribute att = rel->rd_att->attrs[ul];
		CMDName *md_colname =
			CDXLUtils::CreateMDNameFromCharArray(mp, rel->GetPropertyKeyName(ul).c_str());
		// CMDName *md_colname = CDXLUtils::CreateMDNameFromCharArray(mp, "");

		// translate the default column value
		CDXLNode *dxl_default_col_val = NULL;

		ULONG prop_id = rel->GetPropKeyIDs()->at(ul);

		// TODO we don't have default col val..
		// if (!att->attisdropped)
		// {
		// 	dxl_default_col_val = GetDefaultColumnValue(
		// 		mp, md_accessor, rel->rd_att, att->attnum);
		// }

		ULONG col_len = gpos::ulong_max;
		CMDIdGPDB *mdid_col = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral,
			(OID) rel->GetType(ul) + NUM_MAX_LOGICAL_TYPES * rel->GetExtraTypeInfo(ul) + LOGICAL_TYPE_BASE_ID);
		// HeapTuple stats_tup = gpdb::GetAttStats(rel->rd_id, ul + 1);

		// Column width priority:
		// 1. If there is average width kept in the stats for that column, pick that value.
		// 2. If not, if it is a fixed length text type, pick the size of it. E.g if it is
		//    varchar(10), assign 10 as the column length.
		// 3. Else if it not dropped and a fixed length type such as int4, assign the fixed
		//    length.
		// 4. Otherwise, assign it to default column width which is 8.
		col_len = rel->GetTypeSize(ul);
		if (rel->is_fake) {
			// TODO: change hard coded value
			// 1000 means the number of schemas.
			// This is for scaling the width for the temporal table
			col_len = col_len / 500;
		}
		// if (HeapTupleIsValid(stats_tup))
		// {
		// 	Form_pg_statistic form_pg_stats =
		// 		(Form_pg_statistic) GETSTRUCT(stats_tup);

		// 	// column width
		// 	col_len = form_pg_stats->stawidth;
		// 	gpdb::FreeHeapTuple(stats_tup);
		// }
		// else if ((mdid_col->Equals(&CMDIdGPDB::m_mdid_bpchar) ||
		// 		  mdid_col->Equals(&CMDIdGPDB::m_mdid_varchar)) &&
		// 		 (VARHDRSZ < att->atttypmod))
		// {
		// 	col_len = (ULONG) att->atttypmod - VARHDRSZ;
		// }
		// else
		// {
		// 	DOUBLE width = CStatistics::DefaultColumnWidth.Get();
		// 	col_len = (ULONG) width;

		// 	if (!att->attisdropped)
		// 	{
		// 		IMDType *md_type =
		// 			CTranslatorTBGPPToDXL::RetrieveType(mp, mdid_col);
		// 		if (md_type->IsFixedLength())
		// 		{
		// 			col_len = md_type->Length();
		// 		}
		// 		md_type->Release();
		// 	}
		// }

		auto type_mod = rel->GetExtraTypeInfo(ul);
		CMDColumn *md_col = GPOS_NEW(mp)
			CMDColumn(md_colname, attnum++/*att->attnum*/, mdid_col, type_mod,
					  true /*!att->attnotnull, is_nullable*/, false /*att->attisdropped*/,
					  dxl_default_col_val /* default value */, prop_id, col_len);

		mdcol_array->Append(md_col);
	}

	// add system columns // TODO what is this?
	// if (RelHasSystemColumns(rel->rd_rel->relkind))
	// {
	// 	BOOL is_ao_table =
	// 		IMDRelation::ErelstorageAppendOnlyRows == rel_storage_type ||
	// 		IMDRelation::ErelstorageAppendOnlyCols == rel_storage_type;
	// 	AddSystemColumns(mp, mdcol_array, rel, is_ao_table);
	// }

	return mdcol_array;
}

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::GetDefaultColumnValue
// //
// //	@doc:
// //		Return the dxl representation of column's default value
// //
// //---------------------------------------------------------------------------
// CDXLNode *
// CTranslatorTBGPPToDXL::GetDefaultColumnValue(CMemoryPool *mp,
// 												CMDAccessor *md_accessor,
// 												TupleDesc rd_att,
// 												AttrNumber attno)
// {
// 	GPOS_ASSERT(attno > 0);

// 	Node *node = NULL;

// 	// Scan to see if relation has a default for this column
// 	if (NULL != rd_att->constr && 0 < rd_att->constr->num_defval)
// 	{
// 		AttrDefault *defval = rd_att->constr->defval;
// 		INT num_def = rd_att->constr->num_defval;

// 		GPOS_ASSERT(NULL != defval);
// 		for (ULONG ul = 0; ul < (ULONG) num_def; ul++)
// 		{
// 			if (attno == defval[ul].adnum)
// 			{
// 				// found it, convert string representation to node tree.
// 				node = gpdb::StringToNode(defval[ul].adbin);
// 				break;
// 			}
// 		}
// 	}

// 	if (NULL == node)
// 	{
// 		// get the default value for the type
// 		Form_pg_attribute att_tup = rd_att->attrs[attno - 1];
// 		node = gpdb::GetTypeDefault(att_tup->atttypid);
// 	}

// 	if (NULL == node)
// 	{
// 		return NULL;
// 	}

// 	// translate the default value expression
// 	return CTranslatorScalarToDXL::TranslateStandaloneExprToDXL(
// 		mp, md_accessor,
// 		NULL, /* var_colid_mapping --- subquery or external variable are not supported in default expression */
// 		(Expr *) node);
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::GetRelDistribution
// //
// //	@doc:
// //		Return the distribution policy of the relation
// //
// //---------------------------------------------------------------------------
// IMDRelation::Ereldistrpolicy
// CTranslatorTBGPPToDXL::GetRelDistribution(GpPolicy *gp_policy)
// {
// 	if (NULL == gp_policy)
// 	{
// 		return IMDRelation::EreldistrMasterOnly;
// 	}

// 	if (POLICYTYPE_REPLICATED == gp_policy->ptype)
// 	{
// 		return IMDRelation::EreldistrReplicated;
// 	}

// 	if (POLICYTYPE_PARTITIONED == gp_policy->ptype)
// 	{
// 		if (0 == gp_policy->nattrs)
// 		{
// 			return IMDRelation::EreldistrRandom;
// 		}

// 		return IMDRelation::EreldistrHash;
// 	}

// 	if (POLICYTYPE_ENTRY == gp_policy->ptype)
// 	{
// 		return IMDRelation::EreldistrMasterOnly;
// 	}

// 	GPOS_RAISE(gpdxl::ExmaMD, ExmiDXLUnrecognizedType,
// 			   GPOS_WSZ_LIT("unrecognized distribution policy"));
// 	return IMDRelation::EreldistrSentinel;
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::RetrieveRelDistributionCols
// //
// //	@doc:
// //		Get distribution columns
// //
// //---------------------------------------------------------------------------
// ULongPtrArray *
// CTranslatorTBGPPToDXL::RetrieveRelDistributionCols(
// 	CMemoryPool *mp, GpPolicy *gp_policy, CMDColumnArray *mdcol_array,
// 	ULONG size)
// {
// 	ULONG *attno_mapping = GPOS_NEW_ARRAY(mp, ULONG, size);

// 	for (ULONG ul = 0; ul < mdcol_array->Size(); ul++)
// 	{
// 		const IMDColumn *md_col = (*mdcol_array)[ul];
// 		INT attno = md_col->AttrNum();

// 		ULONG idx = (ULONG)(GPDXL_SYSTEM_COLUMNS + attno);
// 		attno_mapping[idx] = ul;
// 	}

// 	ULongPtrArray *distr_cols = GPOS_NEW(mp) ULongPtrArray(mp);

// 	for (ULONG ul = 0; ul < (ULONG) gp_policy->nattrs; ul++)
// 	{
// 		AttrNumber attno = gp_policy->attrs[ul];

// 		distr_cols->Append(
// 			GPOS_NEW(mp) ULONG(GetAttributePosition(attno, attno_mapping)));
// 	}

// 	GPOS_DELETE_ARRAY(attno_mapping);
// 	return distr_cols;
// }

// IMdIdArray *
// CTranslatorTBGPPToDXL::RetrieveRelDistributionOpFamilies(CMemoryPool *mp,
// 															GpPolicy *gp_policy)
// {
// 	IMdIdArray *distr_op_classes = GPOS_NEW(mp) IMdIdArray(mp);

// 	Oid *opclasses = gp_policy->opclasses;
// 	for (ULONG ul = 0; ul < (ULONG) gp_policy->nattrs; ul++)
// 	{
// 		Oid opfamily = gpdb::GetOpclassFamily(opclasses[ul]);
// 		distr_op_classes->Append(GPOS_NEW(mp)
// 									 CMDIdGPDB(IMDId::EmdidGeneral, opfamily));
// 	}

// 	return distr_op_classes;
// }

IMdIdArray *
CTranslatorTBGPPToDXL::RetrieveRelExternalPartitions(CMemoryPool *mp,
														OID rel_oid)
{
	IMdIdArray *external_partitions = GPOS_NEW(mp) IMdIdArray(mp);

	// List *extparts_list = gpdb::GetExternalPartitions(rel_oid);
	// ListCell *lc;
	// foreach (lc, extparts_list)
	// {
	// 	OID ext_rel_oid = lfirst_oid(lc);
	// 	external_partitions->Append(
	// 		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, ext_rel_oid));
	// }

	return external_partitions;
}

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::AddSystemColumns
// //
// //	@doc:
// //		Adding system columns (oid, tid, xmin, etc) in table descriptors
// //
// //---------------------------------------------------------------------------
// void
// CTranslatorTBGPPToDXL::AddSystemColumns(CMemoryPool *mp,
// 										   CMDColumnArray *mdcol_array,
// 										   Relation rel, BOOL is_ao_table)
// {
// 	BOOL has_oids = rel->rd_att->tdhasoid;
// 	is_ao_table = is_ao_table || gpdb::IsAppendOnlyPartitionTable(rel->rd_id);

// 	for (INT i = SelfItemPointerAttributeNumber;
// 		 i > FirstLowInvalidHeapAttributeNumber; i--)
// 	{
// 		AttrNumber attno = AttrNumber(i);
// 		GPOS_ASSERT(0 != attno);

// 		if (ObjectIdAttributeNumber == i && !has_oids)
// 		{
// 			continue;
// 		}

// 		if (IsTransactionVisibilityAttribute(i) && is_ao_table)
// 		{
// 			// skip transaction attrbutes like xmin, xmax, cmin, cmax for AO tables
// 			continue;
// 		}

// 		// get system name for that attribute
// 		const CWStringConst *sys_colname =
// 			CTranslatorUtils::GetSystemColName(attno);
// 		GPOS_ASSERT(NULL != sys_colname);

// 		// copy string into column name
// 		CMDName *md_colname = GPOS_NEW(mp) CMDName(mp, sys_colname);

// 		CMDColumn *md_col = GPOS_NEW(mp) CMDColumn(
// 			md_colname, attno, CTranslatorUtils::GetSystemColType(mp, attno),
// 			default_type_modifier,
// 			false,	// is_nullable
// 			false,	// is_dropped
// 			NULL,	// default value
// 			CTranslatorUtils::GetSystemColLength(attno));

// 		mdcol_array->Append(md_col);
// 	}
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::IsTransactionVisibilityAttribute
// //
// //	@doc:
// //		Check if attribute number is one of the system attributes related to
// //		transaction visibility such as xmin, xmax, cmin, cmax
// //
// //---------------------------------------------------------------------------
// BOOL
// CTranslatorTBGPPToDXL::IsTransactionVisibilityAttribute(INT attno)
// {
// 	return attno == MinTransactionIdAttributeNumber ||
// 		   attno == MaxTransactionIdAttributeNumber ||
// 		   attno == MinCommandIdAttributeNumber ||
// 		   attno == MaxCommandIdAttributeNumber;
// }

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveIndex
//
//	@doc:
//		Retrieve an index from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDIndex *
CTranslatorTBGPPToDXL::RetrieveIndex(CMemoryPool *mp,
										CMDAccessor *md_accessor,
										IMDId *mdid_index)
{
	OID index_oid = CMDIdGPDB::CastMdid(mdid_index)->Oid();
	GPOS_ASSERT(0 != index_oid);
	IndexCatalogEntry *index_cat = duckdb::GetIndex(index_oid);

	if (NULL == index_cat)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid_index->GetBuffer());
	}

	const IMDRelation *md_rel = NULL;
	const PartitionCatalogEntry *part_cat = NULL;
	PropertySchemaCatalogEntry *ps_cat = NULL;
	// Form_pg_index form_pg_index = NULL;
	CMDName *mdname = NULL;
	IMDIndex::EmdindexType index_type = IMDIndex::EmdindSentinel;
	IMDId *mdid_item_type = NULL;
	bool index_clustered = false;
	ULongPtrArray *index_key_cols_array = NULL;
	ULONG *attno_mapping = NULL;

	GPOS_TRY
	{
		if (!IsIndexSupported(index_cat))
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					   GPOS_WSZ_LIT("Index type"));
		}

		// form_pg_index = index_rel->rd_index;
		// GPOS_ASSERT(NULL != form_pg_index);
		// index_clustered = form_pg_index->indisclustered;

		// OID rel_oid = form_pg_index->indrelid;
		// idx_t pid = index_cat->GetPartitionID();
		idx_t psid = index_cat->GetPropertySchemaID();

		// TODO we do not support partition currently
		// if (gpdb::IsLeafPartition(rel_oid))
		// {
		// 	rel_oid = gpdb::GetRootPartition(rel_oid);
		// }

		CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, psid);

		md_rel = md_accessor->RetrieveRel(mdid_rel);

		// TODO we do not support partition currently
		// if (md_rel->IsPartitioned())
		// {
		// 	LogicalIndexes *logical_indexes =
		// 		gpdb::GetLogicalPartIndexes(rel_oid);
		// 	GPOS_ASSERT(NULL != logical_indexes);

		// 	IMDIndex *index = RetrievePartTableIndex(
		// 		mp, md_accessor, mdid_index, md_rel, logical_indexes);

		// 	// cleanup
		// 	gpdb::GPDBFree(logical_indexes);

		// 	if (NULL != index)
		// 	{
		// 		mdid_rel->Release();
		// 		gpdb::CloseRelation(index_rel);
		// 		return index;
		// 	}
		// }

		index_type = IMDIndex::EmdindBtree;
		mdid_item_type = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_ANY);
		if (IndexType::ART == index_cat->index_type)
		{
			index_type = IMDIndex::EmdindBtree;
		}
		else if (IndexType::FORWARD_CSR == index_cat->index_type) // TODO we need to add new index type
		{
			index_type = IMDIndex::EmdindFwdAdjlist;
		}
		else if (IndexType::BACKWARD_CSR == index_cat->index_type)
		{
			index_type = IMDIndex::EmdindBwdAdjlist;
		}

		// get the index name
		string index_name_str = index_cat->GetName();
		CHAR *index_name = const_cast<char *>(index_name_str.c_str());
		CWStringDynamic *str_name =
			CDXLUtils::CreateDynamicStringFromCharArray(mp, index_name);
		mdname = GPOS_NEW(mp) CMDName(mp, str_name);
		GPOS_DELETE(str_name);

		// Relation table =
		// 	gpdb::GetRelation(CMDIdGPDB::CastMdid(md_rel->MDId())->Oid());
		// part_cat = duckdb::GetPartition(pid);
		ps_cat = duckdb::GetRelation(psid);
		// ULONG size = GPDXL_SYSTEM_COLUMNS + part_cat->GetNumberOfColumns() + 1;
		ULONG size = GPDXL_SYSTEM_COLUMNS + ps_cat->GetNumberOfColumns() + 1;
			// = GPDXL_SYSTEM_COLUMNS + (ULONG) table->rd_att->natts + 1;
		// gpdb::CloseRelation(table);	 // close relation as early as possible

		// attno_mapping = PopulateAttnoPositionMap(mp, part_cat, size);
		attno_mapping = PopulateAttnoPositionMap(mp, md_rel, size);

		// extract the position of the key columns
		index_key_cols_array = GPOS_NEW(mp) ULongPtrArray(mp);

		int64_t_vector *index_key_columns = index_cat->GetIndexKeyColumns();
		for (int i = 0; i < index_key_columns->size(); i++)
		{
			INT attno = (*index_key_columns)[i];
			GPOS_ASSERT(0 != attno && "Index expressions not supported");

			index_key_cols_array->Append(
				GPOS_NEW(mp) ULONG(GetAttributePosition(attno, attno_mapping)));
		}
		mdid_rel->Release();
		// gpdb::CloseRelation(index_rel);
	}
	GPOS_CATCH_EX(ex)
	{
		// gpdb::CloseRelation(index_rel);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	ULongPtrArray *included_cols = ComputeIncludedCols(mp, md_rel, index_type);
	// ULongPtrArray *included_cols = ComputeIncludedCols(mp, part_cat);
	mdid_index->AddRef();
	IMdIdArray *op_families_mdids = RetrieveIndexOpFamilies(mp, mdid_index);

	CMDIndexGPDB *index = GPOS_NEW(mp)
		CMDIndexGPDB(mp, mdid_index, mdname, index_clustered, index_type,
					 GetIndexTypeFromOid(index_oid), mdid_item_type,
					 index_key_cols_array, included_cols, op_families_mdids,
					 NULL  // mdpart_constraint
		);

	GPOS_DELETE_ARRAY(attno_mapping);
	return index;
}

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::RetrievePartTableIndex
// //
// //	@doc:
// //		Retrieve an index over a partitioned table from the relcache given its
// //		mdid
// //
// //---------------------------------------------------------------------------
// IMDIndex *
// CTranslatorTBGPPToDXL::RetrievePartTableIndex(
// 	CMemoryPool *mp, CMDAccessor *md_accessor, IMDId *mdid_index,
// 	const IMDRelation *md_rel, LogicalIndexes *logical_indexes)
// {
// 	GPOS_ASSERT(NULL != logical_indexes);
// 	GPOS_ASSERT(0 < logical_indexes->numLogicalIndexes);

// 	OID oid = CMDIdGPDB::CastMdid(mdid_index)->Oid();

// 	LogicalIndexInfo *index_info = LookupLogicalIndexById(logical_indexes, oid);
// 	if (NULL == index_info)
// 	{
// 		return NULL;
// 	}

// 	return RetrievePartTableIndex(mp, md_accessor, index_info, mdid_index,
// 								  md_rel);
// }

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::LookupLogicalIndexById
//
//	@doc:
//		Lookup an index given its id from the logical indexes structure
//
//---------------------------------------------------------------------------
LogicalIndexInfo *
CTranslatorTBGPPToDXL::LookupLogicalIndexById(
	LogicalIndexes *logical_indexes, OID oid)
{
	// GPOS_ASSERT(NULL != logical_indexes &&
	// 			0 <= logical_indexes->numLogicalIndexes);

	ULONG num_index;
	// const ULONG num_index = logical_indexes->numLogicalIndexes;
	LogicalIndexInfo *bitmapInfo = NULL;
	LogicalIndexInfo *otherIndexInfo = NULL;
	for (ULONG ul = 0; ul < num_index; ul++)
	{
		LogicalIndexInfo *index_info; // = (logical_indexes->logicalIndexInfo)[ul];

		//  if both btree and bitmap index exist for a given index OID
		//  (implying that this is a btree index on a homogenous heap
		//  partitioned table), use the full bitmap index only if a full btree
		//  index does not exist
		// When considering an index by the OID, we give preference as follows:
		// 1. full btree index
		// 2. full bitmap index
		// 3. any other index (the first instance, to preserve existing behavior)

		BOOL isFullIndex; // =
			// (index_info->partCons == NULL && index_info->defaultLevels == NIL);
		// if (oid == index_info->logicalIndexOid)
		// {
		// 	if (index_info->indType == INDTYPE_BTREE && isFullIndex)
		// 	{
		// 		return index_info;
		// 	}
		// 	else if (index_info->indType == INDTYPE_BITMAP && isFullIndex)
		// 	{
		// 		bitmapInfo = index_info;
		// 	}
		// 	else if (otherIndexInfo == NULL)
		// 	{
		// 		otherIndexInfo = index_info;
		// 	}
		// }
	}
	if (bitmapInfo != NULL)
	{
		return bitmapInfo;
	}
	else if (otherIndexInfo != NULL)
	{
		return otherIndexInfo;
	}

	return NULL;
}

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::RetrievePartTableIndex
// //
// //	@doc:
// //		Construct an MD cache index object given its logical index representation
// //
// //---------------------------------------------------------------------------
// IMDIndex *
// CTranslatorTBGPPToDXL::RetrievePartTableIndex(CMemoryPool *mp,
// 												 CMDAccessor *md_accessor,
// 												 LogicalIndexInfo *index_info,
// 												 IMDId *mdid_index,
// 												 const IMDRelation *md_rel)
// {
// 	OID index_oid = index_info->logicalIndexOid;

// 	Relation index_rel = gpdb::GetRelation(index_oid);

// 	if (NULL == index_rel)
// 	{
// 		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
// 				   mdid_index->GetBuffer());
// 	}

// 	if (!IsIndexSupported(index_rel))
// 	{
// 		gpdb::CloseRelation(index_rel);
// 		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
// 				   GPOS_WSZ_LIT("Index type"));
// 	}

// 	// get the index name
// 	GPOS_ASSERT(NULL != index_rel->rd_index);
// 	Form_pg_index form_pg_index = index_rel->rd_index;

// 	CHAR *index_name = NameStr(index_rel->rd_rel->relname);
// 	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, index_name);
// 	gpdb::CloseRelation(index_rel);

// 	OID rel_oid = CMDIdGPDB::CastMdid(md_rel->MDId())->Oid();
// 	Relation table = gpdb::GetRelation(rel_oid);
// 	ULONG size = GPDXL_SYSTEM_COLUMNS + (ULONG) table->rd_att->natts + 1;
// 	gpdb::CloseRelation(table);

// 	ULONG *attno_mapping = PopulateAttnoPositionMap(mp, md_rel, size);

// 	ULongPtrArray *included_cols = ComputeIncludedCols(mp, md_rel);

// 	// extract the position of the key columns
// 	ULongPtrArray *index_key_cols_array = GPOS_NEW(mp) ULongPtrArray(mp);

// 	for (int i = 0; i < index_info->nColumns; i++)
// 	{
// 		INT attno = index_info->indexKeys[i];
// 		GPOS_ASSERT(0 != attno && "Index expressions not supported");

// 		index_key_cols_array->Append(
// 			GPOS_NEW(mp) ULONG(GetAttributePosition(attno, attno_mapping)));
// 	}

// 	/*
// 	 * If an index exists only on a leaf part, part_constraint refers to the expression
// 	 * identifying the path to reach the partition holding the index. For indexes
// 	 * available on all parts it is set to NULL.
// 	 */
// 	Node *part_constraint = index_info->partCons;

// 	/*
// 	 * If an index exists all on the parts including default, the logical index
// 	 * info created marks defaultLevels as NIL. However, if an index exists only on
// 	 * leaf parts plDefaultLevel contains the default part level which come across while
// 	 * reaching to the leaf part from root.
// 	 */
// 	List *default_levels = index_info->defaultLevels;

// 	// get number of partitioning levels
// 	List *part_keys = gpdb::GetPartitionAttrs(rel_oid);
// 	const ULONG num_of_levels = gpdb::ListLength(part_keys);
// 	gpdb::ListFree(part_keys);

// 	/* get relation constraints
// 	 * default_levels_rel indicates the levels on which default partitions exists
// 	 * for the partitioned table
// 	 */
// 	List *default_levels_rel = NIL;
// 	Node *part_constraints_rel =
// 		gpdb::GetRelationPartContraints(rel_oid, &default_levels_rel);

// 	BOOL is_unbounded = (NULL == part_constraint) && (NIL == default_levels);
// 	for (ULONG ul = 0; ul < num_of_levels; ul++)
// 	{
// 		is_unbounded =
// 			is_unbounded && LevelHasDefaultPartition(default_levels_rel, ul);
// 	}

// 	/*
// 	 * If part_constraint is NULL and default_levels is NIL,
// 	 * it indicates that the index is available on all the parts including
// 	 * default part. So, we can say that levels on which default partitions
// 	 * exists for the relation applies to the index as well and the relative
// 	 * scan will not be partial.
// 	 */
// 	List *default_levels_derived_list = NIL;
// 	if (NULL == part_constraint && NIL == default_levels)
// 		default_levels_derived_list = default_levels_rel;
// 	else
// 		default_levels_derived_list = default_levels;

// 	ULongPtrArray *default_levels_derived = GPOS_NEW(mp) ULongPtrArray(mp);
// 	for (ULONG ul = 0; ul < num_of_levels; ul++)
// 	{
// 		if (is_unbounded ||
// 			LevelHasDefaultPartition(default_levels_derived_list, ul))
// 		{
// 			default_levels_derived->Append(GPOS_NEW(mp) ULONG(ul));
// 		}
// 	}
// 	gpdb::ListFree(default_levels_derived_list);

// 	if (NULL == part_constraint)
// 	{
// 		if (NIL == default_levels)
// 		{
// 			// NULL part constraints means all non-default partitions -> get constraint from the part table
// 			part_constraint = part_constraints_rel;
// 		}
// 		else
// 		{
// 			part_constraint =
// 				gpdb::MakeBoolConst(false /*value*/, false /*isull*/);
// 		}
// 	}

// 	CMDPartConstraintGPDB *mdpart_constraint =
// 		RetrievePartConstraintForIndex(mp, md_accessor, md_rel, part_constraint,
// 									   default_levels_derived, is_unbounded);

// 	default_levels_derived->Release();
// 	mdid_index->AddRef();

// 	GPOS_ASSERT(INDTYPE_BITMAP == index_info->indType ||
// 				INDTYPE_BTREE == index_info->indType ||
// 				INDTYPE_GIST == index_info->indType ||
// 				INDTYPE_GIN == index_info->indType);

// 	IMDIndex::EmdindexType index_type = IMDIndex::EmdindBtree;
// 	IMDId *mdid_item_type =
// 		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_ANY);
// 	if (INDTYPE_BITMAP == index_info->indType)
// 	{
// 		index_type = IMDIndex::EmdindBitmap;
// 	}
// 	else if (INDTYPE_GIST == index_info->indType)
// 	{
// 		index_type = IMDIndex::EmdindGist;
// 	}
// 	else if (INDTYPE_GIN == index_info->indType)
// 	{
// 		index_type = IMDIndex::EmdindGin;
// 	}

// 	IMdIdArray *pdrgpmdidOpFamilies = RetrieveIndexOpFamilies(mp, mdid_index);

// 	CMDIndexGPDB *index = GPOS_NEW(mp) CMDIndexGPDB(
// 		mp, mdid_index, mdname, form_pg_index->indisclustered, index_type,
// 		GetIndexTypeFromOid(index_oid), mdid_item_type, index_key_cols_array,
// 		included_cols, pdrgpmdidOpFamilies, mdpart_constraint);

// 	GPOS_DELETE_ARRAY(attno_mapping);

// 	return index;
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::LevelHasDefaultPartition
// //
// //	@doc:
// //		Check whether the default partition at level one is included
// //
// //---------------------------------------------------------------------------
// BOOL
// CTranslatorTBGPPToDXL::LevelHasDefaultPartition(List *default_levels,
// 												   ULONG level)
// {
// 	if (NIL == default_levels)
// 	{
// 		return false;
// 	}

// 	ListCell *lc = NULL;
// 	ForEach(lc, default_levels)
// 	{
// 		ULONG default_level = (ULONG) lfirst_int(lc);
// 		if (level == default_level)
// 		{
// 			return true;
// 		}
// 	}

// 	return false;
// }

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::ComputeIncludedCols
//
//	@doc:
//		Compute the included columns in an index
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorTBGPPToDXL::ComputeIncludedCols(CMemoryPool *mp,
											  const IMDRelation *md_rel,
											  IMDIndex::EmdindexType index_type
											  /*const PartitionCatalogEntry *part_cat*/)
{
	// TODO: 3/19/2012; currently we assume that all the columns
	// in the table are available from the index.

	ULongPtrArray *included_cols = GPOS_NEW(mp) ULongPtrArray(mp);
	const ULONG num_included_cols = md_rel->ColumnCount();
	if (index_type == IMDIndex::EmdindFwdAdjlist || index_type == IMDIndex::EmdindBwdAdjlist) 
	// if (false)
	{
		// S62 fwd/bwd adjlist include only sid & tid columns
		GPOS_ASSERT(num_included_cols >= 2);
		// included_cols->Append(GPOS_NEW(mp) ULONG(0));
		included_cols->Append(GPOS_NEW(mp) ULONG(1));
		included_cols->Append(GPOS_NEW(mp) ULONG(2));
	}
	else
	{
		for (ULONG ul = 0; ul < num_included_cols; ul++)
		{
			// if (!md_rel->GetMdCol(ul)->IsDropped()) // TODO we do not have column drop currently
			// {
				included_cols->Append(GPOS_NEW(mp) ULONG(ul));
			// }
		}
	}

	return included_cols;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::GetAttributePosition
//
//	@doc:
//		Return the position of a given attribute
//
//---------------------------------------------------------------------------
ULONG
CTranslatorTBGPPToDXL::GetAttributePosition(INT attno,
											   ULONG *GetAttributePosition)
{
	ULONG idx = (ULONG)(GPDXL_SYSTEM_COLUMNS + attno);
	ULONG pos = GetAttributePosition[idx];
	GPOS_ASSERT(gpos::ulong_max != pos);

	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::PopulateAttnoPositionMap
//
//	@doc:
//		Populate the attribute to position mapping
//
//---------------------------------------------------------------------------
ULONG *
CTranslatorTBGPPToDXL::PopulateAttnoPositionMap(CMemoryPool *mp,
												   const IMDRelation *md_rel,
												   /*const PartitionCatalogEntry *part_cat,*/
												   ULONG size)
{
	GPOS_ASSERT(NULL != md_rel);
	const ULONG num_included_cols = md_rel->ColumnCount();

	GPOS_ASSERT(num_included_cols <= size);
	ULONG *attno_mapping = GPOS_NEW_ARRAY(mp, ULONG, size);

	for (ULONG ul = 0; ul < size; ul++)
	{
		attno_mapping[ul] = gpos::ulong_max;
	}

	for (ULONG ul = 0; ul < num_included_cols; ul++)
	{
		// TODO we do not have partition object in mdcache
		const IMDColumn *md_col = md_rel->GetMdCol(ul);

		INT attno = md_col->AttrNum();

		ULONG idx = (ULONG)(GPDXL_SYSTEM_COLUMNS + attno);
		GPOS_ASSERT(size > idx);
		attno_mapping[idx] = ul;
	}

	return attno_mapping;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveType
//
//	@doc:
//		Retrieve a type from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDType *
CTranslatorTBGPPToDXL::RetrieveType(CMemoryPool *mp, IMDId *mdid)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != oid_type);
	IMDType *tmp;

	// check for supported base types
	// TODO change this to our system
	switch (oid_type)
	{
		case GPDB_INT2_OID:
			// return GPOS_NEW(mp) CMDTypeInt2GPDB(mp);
			D_ASSERT(false);
			break;

		case GPDB_INT4_OID:
			// return GPOS_NEW(mp) CMDTypeInt4GPDB(mp);
			D_ASSERT(false);
			break;

		case GPDB_INT8_OID:
			return GPOS_NEW(mp) CMDTypeInt8GPDB(mp);
			break;

		case GPDB_BOOL:
			return GPOS_NEW(mp) CMDTypeBoolGPDB(mp);

		case GPDB_OID_OID:
			// return GPOS_NEW(mp) CMDTypeOidGPDB(mp);
			D_ASSERT(false);
			break;
	}

	// TODO
	// continue to construct a generic type
	// INT iFlags = TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
	// 			 TYPECACHE_CMP_PROC | TYPECACHE_EQ_OPR_FINFO |
	// 			 TYPECACHE_CMP_PROC_FINFO | TYPECACHE_TUPDESC;

	// TypeCacheEntry *ptce; // = gpdb::LookupTypeCache(oid_type, iFlags);

	// // get type name
	CMDName *mdname = GetTypeName(mp, mdid);

	BOOL is_fixed_length = false;
	ULONG length = 0;
	INT gpdb_length = 0;

	is_fixed_length = duckdb::isTypeFixedLength(oid_type);
	length = duckdb::GetTypeSize(oid_type);
	if (is_fixed_length) { // TODO tricky code
		gpdb_length = length;
	} else {
		gpdb_length = -1;
	}
	// if (0 < ptce->typlen)
	// {
	// 	is_fixed_length = true;
	// 	length = ptce->typlen;
	// }

	//BOOL is_passed_by_value = ptce->typbyval;
	BOOL is_passed_by_value = true; // TODO

	// collect ids of different comparison operators for types
	CMDIdGPDB *mdid_op_eq =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetComparisonOperator(oid_type, oid_type, CmptEq)/*ptce->eq_opr*/);
	CMDIdGPDB *mdid_op_neq = GPOS_NEW(mp)
		CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetComparisonOperator(oid_type, oid_type, CmptNEq)/*gpdb::GetInverseOp(ptce->eq_opr)*/);
	CMDIdGPDB *mdid_op_lt =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetComparisonOperator(oid_type, oid_type, CmptLT)/*ptce->lt_opr*/);
	CMDIdGPDB *mdid_op_leq = GPOS_NEW(mp)
		CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetComparisonOperator(oid_type, oid_type, CmptLEq)/*gpdb::GetInverseOp(ptce->gt_opr)*/);
	CMDIdGPDB *mdid_op_gt =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetComparisonOperator(oid_type, oid_type, CmptGT)/*ptce->gt_opr*/);
	CMDIdGPDB *mdid_op_geq = GPOS_NEW(mp)
		CMDIdGPDB(IMDId::EmdidGeneral, (OID) duckdb::GetComparisonOperator(oid_type, oid_type, CmptGEq)/*gpdb::GetInverseOp(ptce->lt_opr)*/);
	CMDIdGPDB *mdid_op_cmp =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, 0/*ptce->cmp_proc*/); // TODO what is this? B-tree lookup?
	BOOL is_hashable = true;//gpdb::IsOpHashJoinable(ptce->eq_opr, oid_type); //TODO
	BOOL is_merge_joinable = true;//gpdb::IsOpMergeJoinable(ptce->eq_opr, oid_type); //TODO
	BOOL is_composite_type = false;//gpdb::IsCompositeType(oid_type); //TODO do not consider this type in this step
	BOOL is_text_related_type = false;// TODO

	// get standard aggregates
	CMDIdGPDB *mdid_min = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetAggregate("min", oid_type, 1));
	CMDIdGPDB *mdid_max = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetAggregate("max", oid_type, 1));
	CMDIdGPDB *mdid_avg = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetAggregate("avg", oid_type, 1));
	CMDIdGPDB *mdid_sum = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, duckdb::GetAggregate("sum", oid_type, 1));

	// count aggregate is the same for all types
	CMDIdGPDB *mdid_count =
	 	GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, 2147/*COUNT_ANY_OID*/);

	// check if type is composite // TODO do not consider this type in this step
	CMDIdGPDB *mdid_type_relid = NULL;
	// if (is_composite_type)
	// {
	// 	mdid_type_relid = GPOS_NEW(mp)
	// 		CMDIdGPDB(IMDId::EmdidRel, gpdb::GetTypeRelid(oid_type));
	// }

	// get array type mdid
	CMDIdGPDB *mdid_type_array = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, 0/*gpdb::GetArrayType(oid_type)*/);

	OID distr_opfamily = 0;//gpdb::GetDefaultDistributionOpfamilyForType(oid_type);

	BOOL is_redistributable = false;
	CMDIdGPDB *mdid_distr_opfamily = NULL;
	// if (distr_opfamily != InvalidOid)
	// {
	// 	mdid_distr_opfamily =
	// 		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, distr_opfamily);
	// 	is_redistributable = true;
	// }

	CMDIdGPDB *mdid_legacy_distr_opfamily = NULL;
	// OID legacy_opclass = gpdb::GetLegacyCdbHashOpclassForBaseType(oid_type);
	// if (legacy_opclass != InvalidOid)
	// {
	// 	OID legacy_opfamily = gpdb::GetOpclassFamily(legacy_opclass);
	// 	mdid_legacy_distr_opfamily =
	// 		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, legacy_opfamily);
	// }

	mdid->AddRef();
	return GPOS_NEW(mp) CMDTypeGenericGPDB(
	 	mp, mdid, mdname, is_redistributable, is_fixed_length, length,
	 	is_passed_by_value, mdid_distr_opfamily, mdid_legacy_distr_opfamily,
	 	mdid_op_eq, mdid_op_neq, mdid_op_lt, mdid_op_leq, mdid_op_gt,
	 	mdid_op_geq, mdid_op_cmp, mdid_min, mdid_max, mdid_avg, mdid_sum,
	 	mdid_count, is_hashable, is_merge_joinable, is_composite_type,
	 	is_text_related_type, mdid_type_relid, mdid_type_array, gpdb_length/*ptce->typlen*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveScOp
//
//	@doc:
//		Retrieve a scalar operator from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDScalarOpGPDB *
CTranslatorTBGPPToDXL::RetrieveScOp(CMemoryPool *mp, IMDId *mdid)
{
	OID op_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != op_oid);

	// get operator name
	string name_str = duckdb::GetOpName(op_oid);
	CHAR *name = std::strcpy(new char[name_str.length() + 1], name_str.c_str()); // TODO avoid copy?

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, name);

	OID left_oid = InvalidOid;
	OID right_oid = InvalidOid;

	// get operator argument types
	duckdb::GetOpInputTypes(op_oid, &left_oid, &right_oid);

	CMDIdGPDB *mdid_type_left = NULL;
	CMDIdGPDB *mdid_type_right = NULL;

	if (InvalidOid != left_oid)
	{
		mdid_type_left = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, left_oid);
	}

	if (InvalidOid != right_oid)
	{
		mdid_type_right =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, right_oid);
	}

	// get comparison type
	CmpType cmpt = (CmpType) duckdb::GetComparisonType(op_oid);
	IMDType::ECmpType cmp_type = ParseCmpType(cmpt);

	// get func oid
	OID func_oid = duckdb::GetOpFunc(op_oid);
	GPOS_ASSERT(InvalidOid != func_oid);

	CMDIdGPDB *mdid_func =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, func_oid);

	// get result type
	OID result_oid = LOGICAL_TYPE_BASE_ID + (OID) LogicalTypeId::BOOLEAN;// TODO = gpdb::GetFuncRetType(func_oid);

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);

	// get commutator and inverse
	CMDIdGPDB *mdid_commute_opr = NULL;

	OID commute_oid = duckdb::GetCommutatorOp(op_oid);

	if (InvalidOid != commute_oid)
	{
		mdid_commute_opr =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, commute_oid);
	}

	CMDIdGPDB *m_mdid_inverse_opr = NULL;

	OID inverse_oid = duckdb::GetInverseOp(op_oid);

	if (InvalidOid != inverse_oid)
	{
		m_mdid_inverse_opr =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, inverse_oid);
	}

	BOOL returns_null_on_null_input = true;//TODO = gpdb::IsOpStrict(op_oid);
	BOOL is_ndv_preserving = true;//TODO = gpdb::IsOpNDVPreserving(op_oid);

	CMDIdGPDB *mdid_hash_opfamily = NULL;
	OID distr_opfamily;//TODO = gpdb::GetCompatibleHashOpFamily(op_oid);
	if (InvalidOid != distr_opfamily)
	{
		mdid_hash_opfamily =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, distr_opfamily);
	}

	CMDIdGPDB *mdid_legacy_hash_opfamily = NULL;
	OID legacy_distr_opfamily;//TODO = gpdb::GetCompatibleLegacyHashOpFamily(op_oid);
	if (InvalidOid != legacy_distr_opfamily)
	{
		mdid_legacy_hash_opfamily =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, legacy_distr_opfamily);
	}

	mdid->AddRef();
	CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(mp) CMDScalarOpGPDB(
		mp, mdid, mdname, mdid_type_left, mdid_type_right, result_type_mdid,
		mdid_func, mdid_commute_opr, m_mdid_inverse_opr, cmp_type,
		returns_null_on_null_input, RetrieveScOpOpFamilies(mp, mdid),
		mdid_hash_opfamily, mdid_legacy_hash_opfamily, is_ndv_preserving);
	return md_scalar_op;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::LookupFuncProps
//
//	@doc:
//		Lookup function properties
//
//---------------------------------------------------------------------------
void
CTranslatorTBGPPToDXL::LookupFuncProps(
	OID func_oid,
	IMDFunction::EFuncStbl *stability,	// output: function stability
	IMDFunction::EFuncDataAcc *access,	// output: function datya access
	BOOL *is_strict,					// output: is function strict?
	BOOL *is_ndv_preserving,			// output: preserves NDVs of inputs
	BOOL *returns_set,					// output: does function return set?
	BOOL *
		is_allowed_for_PS  // output: is this a lossy (non-implicit) cast which is allowed for Partition selection
)
{
	GPOS_ASSERT(NULL != stability);
	GPOS_ASSERT(NULL != access);
	GPOS_ASSERT(NULL != is_strict);
	GPOS_ASSERT(NULL != is_ndv_preserving);
	GPOS_ASSERT(NULL != returns_set);

	// *stability = GetFuncStability(gpdb::FuncStability(func_oid));
	// *access = GetEFuncDataAccess(gpdb::FuncDataAccess(func_oid));

	// if (gpdb::FuncExecLocation(func_oid) != PROEXECLOCATION_ANY)
	// 	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
	// 			   GPOS_WSZ_LIT("unsupported exec location"));

	// *returns_set = gpdb::GetFuncRetset(func_oid);
	// *is_strict = gpdb::FuncStrict(func_oid);
	// *is_ndv_preserving = gpdb::IsFuncNDVPreserving(func_oid);
	// *is_allowed_for_PS = gpdb::IsFuncAllowedForPartitionSelection(func_oid);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveFunc
//
//	@doc:
//		Retrieve a function from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDFunctionGPDB *
CTranslatorTBGPPToDXL::RetrieveFunc(CMemoryPool *mp, IMDId *mdid)
{
	OID func_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != func_oid);

	// get aggfunc catalog entry
	duckdb::ScalarFunctionCatalogEntry *scalar_func_cat =
		duckdb::GetScalarFunc(func_oid);

	// get func name
	string name_str = scalar_func_cat->GetName();
	CHAR *name = std::strcpy(new char[name_str.length() + 1], name_str.c_str());

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	CWStringDynamic *func_name_str =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, func_name_str);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(func_name_str);

	// get result type
	idx_t scalar_func_idx = duckdb::GetScalarFuncIndex(func_oid);
	GPOS_ASSERT(scalar_func_cat->functions->functions.size() > scalar_func_idx);
	OID result_oid = LOGICAL_TYPE_BASE_ID + (OID) scalar_func_cat->functions->functions[scalar_func_idx].return_type.id();
	
	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);

	// get output argument types if any
	// List *out_arg_types_list = gpdb::GetFuncOutputArgTypes(func_oid);

	IMdIdArray *arg_type_mdids = NULL;
	// if (NULL != out_arg_types_list)
	// {
	// 	ListCell *lc = NULL;
	// 	arg_type_mdids = GPOS_NEW(mp) IMdIdArray(mp);

	// 	ForEach(lc, out_arg_types_list)
	// 	{
	// 		OID oidArgType = lfirst_oid(lc);
	// 		GPOS_ASSERT(InvalidOid != oidArgType);
	// 		CMDIdGPDB *pmdidArgType =
	// 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, oidArgType);
	// 		arg_type_mdids->Append(pmdidArgType);
	// 	}

	// 	gpdb::GPDBFree(out_arg_types_list);
	// }

	IMDFunction::EFuncStbl stability = IMDFunction::EfsImmutable;
	IMDFunction::EFuncDataAcc access = IMDFunction::EfdaNoSQL;
	BOOL is_strict = true;
	BOOL returns_set = true;
	BOOL is_ndv_preserving = true;
	BOOL is_allowed_for_PS = false;
	// LookupFuncProps(func_oid, &stability, &access, &is_strict,
	// 				&is_ndv_preserving, &returns_set, &is_allowed_for_PS);

	mdid->AddRef();
	CMDFunctionGPDB *md_func = GPOS_NEW(mp) CMDFunctionGPDB(
		mp, mdid, mdname, result_type_mdid, arg_type_mdids, returns_set,
		stability, access, is_strict, is_ndv_preserving, is_allowed_for_PS);

	return md_func;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveAgg
//
//	@doc:
//		Retrieve an aggregate from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDAggregateGPDB *
CTranslatorTBGPPToDXL::RetrieveAgg(CMemoryPool *mp, IMDId *mdid)
{

	OID agg_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != agg_oid);

	// get aggfunc catalog entry
	duckdb::AggregateFunctionCatalogEntry *agg_func_cat =
		duckdb::GetAggFunc(agg_oid);

	// get agg name
	string name_str = agg_func_cat->GetName();
	CHAR *name = std::strcpy(new char[name_str.length() + 1], name_str.c_str());

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	CWStringDynamic *agg_name_str =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, agg_name_str);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(agg_name_str);

	// get result type
	idx_t agg_func_idx = duckdb::GetAggFuncIndex(agg_oid);
	GPOS_ASSERT(agg_func_cat->functions->functions.size() > agg_func_idx);
	OID result_oid = LOGICAL_TYPE_BASE_ID + (OID) agg_func_cat->functions->functions[agg_func_idx].return_type.id();

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);
	IMDId *intermediate_result_type_mdid = // S62 TODO we need this when we use local aggregate
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);
		// RetrieveAggIntermediateResultType(mp, mdid);

	mdid->AddRef();

	BOOL is_ordered = false;// = gpdb::IsOrderedAgg(agg_oid); // S62 TODO

	// GPDB does not support splitting of ordered aggs and aggs without a
	// combine function
	BOOL is_splittable = false;//!is_ordered && gpdb::IsAggPartialCapable(agg_oid);

	// cannot use hash agg for ordered aggs or aggs without a combine func
	// due to the fact that hashAgg may spill
	BOOL is_hash_agg_capable = true;// = // S62 TODO
		// !is_ordered && gpdb::IsAggPartialCapable(agg_oid);

	CMDAggregateGPDB *pmdagg = GPOS_NEW(mp) CMDAggregateGPDB(
		mp, mdid, mdname, result_type_mdid, intermediate_result_type_mdid,
		is_ordered, is_splittable, is_hash_agg_capable);
	return pmdagg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveTrigger
//
//	@doc:
//		Retrieve a trigger from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDTriggerGPDB *
CTranslatorTBGPPToDXL::RetrieveTrigger(CMemoryPool *mp, IMDId *mdid)
{
	D_ASSERT(false);
	return nullptr; // TODO Disable currently
	// OID trigger_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	// GPOS_ASSERT(InvalidOid != trigger_oid);

	// // get trigger name
	// CHAR *name = gpdb::GetTriggerName(trigger_oid);

	// if (NULL == name)
	// {
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
	// 			   mdid->GetBuffer());
	// }

	// CWStringDynamic *trigger_name_str =
	// 	CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	// CMDName *mdname = GPOS_NEW(mp) CMDName(mp, trigger_name_str);
	// GPOS_DELETE(trigger_name_str);

	// // get relation oid
	// OID rel_oid = gpdb::GetTriggerRelid(trigger_oid);
	// GPOS_ASSERT(InvalidOid != rel_oid);
	// CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, rel_oid);

	// // get function oid
	// OID func_oid = gpdb::GetTriggerFuncid(trigger_oid);
	// GPOS_ASSERT(InvalidOid != func_oid);
	// CMDIdGPDB *mdid_func =
	// 	GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, func_oid);

	// // get type
	// INT trigger_type = gpdb::GetTriggerType(trigger_oid);

	// // is trigger enabled
	// BOOL is_enabled = gpdb::IsTriggerEnabled(trigger_oid);

	// mdid->AddRef();
	// CMDTriggerGPDB *pmdtrigger = GPOS_NEW(mp) CMDTriggerGPDB(
	// 	mp, mdid, mdname, mdid_rel, mdid_func, trigger_type, is_enabled);
	// return pmdtrigger;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveCheckConstraints
//
//	@doc:
//		Retrieve a check constraint from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDCheckConstraintGPDB *
CTranslatorTBGPPToDXL::RetrieveCheckConstraints(CMemoryPool *mp,
												   CMDAccessor *md_accessor,
												   IMDId *mdid)
{
	OID check_constraint_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != check_constraint_oid);

	// get name of the check constraint
	CHAR *name; // = gpdb::GetCheckConstraintName(check_constraint_oid);
	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}
	CWStringDynamic *check_constr_name =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, name);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, check_constr_name);
	GPOS_DELETE(check_constr_name);

	// get relation oid associated with the check constraint
	OID rel_oid; // = gpdb::GetCheckConstraintRelid(check_constraint_oid);
	GPOS_ASSERT(InvalidOid != rel_oid);
	CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, rel_oid);

	// translate the check constraint expression
	// Node *node = gpdb::PnodeCheckConstraint(check_constraint_oid);
	// GPOS_ASSERT(NULL != node);

	// // generate a mock mapping between var to column information
	// CMappingVarColId *var_colid_mapping = GPOS_NEW(mp) CMappingVarColId(mp);
	// CDXLColDescrArray *dxl_col_descr_array = GPOS_NEW(mp) CDXLColDescrArray(mp);
	// const IMDRelation *md_rel = md_accessor->RetrieveRel(mdid_rel);
	// const ULONG length = md_rel->ColumnCount();
	// for (ULONG ul = 0; ul < length; ul++)
	// {
	// 	const IMDColumn *md_col = md_rel->GetMdCol(ul);
	// 	CMDName *md_colname =
	// 		GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
	// 	CMDIdGPDB *mdid_col_type = CMDIdGPDB::CastMdid(md_col->MdidType());
	// 	mdid_col_type->AddRef();

	// 	// create a column descriptor for the column
	// 	CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
	// 		mp, md_colname, ul + 1 /*colid*/, md_col->AttrNum(), mdid_col_type,
	// 		md_col->TypeModifier(), false /* fColDropped */
	// 	);
	// 	dxl_col_descr_array->Append(dxl_col_descr);
	// }
	// var_colid_mapping->LoadColumns(0 /*query_level */, 1 /* rteIndex */,
	// 							   dxl_col_descr_array);

	// translate the check constraint expression
	CDXLNode *scalar_dxlnode;// =
		// CTranslatorScalarToDXL::TranslateStandaloneExprToDXL(
		// 	mp, md_accessor, var_colid_mapping, (Expr *) node);

	// cleanup
	// dxl_col_descr_array->Release();
	// GPOS_DELETE(var_colid_mapping);

	mdid->AddRef();

	return GPOS_NEW(mp)
		CMDCheckConstraintGPDB(mp, mdid, mdname, mdid_rel, scalar_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::GetTypeName
//
//	@doc:
//		Retrieve a type's name from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDName *
CTranslatorTBGPPToDXL::GetTypeName(CMemoryPool *mp, IMDId *mdid)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != oid_type);

	CHAR *typename_str = std::strcpy(new char[duckdb::GetTypeName(oid_type).length() + 1], duckdb::GetTypeName(oid_type).c_str());
	//CHAR *typename_str = const_cast<char *>(duckdb::GetTypeName(oid_type).c_str());
	GPOS_ASSERT(NULL != typename_str);

	CWStringDynamic *str_name =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, typename_str);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::GetFuncStability
//
//	@doc:
//		Get function stability property from the GPDB character representation
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncStbl
CTranslatorTBGPPToDXL::GetFuncStability(CHAR c)
{
	CMDFunctionGPDB::EFuncStbl efuncstbl = CMDFunctionGPDB::EfsSentinel;

	switch (c)
	{
		case 's':
			efuncstbl = CMDFunctionGPDB::EfsStable;
			break;
		case 'i':
			efuncstbl = CMDFunctionGPDB::EfsImmutable;
			break;
		case 'v':
			efuncstbl = CMDFunctionGPDB::EfsVolatile;
			break;
		default:
			GPOS_ASSERT(!"Invalid stability property");
	}

	return efuncstbl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::GetEFuncDataAccess
//
//	@doc:
//		Get function data access property from the GPDB character representation
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncDataAcc
CTranslatorTBGPPToDXL::GetEFuncDataAccess(CHAR c)
{
	CMDFunctionGPDB::EFuncDataAcc access = CMDFunctionGPDB::EfdaSentinel;

	switch (c)
	{
		case 'n':
			access = CMDFunctionGPDB::EfdaNoSQL;
			break;
		case 'c':
			access = CMDFunctionGPDB::EfdaContainsSQL;
			break;
		case 'r':
			access = CMDFunctionGPDB::EfdaReadsSQLData;
			break;
		case 'm':
			access = CMDFunctionGPDB::EfdaModifiesSQLData;
			break;
		case 's':
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("unknown data access"));
		default:
			GPOS_ASSERT(!"Invalid data access property");
	}

	return access;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveAggIntermediateResultType
//
//	@doc:
//		Retrieve the type id of an aggregate's intermediate results
//
//---------------------------------------------------------------------------
IMDId *
CTranslatorTBGPPToDXL::RetrieveAggIntermediateResultType(CMemoryPool *mp,
															IMDId *mdid)
{
	OID agg_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	OID intermediate_type_oid;

	GPOS_ASSERT(InvalidOid != agg_oid);
	duckdb::AggregateFunctionCatalogEntry *agg_func_cat = duckdb::GetAggFunc(agg_oid);
	intermediate_type_oid = GPDB_BOOL; // S62 TODO temporary.. maybe we don't use this info in this step
	// intermediate_type_oid = gpdb::GetAggIntermediateResultType(agg_oid);

	/*
	 * If the transition type is 'internal', we will use the
	 * serial/deserial type to convert it to a bytea, for transfer
	 * between the segments. Therefore return 'bytea' as the
	 * intermediate type, so that any Motion nodes in between use the
	 * right datatype.
	 */
	// if (intermediate_type_oid == INTERNALOID)
	// 	intermediate_type_oid = BYTEAOID;

	return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, intermediate_type_oid);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelStats
//
//	@doc:
//		Retrieve relation statistics from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorTBGPPToDXL::RetrieveRelStats(CMemoryPool *mp, IMDId *mdid)
{
	CMDIdRelStats *m_rel_stats_mdid = CMDIdRelStats::CastMdid(mdid);
	IMDId *mdid_rel = m_rel_stats_mdid->GetRelMdId();
	OID rel_oid = CMDIdGPDB::CastMdid(mdid_rel)->Oid();

	PropertySchemaCatalogEntry *rel = duckdb::GetRelation(rel_oid);
	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	double num_rows = 0.0;
	CMDName *mdname = NULL;
	ULONG relpages = 0;
	ULONG relallvisible = 0;

	GPOS_TRY
	{
		// get rel name
		CHAR *relname = std::strcpy(new char[rel->GetName().length() + 1], rel->GetName().c_str()); // TODO free
		// CHAR *relname = const_cast<CHAR *>(rel->GetName().c_str());
		CWStringDynamic *relname_str =
			CDXLUtils::CreateDynamicStringFromCharArray(mp, relname);
		mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
		// CMDName ctor created a copy of the string
		GPOS_DELETE(relname_str);

		num_rows = rel->GetNumberOfRowsApproximately();// gpdb::CdbEstimatePartitionedNumTuples(rel);

		// relpages = rel->rd_rel->relpages;
		// relallvisible = rel->rd_rel->relallvisible;

		m_rel_stats_mdid->AddRef();
		// gpdb::CloseRelation(rel);
	}
	GPOS_CATCH_EX(ex)
	{
		// gpdb::CloseRelation(rel);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	/*
	 * relation_empty should be set to true only if the total row
	 * count of the partition table is 0.
	 */
	BOOL relation_empty = false;
	if (num_rows == 0.0)
	{
		relation_empty = true;
	}

	CDXLRelStats *dxl_rel_stats = GPOS_NEW(mp)
		CDXLRelStats(mp, m_rel_stats_mdid, mdname, CDouble(num_rows),
					 relation_empty, relpages, relallvisible);

	return dxl_rel_stats;
}

// Retrieve column statistics from relcache
// If all statistics are missing, create dummy statistics
// Also, if the statistics are broken, create dummy statistics
// However, if any statistics are present and not broken,
// create column statistics using these statistics
IMDCacheObject *
CTranslatorTBGPPToDXL::RetrieveColStats(CMemoryPool *mp,
										   CMDAccessor *md_accessor,
										   IMDId *mdid)
{
	CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);
	IMDId *mdid_rel = mdid_col_stats->GetRelMdId();
	ULONG pos = mdid_col_stats->Position();
	OID rel_oid = CMDIdGPDB::CastMdid(mdid_rel)->Oid();

	duckdb::PropertySchemaCatalogEntry *rel = duckdb::GetRelation(rel_oid);
	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	const IMDRelation *md_rel = md_accessor->RetrieveRel(mdid_rel);
	const IMDColumn *md_col = md_rel->GetMdCol(pos);
	AttrNumber attno = (AttrNumber) md_col->AttrNum();

	// number of rows from pg_class
	double num_rows = 0.0;

	num_rows = rel->GetNumberOfRowsApproximately();// gpdb::CdbEstimatePartitionedNumTuples(rel);

	// extract column name and type
	CMDName *md_colname =
		GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
	OID att_type = CMDIdGPDB::CastMdid(md_col->MdidType())->Oid();
	//gpdb::CloseRelation(rel);

	CDXLBucketArray *dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);

	if (0 > attno)
	{
		mdid_col_stats->AddRef();
		return GenerateStatsForSystemCols(mp, rel_oid, mdid_col_stats,
										  md_colname, att_type, attno,
										  dxl_stats_bucket_array, num_rows);
	}

	// // extract out histogram and mcv information from pg_statistic
	// HeapTuple stats_tup = gpdb::GetAttStats(rel_oid, attno);

	// // if there is no colstats
	// if (!HeapTupleIsValid(stats_tup))
	// {
	// 	dxl_stats_bucket_array->Release();
	// 	mdid_col_stats->AddRef();

	// 	CDouble width = CStatistics::DefaultColumnWidth;

	// 	if (!md_col->IsDropped())
	// 	{
	// 		CMDIdGPDB *mdid_atttype =
	// 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, att_type);
	// 		IMDType *md_type = RetrieveType(mp, mdid_atttype);
	// 		width = CStatisticsUtils::DefaultColumnWidth(md_type);
	// 		md_type->Release();
	// 		mdid_atttype->Release();
	// 	}

	// 	return CDXLColStats::CreateDXLDummyColStats(mp, mdid_col_stats,
	// 												md_colname, width);
	// }

	// Form_pg_statistic form_pg_stats = (Form_pg_statistic) GETSTRUCT(stats_tup);

	// null frequency and NDV
	CDouble null_freq(0.0);
	int null_ndv = 0;
	// if (CStatistics::Epsilon < form_pg_stats->stanullfrac)
	// {
	// 	null_freq = form_pg_stats->stanullfrac;
	// 	null_ndv = 1;
	// }

	// column width
	idx_t width_penalty = 1;//rel->GetName().rfind("vps", 0) == 0 ? 1 : 100L; // S62 TODO.. avoid edge table scan
	CDouble width = CDouble(width_penalty * duckdb::GetTypeSize(att_type));// = CDouble(form_pg_stats->stawidth);
	if(rel->is_fake) {
		// TODO: change hard coded value
		// 500 means the number of schemas.
		// This is for scaling the width for the temporal table
		width = width / 500;
	}

	// calculate total number of distinct values
	CDouble num_distinct(1.0);
	num_distinct = CDouble(duckdb::GetNDV(rel, attno));
	// if (form_pg_stats->stadistinct < 0)
	// {
	// 	GPOS_ASSERT(form_pg_stats->stadistinct > -1.01);
	// 	num_distinct =
	// 		num_rows * (1 - null_freq) * CDouble(-form_pg_stats->stadistinct);
	// }
	// else
	// {
	// 	num_distinct = CDouble(form_pg_stats->stadistinct);
	// }
	num_distinct = num_distinct.Ceil();

	BOOL is_dummy_stats = false;
	// most common values and their frequencies extracted from the pg_statistic
	// tuple for a given column
	// AttStatsSlot mcv_slot;

	// (void) gpdb::GetAttrStatsSlot(&mcv_slot, stats_tup, STATISTIC_KIND_MCV,
	// 							  InvalidOid,
	// 							  ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
	// if (InvalidOid != mcv_slot.valuetype && mcv_slot.valuetype != att_type)
	// {
	// 	char msgbuf[NAMEDATALEN * 2 + 100];
	// 	snprintf(
	// 		msgbuf, sizeof(msgbuf),
	// 		"Type mismatch between attribute %ls of table %ls having type %d and statistic having type %d, please ANALYZE the table again",
	// 		md_col->Mdname().GetMDName()->GetBuffer(),
	// 		md_rel->Mdname().GetMDName()->GetBuffer(), att_type,
	// 		mcv_slot.valuetype);
	// 	GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION, NOTICE, msgbuf, NULL);

	// 	gpdb::FreeAttrStatsSlot(&mcv_slot);
	// 	is_dummy_stats = true;
	// }

	// else if (mcv_slot.nvalues != mcv_slot.nnumbers)
	// {
	// 	char msgbuf[NAMEDATALEN * 2 + 100];
	// 	snprintf(
	// 		msgbuf, sizeof(msgbuf),
	// 		"The number of most common values and frequencies do not match on column %ls of table %ls.",
	// 		md_col->Mdname().GetMDName()->GetBuffer(),
	// 		md_rel->Mdname().GetMDName()->GetBuffer());
	// 	GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION, NOTICE, msgbuf, NULL);

	// 	// if the number of MCVs(nvalues) and number of MCFs(nnumbers) do not match, we discard the MCVs and MCFs
	// 	gpdb::FreeAttrStatsSlot(&mcv_slot);
	// 	is_dummy_stats = true;
	// }
	// else
	// {
	// 	// fix mcv and null frequencies (sometimes they can add up to more than 1.0)
	// 	NormalizeFrequencies(mcv_slot.numbers, (ULONG) mcv_slot.nvalues,
	// 						 &null_freq);

	// 	// total MCV frequency
	// 	CDouble sum_mcv_freq = 0.0;
	// 	for (int i = 0; i < mcv_slot.nvalues; i++)
	// 	{
	// 		sum_mcv_freq = sum_mcv_freq + CDouble(mcv_slot.numbers[i]);
	// 	}
	// }

	// histogram values extracted from the pg_statistic tuple for a given column
	AttStatsSlot hist_slot;

	// get histogram datums from pg_statistic entry
	// (void) gpdb::GetAttrStatsSlot(&hist_slot, stats_tup,
	// 							  STATISTIC_KIND_HISTOGRAM, InvalidOid,
	// 							  ATTSTATSSLOT_VALUES);
	duckdb::GetHistogramInfo(rel, attno, &hist_slot);

	if (InvalidOid != hist_slot.valuetype && hist_slot.valuetype != att_type)
	{
		// char msgbuf[NAMEDATALEN * 2 + 100];
		// snprintf(
		// 	msgbuf, sizeof(msgbuf),
		// 	"Type mismatch between attribute %ls of table %ls having type %d and statistic having type %d, please ANALYZE the table again",
		// 	md_col->Mdname().GetMDName()->GetBuffer(),
		// 	md_rel->Mdname().GetMDName()->GetBuffer(), att_type,
		// 	hist_slot.valuetype);
		// GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION, NOTICE, msgbuf, NULL);

		// TODO release hist_slot
		// gpdb::FreeAttrStatsSlot(&hist_slot);
		is_dummy_stats = true;
	}

	// s62 added
	if (InvalidOid == hist_slot.valuetype)
	{
		is_dummy_stats = true;
	}

	if (is_dummy_stats)
	{
		dxl_stats_bucket_array->Release();
		mdid_col_stats->AddRef();

		CDouble col_width = CStatistics::DefaultColumnWidth;
		if(rel->is_fake) {
			// TODO: change hard coded value
			// 500 means the number of schemas.
			// This is for scaling the width for the temporal table
			col_width = col_width / 500;
		}
		// gpdb::FreeHeapTuple(stats_tup);
		return CDXLColStats::CreateDXLDummyColStats(mp, mdid_col_stats,
													md_colname, col_width);
	}

	CDouble num_ndv_buckets(0.0);
	CDouble num_freq_buckets(0.0);
	CDouble distinct_remaining(0.0);
	CDouble freq_remaining(0.0);

	// transform all the bits and pieces from pg_statistic
	// to a single bucket structure
	CDXLBucketArray *dxl_stats_bucket_array_transformed =
		TransformStatsToDXLBucketArray(
			mp, att_type, num_distinct, null_freq, NULL /*mcv_slot.values*/,
			NULL /*mcv_slot.numbers*/, 0 /*ULONG(mcv_slot.nvalues)*/, hist_slot.values,
			hist_slot.freq_values, ULONG(hist_slot.nvalues));

	GPOS_ASSERT(NULL != dxl_stats_bucket_array_transformed);

	const ULONG num_buckets = dxl_stats_bucket_array_transformed->Size();
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		CDXLBucket *dxl_bucket = (*dxl_stats_bucket_array_transformed)[ul];
		num_ndv_buckets = num_ndv_buckets + dxl_bucket->GetNumDistinct();
		num_freq_buckets = num_freq_buckets + dxl_bucket->GetFrequency();
	}

	CUtils::AddRefAppend(dxl_stats_bucket_array,
						 dxl_stats_bucket_array_transformed);
	dxl_stats_bucket_array_transformed->Release();

	// there will be remaining tuples if the merged histogram and the NULLS do not cover
	// the total number of distinct values
	if ((1 - CStatistics::Epsilon > num_freq_buckets + null_freq) &&
		(0 < num_distinct - num_ndv_buckets))
	{
		distinct_remaining =
			std::max(CDouble(0.0), (num_distinct - num_ndv_buckets));
		freq_remaining =
			std::max(CDouble(0.0), (1 - num_freq_buckets - null_freq));
	}

	// // free up allocated datum and float4 arrays
	// gpdb::FreeAttrStatsSlot(&mcv_slot);
	// gpdb::FreeAttrStatsSlot(&hist_slot);

	// gpdb::FreeHeapTuple(stats_tup);

	// create col stats object
	mdid_col_stats->AddRef();
	CDXLColStats *dxl_col_stats = GPOS_NEW(mp) CDXLColStats(
		mp, mdid_col_stats, md_colname, width, null_freq, distinct_remaining,
		freq_remaining, dxl_stats_bucket_array, false /* is_col_stats_missing */
	);

	return dxl_col_stats;
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorTBGPPToDXL::GenerateStatsForSystemCols
//
//      @doc:
//              Generate statistics for the system level columns
//
//---------------------------------------------------------------------------
CDXLColStats *
CTranslatorTBGPPToDXL::GenerateStatsForSystemCols(
	CMemoryPool *mp, OID rel_oid, CMDIdColStats *mdid_col_stats,
	CMDName *md_colname, OID att_type, AttrNumber attno,
	CDXLBucketArray *dxl_stats_bucket_array, CDouble num_rows)
{
	GPOS_ASSERT(NULL != mdid_col_stats);
	GPOS_ASSERT(NULL != md_colname);
	GPOS_ASSERT(InvalidOid != att_type);
	GPOS_ASSERT(0 > attno);
	GPOS_ASSERT(NULL != dxl_stats_bucket_array);

	CMDIdGPDB *mdid_atttype =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, att_type);
	IMDType *md_type = RetrieveType(mp, mdid_atttype);
	GPOS_ASSERT(md_type->IsFixedLength());

	BOOL is_col_stats_missing = true;
	CDouble null_freq(0.0);
	CDouble width(md_type->Length());
	CDouble distinct_remaining(0.0);
	CDouble freq_remaining(0.0);

	if (CStatistics::MinRows <= num_rows)
	{
		switch (attno)
		{
			case GpSegmentIdAttributeNumber:  // gp_segment_id
			{
				is_col_stats_missing = false;
				freq_remaining = CDouble(1.0);
				distinct_remaining = CDouble(0.0);//(gpdb::GetGPSegmentCount());
				break;
			}
			case TableOidAttributeNumber:  // tableoid
			{
				is_col_stats_missing = false;
				freq_remaining = CDouble(1.0);
				distinct_remaining =
					CDouble(RetrieveNumChildPartitions(rel_oid));
				break;
			}
			case SelfItemPointerAttributeNumber:  // ctid
			{
				is_col_stats_missing = false;
				freq_remaining = CDouble(1.0);
				distinct_remaining = num_rows;
				break;
			}
			default:
				break;
		}
	}

	// cleanup
	mdid_atttype->Release();
	md_type->Release();

	return GPOS_NEW(mp) CDXLColStats(
		mp, mdid_col_stats, md_colname, width, null_freq, distinct_remaining,
		freq_remaining, dxl_stats_bucket_array, is_col_stats_missing);
}


//---------------------------------------------------------------------------
//     @function:
//     CTranslatorTBGPPToDXL::RetrieveNumChildPartitions
//
//  @doc:
//      For non-leaf partition tables return the number of child partitions
//      else return 1
//
//---------------------------------------------------------------------------
ULONG
CTranslatorTBGPPToDXL::RetrieveNumChildPartitions(OID rel_oid)
{
	GPOS_ASSERT(InvalidOid != rel_oid);

	ULONG num_part_tables = gpos::ulong_max;
	// if (gpdb::RelPartIsNone(rel_oid))
	// {
		// not a partitioned table
		num_part_tables = 1;
	// }
	// else if (gpdb::IsLeafPartition(rel_oid))
	// {
	// 	// leaf partition
	// 	num_part_tables = 1;
	// }
	// else
	// {
	// 	num_part_tables = gpdb::CountLeafPartTables(rel_oid);
	// }
	// GPOS_ASSERT(gpos::ulong_max != num_part_tables);

	return num_part_tables;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveCast
//
//	@doc:
//		Retrieve a cast function from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorTBGPPToDXL::RetrieveCast(CMemoryPool *mp, IMDId *mdid)
{
	// CMDIdCast *mdid_cast = CMDIdCast::CastMdid(mdid);
	// IMDId *mdid_src = mdid_cast->MdidSrc();
	// IMDId *mdid_dest = mdid_cast->MdidDest();
	// IMDCast::EmdCoercepathType coercePathType;

	// OID src_oid = CMDIdGPDB::CastMdid(mdid_src)->Oid();
	// OID dest_oid = CMDIdGPDB::CastMdid(mdid_dest)->Oid();
	// CoercionPathType pathtype;

	// OID cast_fn_oid = 0;
	// BOOL is_binary_coercible = false;

	// BOOL cast_exists = false; //gpdb::GetCastFunc(
	// 	// src_oid, dest_oid, &is_binary_coercible, &cast_fn_oid, &pathtype);

	// if (!cast_exists)
	// {
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
	// 			   mdid->GetBuffer());
	// }

	// CHAR *func_name = NULL;
	// // if (InvalidOid != cast_fn_oid)
	// // {
	// // 	func_name = gpdb::GetFuncName(cast_fn_oid);
	// // }
	// // else
	// // {
	// // 	// no explicit cast function: use the destination type name as the cast name
	// // 	func_name = gpdb::GetTypeName(dest_oid);
	// // }

	// if (NULL == func_name)
	// {
	// 	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
	// 			   mdid->GetBuffer());
	// }

	// mdid->AddRef();
	// mdid_src->AddRef();
	// mdid_dest->AddRef();

	// CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, func_name);

	// // switch (pathtype)
	// // {
	// // 	case COERCION_PATH_ARRAYCOERCE:
	// // 	{
	// // 		coercePathType = IMDCast::EmdtArrayCoerce;
	// // 		return GPOS_NEW(mp) CMDArrayCoerceCastGPDB(
	// // 			mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible,
	// // 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, cast_fn_oid),
	// // 			IMDCast::EmdtArrayCoerce, default_type_modifier, false,
	// // 			EdxlcfImplicitCast, -1);
	// // 	}
	// // 	break;
	// // 	case COERCION_PATH_FUNC:
	// // 		return GPOS_NEW(mp) CMDCastGPDB(
	// // 			mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible,
	// // 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, cast_fn_oid),
	// // 			IMDCast::EmdtFunc);
	// // 		break;
	// // 	case COERCION_PATH_RELABELTYPE:
	// // 		// binary-compatible cast, no function
	// // 		GPOS_ASSERT(cast_fn_oid == 0);
	// // 		return GPOS_NEW(mp) CMDCastGPDB(
	// // 			mp, mdid, mdname, mdid_src, mdid_dest,
	// // 			true /*is_binary_coercible*/,
	// // 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, cast_fn_oid));
	// // 		break;
	// // 	case COERCION_PATH_COERCEVIAIO:
	// // 		// uses IO functions from types, no function in the cast
	// // 		GPOS_ASSERT(cast_fn_oid == 0);
	// // 		return GPOS_NEW(mp) CMDCastGPDB(
	// // 			mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible,
	// // 			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, cast_fn_oid),
	// // 			IMDCast::EmdtCoerceViaIO);
	// // 	default:
	// // 		break;
	// // }

	// // fall back for none path types
	// return GPOS_NEW(mp)
	// 	CMDCastGPDB(mp, mdid, mdname, mdid_src, mdid_dest, is_binary_coercible,
	// 				GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, cast_fn_oid));

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveScCmp
//
//	@doc:
//		Retrieve a scalar comparison from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorTBGPPToDXL::RetrieveScCmp(CMemoryPool *mp, IMDId *mdid)
{
	CMDIdScCmp *mdid_scalar_cmp = CMDIdScCmp::CastMdid(mdid);
	IMDId *mdid_left = mdid_scalar_cmp->GetLeftMdid();
	IMDId *mdid_right = mdid_scalar_cmp->GetRightMdid();

	IMDType::ECmpType cmp_type = mdid_scalar_cmp->ParseCmpType();

	OID left_oid = CMDIdGPDB::CastMdid(mdid_left)->Oid();
	OID right_oid = CMDIdGPDB::CastMdid(mdid_right)->Oid();
	CmpType cmpt = (CmpType) GetComparisonType(cmp_type);

	OID scalar_cmp_oid = duckdb::GetComparisonOperator(left_oid, right_oid, cmpt);

	if (InvalidOid == scalar_cmp_oid)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	string name_str = duckdb::GetOpName(scalar_cmp_oid);
	CHAR *name = std::strcpy(new char[name_str.length() + 1], name_str.c_str()); // TODO avoid copy?

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	mdid->AddRef();
	mdid_left->AddRef();
	mdid_right->AddRef();

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, name);

	return GPOS_NEW(mp) CMDScCmpGPDB(
		mp, mdid, mdname, mdid_left, mdid_right, cmp_type,
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, scalar_cmp_oid));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::TransformStatsToDXLBucketArray
//
//	@doc:
//		transform stats from pg_stats form to optimizer's preferred form
//
//---------------------------------------------------------------------------
CDXLBucketArray *
CTranslatorTBGPPToDXL::TransformStatsToDXLBucketArray(
    CMemoryPool *mp, OID att_type, CDouble num_distinct, CDouble null_freq,
    const Datum *mcv_values, const float4 *mcv_frequencies,
    ULONG num_mcv_values, const Datum *hist_values,
    const Datum *hist_freq_values, ULONG num_hist_values)
{
	CMDIdGPDB *mdid_atttype =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, att_type);
	IMDType *md_type = RetrieveType(mp, mdid_atttype);

	// translate MCVs to Orca histogram. Create an empty histogram if there are no MCVs.
	CHistogram *gpdb_mcv_hist = TransformMcvToOrcaHistogram(
		mp, md_type, mcv_values, mcv_frequencies, num_mcv_values);

	GPOS_ASSERT(gpdb_mcv_hist->IsValid());

	CDouble mcv_freq = gpdb_mcv_hist->GetFrequency();
	BOOL has_mcv = 0 < num_mcv_values && CStatistics::Epsilon < mcv_freq;

	CDouble hist_freq = 0.0;
	if (1 < num_hist_values)
	{
		hist_freq = CDouble(1.0) - null_freq - mcv_freq;
	}

	BOOL is_text_type = mdid_atttype->Equals(&CMDIdGPDB::m_mdid_varchar) ||
						mdid_atttype->Equals(&CMDIdGPDB::m_mdid_bpchar) ||
						mdid_atttype->Equals(&CMDIdGPDB::m_mdid_text);
	BOOL has_hist = !is_text_type && 1 < num_hist_values &&
					CStatistics::Epsilon < hist_freq;

	CHistogram *histogram = NULL;

	// if histogram has any significant information, then extract it
	if (has_hist)
	{
		// histogram from gpdb histogram
		histogram = TransformHistToOrcaHistogram(
			mp, md_type, hist_values, hist_freq_values, num_hist_values, num_distinct, hist_freq);
		if (0 == histogram->GetNumBuckets())
		{
			has_hist = false;
		}
	}

	CDXLBucketArray *dxl_stats_bucket_array = NULL;

	if (has_hist && !has_mcv)
	{
		// if histogram exists and dominates, use histogram only
		dxl_stats_bucket_array =
			TransformHistogramToDXLBucketArray(mp, md_type, histogram);
	}
	else if (!has_hist && has_mcv)
	{
		// if MCVs exist and dominate, use MCVs only
		dxl_stats_bucket_array =
			TransformHistogramToDXLBucketArray(mp, md_type, gpdb_mcv_hist);
	}
	else if (has_hist && has_mcv)
	{
		// both histogram and MCVs exist and have significant info, merge MCV and histogram buckets
		CHistogram *merged_hist =
			CStatisticsUtils::MergeMCVHist(mp, gpdb_mcv_hist, histogram);
		dxl_stats_bucket_array =
			TransformHistogramToDXLBucketArray(mp, md_type, merged_hist);
		GPOS_DELETE(merged_hist);
	}
	else
	{
		// no MCVs nor histogram
		GPOS_ASSERT(!has_hist && !has_mcv);
		dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);
	}

	// cleanup
	mdid_atttype->Release();
	md_type->Release();
	GPOS_DELETE(gpdb_mcv_hist);

	if (NULL != histogram)
	{
		GPOS_DELETE(histogram);
	}

	return dxl_stats_bucket_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::TransformMcvToOrcaHistogram
//
//	@doc:
//		Transform gpdb's mcv info to optimizer histogram
//
//---------------------------------------------------------------------------
CHistogram *
CTranslatorTBGPPToDXL::TransformMcvToOrcaHistogram(
	CMemoryPool *mp, const IMDType *md_type, const Datum *mcv_values,
	const float4 *mcv_frequencies, ULONG num_mcv_values)
{
	IDatumArray *datums = GPOS_NEW(mp) IDatumArray(mp);
	CDoubleArray *freqs = GPOS_NEW(mp) CDoubleArray(mp);

	GPOS_ASSERT(num_mcv_values == 0); // S62 we do not have mvc now

	// for (ULONG ul = 0; ul < num_mcv_values; ul++)
	// {
	// 	Datum datumMCV = mcv_values[ul];
	// 	IDatum *datum = CTranslatorScalarToDXL::CreateIDatumFromGpdbDatum(
	// 		mp, md_type, false /* is_null */, datumMCV);
	// 	datums->Append(datum);
	// 	freqs->Append(GPOS_NEW(mp) CDouble(mcv_frequencies[ul]));

	// 	if (!datum->StatsAreComparable(datum))
	// 	{
	// 		// if less than operation is not supported on this datum, then no point
	// 		// building a histogram. return an empty histogram
	// 		datums->Release();
	// 		freqs->Release();
	// 		return GPOS_NEW(mp) CHistogram(mp);
	// 	}
	// }

	CHistogram *hist = CStatisticsUtils::TransformMCVToHist(
		mp, md_type, datums, freqs, num_mcv_values);

	datums->Release();
	freqs->Release();
	return hist;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::TransformHistToOrcaHistogram
//
//	@doc:
//		Transform GPDB's hist info to optimizer's histogram
//
//---------------------------------------------------------------------------
CHistogram *CTranslatorTBGPPToDXL::TransformHistToOrcaHistogram(
    CMemoryPool *mp, const IMDType *md_type, const Datum *hist_values,
    const Datum *hist_freq_values, ULONG num_hist_values, CDouble num_distinct,
    CDouble hist_freq)
{
    GPOS_ASSERT(1 < num_hist_values);

	const ULONG num_buckets = num_hist_values - 1;
	CDouble distinct_per_bucket = num_distinct / CDouble(num_buckets);
	// CDouble freq_per_bucket = hist_freq / CDouble(num_buckets);
	CDouble total_freq(0.0);
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		total_freq = total_freq + CDouble(hist_freq_values[ul]);
	}

	BOOL last_bucket_was_singleton = false;
	// create buckets
	CBucketArray *buckets = GPOS_NEW(mp) CBucketArray(mp);
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		IDatum *min_datum = CTranslatorScalarToDXL::CreateIDatumFromGpdbDatum(
			mp, md_type, false /* is_null */, hist_values[ul]);
		IDatum *max_datum = CTranslatorScalarToDXL::CreateIDatumFromGpdbDatum(
			mp, md_type, false /* is_null */, hist_values[ul + 1]);
		BOOL is_lower_closed, is_upper_closed;
		CDouble cur_freq = CDouble(hist_freq_values[ul]) / total_freq;

		if (min_datum->StatsAreEqual(max_datum))
		{
			// Singleton bucket !!!!!!!!!!!!!
			is_lower_closed = true;
			is_upper_closed = true;
			last_bucket_was_singleton = true;
		}
		else if (last_bucket_was_singleton)
		{
			// Last bucket was a singleton, so lower must be open now.
			is_lower_closed = false;
			is_upper_closed = false;
			last_bucket_was_singleton = false;
		}
		else
		{
			// Normal bucket
			// GPDB histograms assumes lower bound to be closed and upper bound to be open
			is_lower_closed = true;
			is_upper_closed = false;
		}

		if (ul == num_buckets - 1)
		{
			// last bucket upper bound is also closed
			is_upper_closed = true;
		}

		CBucket *bucket = GPOS_NEW(mp)
			CBucket(GPOS_NEW(mp) CPoint(min_datum),
					GPOS_NEW(mp) CPoint(max_datum), is_lower_closed,
					is_upper_closed, cur_freq, distinct_per_bucket);
		buckets->Append(bucket);

		if (!min_datum->StatsAreComparable(max_datum) ||
			!min_datum->StatsAreLessThan(max_datum))
		{
			// if less than operation is not supported on this datum,
			// or the translated histogram does not conform to GPDB sort order (e.g. text column in Linux platform),
			// then no point building a histogram. return an empty histogram

			// TODO: 03/01/2014 translate histogram into Orca even if sort
			// order is different in GPDB, and use const expression eval to compare
			// datums in Orca (MPP-22780)
			buckets->Release();
			return GPOS_NEW(mp) CHistogram(mp);
		}
	}

	CHistogram *hist = GPOS_NEW(mp) CHistogram(mp, buckets);
	return hist;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::TransformHistogramToDXLBucketArray
//
//	@doc:
//		Histogram to array of dxl buckets
//
//---------------------------------------------------------------------------
CDXLBucketArray *
CTranslatorTBGPPToDXL::TransformHistogramToDXLBucketArray(
	CMemoryPool *mp, const IMDType *md_type, const CHistogram *hist)
{
	CDXLBucketArray *dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);
	const CBucketArray *buckets = hist->GetBuckets();
	ULONG num_buckets = buckets->Size();
	for (ULONG ul = 0; ul < num_buckets; ul++)
	{
		CBucket *bucket = (*buckets)[ul];
		IDatum *datum_lower = bucket->GetLowerBound()->GetDatum();
		CDXLDatum *dxl_lower = md_type->GetDatumVal(mp, datum_lower);
		IDatum *datum_upper = bucket->GetUpperBound()->GetDatum();
		CDXLDatum *dxl_upper = md_type->GetDatumVal(mp, datum_upper);
		CDXLBucket *dxl_bucket = GPOS_NEW(mp)
			CDXLBucket(dxl_lower, dxl_upper, bucket->IsLowerClosed(),
					   bucket->IsUpperClosed(), bucket->GetFrequency(),
					   bucket->GetNumDistinct());
		dxl_stats_bucket_array->Append(dxl_bucket);
	}
	return dxl_stats_bucket_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelStorageType
//
//	@doc:
//		Get relation storage type
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype
CTranslatorTBGPPToDXL::RetrieveRelStorageType(CHAR storage_type)
{
	IMDRelation::Erelstoragetype rel_storage_type =
		IMDRelation::ErelstorageSentinel;

	// switch (storage_type)
	// {
		
	// 	case RELSTORAGE_HEAP:
	// 		rel_storage_type = IMDRelation::ErelstorageHeap;
	// 		break;
	// 	case RELSTORAGE_AOCOLS:
	// 		rel_storage_type = IMDRelation::ErelstorageAppendOnlyCols;
	// 		break;
	// 	case RELSTORAGE_AOROWS:
	// 		rel_storage_type = IMDRelation::ErelstorageAppendOnlyRows;
	// 		break;
	// 	case RELSTORAGE_VIRTUAL:
	// 		rel_storage_type = IMDRelation::ErelstorageVirtual;
	// 		break;
	// 	case RELSTORAGE_EXTERNAL:
	// 		rel_storage_type = IMDRelation::ErelstorageExternal;
	// 		break;
	// 	default:
	// 		GPOS_ASSERT(!"Unsupported relation type");
	// }
	rel_storage_type = IMDRelation::ErelstorageHeap; // TODO temporary

	return rel_storage_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrievePartKeysAndTypes
//
//	@doc:
//		Get partition keys and types for relation or NULL if relation not partitioned.
//		Caller responsible for closing the relation if an exception is raised
//
//---------------------------------------------------------------------------
void
CTranslatorTBGPPToDXL::RetrievePartKeysAndTypes(CMemoryPool *mp,
												   Relation rel, OID oid,
												   ULongPtrArray **part_keys,
												   CharPtrArray **part_types)
{
	GPOS_ASSERT(NULL != rel);

	// if (!gpdb::RelPartIsRoot(oid))
	// {
	// 	// not a partitioned table
	// 	*part_keys = NULL;
	// 	*part_types = NULL;
	// 	return;
	// }

	// // TODO: Feb 23, 2012; support intermediate levels

	// *part_keys = GPOS_NEW(mp) ULongPtrArray(mp);
	// *part_types = GPOS_NEW(mp) CharPtrArray(mp);

	// List *part_keys_list = NIL;
	// List *part_types_list = NIL;
	// gpdb::GetOrderedPartKeysAndKinds(oid, &part_keys_list, &part_types_list);

	// ListCell *lc_key = NULL;
	// ListCell *lc_type = NULL;
	// ForBoth(lc_key, part_keys_list, lc_type, part_types_list)
	// {
	// 	List *part_key = (List *) lfirst(lc_key);

	// 	if (1 < gpdb::ListLength(part_key))
	// 	{
	// 		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
	// 				   GPOS_WSZ_LIT("Composite part key"));
	// 	}

	// 	INT attno = linitial_int(part_key);
	// 	CHAR part_type = (CHAR) lfirst_int(lc_type);
	// 	GPOS_ASSERT(0 < attno);
	// 	(*part_keys)->Append(GPOS_NEW(mp) ULONG(attno - 1));
	// 	(*part_types)->Append(GPOS_NEW(mp) CHAR(part_type));
	// }

	// gpdb::ListFree(part_keys_list);
	// gpdb::ListFree(part_types_list);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::ConstructAttnoMapping
//
//	@doc:
//		Construct a mapping for GPDB attnos to positions in the columns array
//
//---------------------------------------------------------------------------
ULONG *
CTranslatorTBGPPToDXL::ConstructAttnoMapping(CMemoryPool *mp,
												CMDColumnArray *mdcol_array,
												ULONG max_cols)
{
	GPOS_ASSERT(NULL != mdcol_array);
	GPOS_ASSERT(0 < mdcol_array->Size());
	GPOS_ASSERT(max_cols > mdcol_array->Size());

	// build a mapping for attnos->positions
	const ULONG num_of_cols = mdcol_array->Size();
	ULONG *attno_mapping = GPOS_NEW_ARRAY(mp, ULONG, max_cols);

	// initialize all positions to gpos::ulong_max
	for (ULONG ul = 0; ul < max_cols; ul++)
	{
		attno_mapping[ul] = gpos::ulong_max;
	}

	for (ULONG ul = 0; ul < num_of_cols; ul++)
	{
		const IMDColumn *md_col = (*mdcol_array)[ul];
		INT attno = md_col->AttrNum();

		ULONG idx = (ULONG)(GPDXL_SYSTEM_COLUMNS + attno);
		attno_mapping[idx] = ul;
	}

	return attno_mapping;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveRelKeysets
//
//	@doc:
//		Get key sets for relation
//
//---------------------------------------------------------------------------
ULongPtr2dArray *
CTranslatorTBGPPToDXL::RetrieveRelKeysets(CMemoryPool *mp, OID oid,
											 BOOL should_add_default_keys,
											 BOOL is_partitioned,
											 ULONG *attno_mapping)
{
	// TODO what is key sets??
	ULongPtr2dArray *key_sets = GPOS_NEW(mp) ULongPtr2dArray(mp);

	// List *rel_keys = gpdb::GetRelationKeys(oid);

	// ListCell *lc_key = NULL;
	// ForEach(lc_key, rel_keys)
	// {
	// 	List *key_elem_list = (List *) lfirst(lc_key);

	// 	ULongPtrArray *key_set = GPOS_NEW(mp) ULongPtrArray(mp);

	// 	ListCell *lc_key_elem = NULL;
	// 	ForEach(lc_key_elem, key_elem_list)
	// 	{
	// 		INT key_idx = lfirst_int(lc_key_elem);
	// 		ULONG pos = GetAttributePosition(key_idx, attno_mapping);
	// 		key_set->Append(GPOS_NEW(mp) ULONG(pos));
	// 	}
	// 	GPOS_ASSERT(0 < key_set->Size());

	// 	key_sets->Append(key_set);
	// }

	// // add {segid, ctid} as a key

	// if (should_add_default_keys)
	// {
	// 	ULongPtrArray *key_set = GPOS_NEW(mp) ULongPtrArray(mp);
	// 	if (is_partitioned)
	// 	{
	// 		// TableOid is part of default key for partitioned tables
	// 		ULONG table_oid_pos =
	// 			GetAttributePosition(TableOidAttributeNumber, attno_mapping);
	// 		key_set->Append(GPOS_NEW(mp) ULONG(table_oid_pos));
	// 	}
	// 	ULONG seg_id_pos =
	// 		GetAttributePosition(GpSegmentIdAttributeNumber, attno_mapping);
	// 	ULONG ctid_pos =
	// 		GetAttributePosition(SelfItemPointerAttributeNumber, attno_mapping);
	// 	key_set->Append(GPOS_NEW(mp) ULONG(seg_id_pos));
	// 	key_set->Append(GPOS_NEW(mp) ULONG(ctid_pos));

	// 	key_sets->Append(key_set);
	// }

	return key_sets;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::NormalizeFrequencies
//
//	@doc:
//		Sometimes a set of frequencies can add up to more than 1.0.
//		Fix these cases
//
//---------------------------------------------------------------------------
void
CTranslatorTBGPPToDXL::NormalizeFrequencies(float4 *freqs, ULONG length,
											   CDouble *null_freq)
{
	if (length == 0 && (*null_freq) < 1.0)
	{
		return;
	}

	CDouble total = *null_freq;
	for (ULONG ul = 0; ul < length; ul++)
	{
		total = total + CDouble(freqs[ul]);
	}

	if (total > CDouble(1.0))
	{
		float4 denom = (float4)(total + CStatistics::Epsilon).Get();

		// divide all values by the total
		for (ULONG ul = 0; ul < length; ul++)
		{
			freqs[ul] = freqs[ul] / denom;
		}
		*null_freq = *null_freq / denom;
	}

#ifdef GPOS_DEBUG
	// recheck
	CDouble recheck_total = *null_freq;
	for (ULONG ul = 0; ul < length; ul++)
	{
		recheck_total = recheck_total + CDouble(freqs[ul]);
	}
	GPOS_ASSERT(recheck_total <= CDouble(1.0));
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::IsIndexSupported
//
//	@doc:
//		Check if index type is supported
//
//---------------------------------------------------------------------------
BOOL
CTranslatorTBGPPToDXL::IsIndexSupported(IndexCatalogEntry *index_cat)
{
	// HeapTupleData *tup = index_rel->rd_indextuple;

	// // index expressions and index constraints not supported
	// return gpdb::HeapAttIsNull(tup, Anum_pg_index_indexprs) &&
	// 	   gpdb::HeapAttIsNull(tup, Anum_pg_index_indpred) &&
	// 	   index_rel->rd_index->indisvalid &&
	// 	   (BTREE_AM_OID == index_rel->rd_rel->relam ||
	// 		BITMAP_AM_OID == index_rel->rd_rel->relam ||
	// 		GIST_AM_OID == index_rel->rd_rel->relam ||
	// 		GIN_AM_OID == index_rel->rd_rel->relam);
	return true;
}

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::RetrievePartConstraintForIndex
// //
// //	@doc:
// //		Retrieve part constraint for index
// //
// //---------------------------------------------------------------------------
// CMDPartConstraintGPDB *
// CTranslatorTBGPPToDXL::RetrievePartConstraintForIndex(
// 	CMemoryPool *mp, CMDAccessor *md_accessor, const IMDRelation *md_rel,
// 	Node *part_constraint, ULongPtrArray *level_with_default_part_array,
// 	BOOL is_unbounded)
// {
// 	CDXLColDescrArray *dxl_col_descr_array = GPOS_NEW(mp) CDXLColDescrArray(mp);
// 	const ULONG num_columns = md_rel->ColumnCount();

// 	for (ULONG ul = 0; ul < num_columns; ul++)
// 	{
// 		const IMDColumn *md_col = md_rel->GetMdCol(ul);
// 		CMDName *md_colname =
// 			GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
// 		CMDIdGPDB *mdid_col_type = CMDIdGPDB::CastMdid(md_col->MdidType());
// 		mdid_col_type->AddRef();

// 		// create a column descriptor for the column
// 		CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
// 			mp, md_colname,
// 			ul + 1,	 // colid
// 			md_col->AttrNum(), mdid_col_type, md_col->TypeModifier(),
// 			false  // fColDropped
// 		);
// 		dxl_col_descr_array->Append(dxl_col_descr);
// 	}

// 	CMDPartConstraintGPDB *mdpart_constraint = RetrievePartConstraintFromNode(
// 		mp, md_accessor, dxl_col_descr_array, part_constraint,
// 		level_with_default_part_array, is_unbounded);

// 	dxl_col_descr_array->Release();

// 	return mdpart_constraint;
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::RetrievePartConstraintForRel
// //
// //	@doc:
// //		Retrieve part constraint for relation
// //
// //---------------------------------------------------------------------------
// CMDPartConstraintGPDB *
// CTranslatorTBGPPToDXL::RetrievePartConstraintForRel(
// 	CMemoryPool *mp, CMDAccessor *md_accessor, OID rel_oid,
// 	CMDColumnArray *mdcol_array, bool construct_full_expr)
// {
// 	// get the part constraints
// 	List *default_levels_rel = NIL;
// 	Node *node;

// 	if (!GPOS_FTRACE(EopttraceEnableExternalPartitionedTables) ||
// 		gpdb::RelPartIsRoot(rel_oid))
// 	{
// 		node = gpdb::GetRelationPartContraints(rel_oid, &default_levels_rel);
// 	}
// 	else if (gpdb::IsLeafPartition(rel_oid))
// 	{
// 		GPOS_ASSERT(GPOS_FTRACE(EopttraceEnableExternalPartitionedTables));
// 		node = gpdb::GetLeafPartContraints(rel_oid, &default_levels_rel);
// 	}
// 	else
// 	{
// 		// interior partition of a partition table or not a partitioned table
// 		return NULL;
// 	}

// 	// don't retrieve part constraints if they are not needed
// 	// and if there no default partitions at any level
// 	if (!construct_full_expr && NIL == default_levels_rel)
// 	{
// 		return NULL;
// 	}

// 	List *part_keys = gpdb::GetPartitionAttrs(rel_oid);
// 	const ULONG num_of_levels = gpdb::ListLength(part_keys);
// 	gpdb::ListFree(part_keys);

// 	BOOL is_unbounded = true;
// 	ULongPtrArray *default_levels_derived = GPOS_NEW(mp) ULongPtrArray(mp);
// 	for (ULONG ul = 0; ul < num_of_levels; ul++)
// 	{
// 		if (LevelHasDefaultPartition(default_levels_rel, ul))
// 		{
// 			default_levels_derived->Append(GPOS_NEW(mp) ULONG(ul));
// 		}
// 		else
// 		{
// 			is_unbounded = false;
// 		}
// 	}

// 	CMDPartConstraintGPDB *mdpart_constraint = NULL;

// 	// FIXME: Deal with default partitions for external tables
// 	if (gpdb::IsLeafPartition(rel_oid))
// 	{
// 		is_unbounded = false;
// 	}

// 	if (!construct_full_expr)
// 	{
// 		// Do not construct the partition constraint expression since ORCA is not going
// 		// to use it. Only send the default partition information.
// 		default_levels_derived->AddRef();
// 		mdpart_constraint = GPOS_NEW(mp) CMDPartConstraintGPDB(
// 			mp, default_levels_derived, is_unbounded, NULL);
// 	}
// 	else
// 	{
// 		CDXLColDescrArray *dxl_col_descr_array =
// 			GPOS_NEW(mp) CDXLColDescrArray(mp);
// 		const ULONG num_columns = mdcol_array->Size();
// 		for (ULONG ul = 0; ul < num_columns; ul++)
// 		{
// 			const IMDColumn *md_col = (*mdcol_array)[ul];
// 			CMDName *md_colname =
// 				GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
// 			CMDIdGPDB *mdid_col_type = CMDIdGPDB::CastMdid(md_col->MdidType());
// 			mdid_col_type->AddRef();

// 			// create a column descriptor for the column
// 			CDXLColDescr *dxl_col_descr = GPOS_NEW(mp) CDXLColDescr(
// 				mp, md_colname,
// 				ul + 1,	 // colid
// 				md_col->AttrNum(), mdid_col_type, md_col->TypeModifier(),
// 				false  // fColDropped
// 			);
// 			dxl_col_descr_array->Append(dxl_col_descr);
// 		}

// 		mdpart_constraint = RetrievePartConstraintFromNode(
// 			mp, md_accessor, dxl_col_descr_array, node, default_levels_derived,
// 			is_unbounded);
// 		dxl_col_descr_array->Release();
// 	}

// 	gpdb::ListFree(default_levels_rel);
// 	default_levels_derived->Release();

// 	return mdpart_constraint;
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CTranslatorTBGPPToDXL::RetrievePartConstraintFromNode
// //
// //	@doc:
// //		Retrieve part constraint from GPDB node
// //
// //---------------------------------------------------------------------------
// CMDPartConstraintGPDB *
// CTranslatorTBGPPToDXL::RetrievePartConstraintFromNode(
// 	CMemoryPool *mp, CMDAccessor *md_accessor,
// 	CDXLColDescrArray *dxl_col_descr_array, Node *part_constraints,
// 	ULongPtrArray *level_with_default_part_array, BOOL is_unbounded)
// {
// 	if (NULL == part_constraints)
// 	{
// 		return NULL;
// 	}

// 	// generate a mock mapping between var to column information
// 	CMappingVarColId *var_colid_mapping = GPOS_NEW(mp) CMappingVarColId(mp);

// 	var_colid_mapping->LoadColumns(0 /*query_level */, 1 /* rteIndex */,
// 								   dxl_col_descr_array);

// 	// translate the check constraint expression
// 	CDXLNode *scalar_dxlnode =
// 		CTranslatorScalarToDXL::TranslateStandaloneExprToDXL(
// 			mp, md_accessor, var_colid_mapping, (Expr *) part_constraints);

// 	// cleanup
// 	GPOS_DELETE(var_colid_mapping);

// 	level_with_default_part_array->AddRef();
// 	return GPOS_NEW(mp) CMDPartConstraintGPDB(mp, level_with_default_part_array,
// 											  is_unbounded, scalar_dxlnode);
// }

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RelHasSystemColumns
//
//	@doc:
//		Does given relation type have system columns.
//		Currently only regular relations, sequences, toast values relations and
//		AO segment relations have system columns
//
//---------------------------------------------------------------------------
BOOL
CTranslatorTBGPPToDXL::RelHasSystemColumns(char rel_kind)
{
	return true;
	// return RELKIND_RELATION == rel_kind || RELKIND_SEQUENCE == rel_kind ||
	// 	   RELKIND_AOSEGMENTS == rel_kind || RELKIND_TOASTVALUE == rel_kind;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::ParseCmpType
//
//	@doc:
//		Translate GPDB comparison types into optimizer comparison types
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CTranslatorTBGPPToDXL::ParseCmpType(ULONG cmpt)
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(cmp_type_mappings); ul++)
	{
		const ULONG *mapping = cmp_type_mappings[ul];
		if (mapping[1] == cmpt)
		{
			return (IMDType::ECmpType) mapping[0];
		}
	}

	return IMDType::EcmptOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::GetComparisonType
//
//	@doc:
//		Translate optimizer comparison types into GPDB comparison types
//
//---------------------------------------------------------------------------
ULONG
CTranslatorTBGPPToDXL::GetComparisonType(IMDType::ECmpType cmp_type)
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(cmp_type_mappings); ul++)
	{
		const ULONG *mapping = cmp_type_mappings[ul];
		if (mapping[0] == cmp_type)
		{
			return (ULONG) mapping[1];
		}
	}

	return CmptOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveIndexOpFamilies
//
//	@doc:
//		Retrieve the opfamilies for the keys of the given index
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorTBGPPToDXL::RetrieveIndexOpFamilies(CMemoryPool *mp,
												  IMDId *mdid_index)
{
	// List *op_families =
	// 	gpdb::GetIndexOpFamilies(CMDIdGPDB::CastMdid(mdid_index)->Oid());
	IMdIdArray *input_col_mdids = GPOS_NEW(mp) IMdIdArray(mp);

	// ListCell *lc = NULL;

	// ForEach(lc, op_families)
	// {
		// OID op_family_oid = lfirst_oid(lc);
		OID op_family_logical_oid = duckdb::GetComparisonOperator( // TODO for adjidx currently..
			(idx_t)LogicalTypeId::ID + LOGICAL_TYPE_BASE_ID,
			(idx_t)LogicalTypeId::ID + LOGICAL_TYPE_BASE_ID,
			CmptEq);
		OID op_family_oid = duckdb::GetOpFamiliesForScOp(op_family_logical_oid);
		input_col_mdids->Append(
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, op_family_oid));
	// }

	return input_col_mdids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorTBGPPToDXL::RetrieveScOpOpFamilies
//
//	@doc:
//		Retrieve the families for the keys of the given scalar operator
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorTBGPPToDXL::RetrieveScOpOpFamilies(CMemoryPool *mp,
												 IMDId *mdid_scalar_op)
{
	// List *op_families =
	// 	gpdb::GetOpFamiliesForScOp(CMDIdGPDB::CastMdid(mdid_scalar_op)->Oid());
	OID op_family_oid =
		duckdb::GetOpFamiliesForScOp(CMDIdGPDB::CastMdid(mdid_scalar_op)->Oid());
	IMdIdArray *input_col_mdids = GPOS_NEW(mp) IMdIdArray(mp);

	// ListCell *lc = NULL;

	// ForEach(lc, op_families)
	// {
	// 	OID op_family_oid = lfirst_oid(lc);
		input_col_mdids->Append(
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, op_family_oid));
	// }

	return input_col_mdids;
}

IMDId *
CTranslatorTBGPPToDXL::AddVirtualTable(CMemoryPool *mp, IMDId *mdid, IMdIdArray *pdrgmdid) {
	// Convert IMdIdArray to array of OIDs
	ULONG size = pdrgmdid->Size();
	uint32_t original_vtbl_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	uint32_t *oid_array = new uint32_t[size];
	
	for (ULONG i = 0; i < size; i++) {
		IMDId *graphlet_mdid = (*pdrgmdid)[i];
		oid_array[i] = CMDIdGPDB::CastMdid(graphlet_mdid)->Oid();
	}
	ULONG virtual_table_oid = duckdb::AddVirtualTable(original_vtbl_oid, oid_array, size);

	CMDIdGPDB *new_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, virtual_table_oid, 0, 0);
	return new_mdid;
}

// EOF
