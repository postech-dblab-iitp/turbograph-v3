extern "C" {
#include "utils/typcache.h"
}
#include "utils/s62_rel.hpp"

#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/io/COstreamString.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
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

// S62 related classes
#include "s62dbwrappers.hpp"
#include "catalog/catalog.hpp"
#include "translate/CTranslatorS62ToDXL.hpp"

using namespace gpdxl;
using namespace gpopt;
using namespace s62;

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
		default:
			break;
	}
	GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
			   GPOS_WSZ_LIT("Query references unknown index type"));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveObject
//
//	@doc:
//		Retrieve a metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorS62ToDXL::RetrieveObject(CMemoryPool *mp,
										 CMDAccessor *md_accessor, IMDId *mdid,
										 IMDCacheObject::Emdtype mdtype)
{
	IMDCacheObject *md_obj = NULL;
	GPOS_ASSERT(NULL != md_accessor);

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
//		CTranslatorS62ToDXL::RetrieveMDObjGPDB
//
//	@doc:
//		Retrieve a GPDB metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorS62ToDXL::RetrieveObjectGPDB(CMemoryPool *mp, IMDId *mdid,
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
			return NULL;

		default:
			GPOS_RTL_ASSERT_MSG(false, "Unexpected MD type.");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::GetRelName
//
//	@doc:
//		Return a relation name
//
//---------------------------------------------------------------------------
CMDName *
CTranslatorS62ToDXL::GetRelName(CMemoryPool *mp, s62::PropertySchemaCatalogEntry *rel)
{
	GPOS_ASSERT(NULL != rel);
	CHAR *relname = std::strcpy(new char[rel->GetName().length() + 1], rel->GetName().c_str());
	CWStringDynamic *relname_str =
		CDXLUtils::CreateDynamicStringFromCharArray(mp, relname);
	CMDName *mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
	GPOS_DELETE(relname_str);
	return mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveRelIndexInfo
//
//	@doc:
//		Return the indexes defined on the given relation
//
//---------------------------------------------------------------------------
CMDIndexInfoArray *
CTranslatorS62ToDXL::RetrieveRelIndexInfo(CMemoryPool *mp, PropertySchemaCatalogEntry *rel)
{
	GPOS_ASSERT(NULL != rel);
	return RetrieveRelIndexInfoForNonPartTable(mp, rel);
}

// return index info list of indexes defined on a partitioned table
CMDIndexInfoArray *
CTranslatorS62ToDXL::RetrieveRelIndexInfoForPartTable(CMemoryPool *mp,
														   Relation root_rel)
{
	GPOS_ASSERT(false);
	return NULL;
}

// return index info list of indexes defined on regular, external tables or leaf partitions
CMDIndexInfoArray *
CTranslatorS62ToDXL::RetrieveRelIndexInfoForNonPartTable(CMemoryPool *mp,
															  PropertySchemaCatalogEntry *rel)
{
	CMDIndexInfoArray *md_index_info_array = GPOS_NEW(mp) CMDIndexInfoArray(mp);

	auto append_index_md = [&](idx_t index_oid) {
		IndexCatalogEntry *index_cat = s62::GetIndex(index_oid);
	
		if (NULL == index_cat)
		{
			WCHAR wstr[1024];
			CWStringStatic str(wstr, 1024);
			COstreamString oss(&str);
			oss << (ULONG) index_oid;
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
					   str.GetBuffer());
		}

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
		}
		GPOS_CATCH_EX(ex)
		{
			GPOS_RETHROW(ex);
		}
		GPOS_CATCH_END;
	};

	// not a partitioned table: obtain indexes directly from the catalog
	idx_t partition_oid = rel->GetPartitionOID();
	PartitionCatalogEntry *part_cat = s62::GetPartition(partition_oid);

	// Get PhysicalID Index
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
//		CTranslatorS62ToDXL::RetrieveRelTriggers
//
//	@doc:
//		Return the triggers defined on the given relation
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorS62ToDXL::RetrieveRelTriggers(CMemoryPool *mp, PropertySchemaCatalogEntry *rel)
{
	GPOS_ASSERT(NULL != rel);

	IMdIdArray *mdid_triggers_array = GPOS_NEW(mp) IMdIdArray(mp);

	return mdid_triggers_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveRelCheckConstraints
//
//	@doc:
//		Return the check constraints defined on the relation with the given oid
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorS62ToDXL::RetrieveRelCheckConstraints(CMemoryPool *mp, OID oid)
{
	IMdIdArray *check_constraint_mdids = GPOS_NEW(mp) IMdIdArray(mp);

	return check_constraint_mdids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::CheckUnsupportedRelation
//
//	@doc:
//		Check and fall back to planner for unsupported relations
//
//---------------------------------------------------------------------------
void
CTranslatorS62ToDXL::CheckUnsupportedRelation(OID rel_oid)
{
	GPOS_ASSERT(false);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveRel
//
//	@doc:
//		Retrieve a relation from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDRelation *
CTranslatorS62ToDXL::RetrieveRel(CMemoryPool *mp, CMDAccessor *md_accessor,
									  IMDId *mdid)
{
	OID oid = CMDIdGPDB::CastMdid(mdid)->Oid(); 
	GPOS_ASSERT(InvalidOid != oid);

	s62::PropertySchemaCatalogEntry *rel = s62::GetRelation(oid);

	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

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
		rel_storage_type = RetrieveRelStorageType('c'); // temporary

		// get relation columns
		mdcol_array =
			RetrieveRelColumns(mp, md_accessor, rel, rel_storage_type);
		const ULONG max_cols =
			GPDXL_SYSTEM_COLUMNS + (ULONG) rel->GetNumberOfColumns() + 1;
		ULONG *attno_mapping = ConstructAttnoMapping(mp, mdcol_array, max_cols);

		// get distribution policy
		dist = IMDRelation::EreldistrMasterOnly;

		// collect relation indexes
		md_index_info_array = RetrieveRelIndexInfo(mp, rel);

		// collect relation triggers 
		mdid_triggers_array = RetrieveRelTriggers(mp, rel);

		// get key sets
		BOOL should_add_default_keys = true;
		keyset_array = RetrieveRelKeysets(mp, oid, should_add_default_keys,
										  is_partitioned, attno_mapping);

		// collect all check constraints
		check_constraint_mdids = RetrieveRelCheckConstraints(mp, oid);
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

	CMDPartConstraintGPDB *mdpart_constraint = NULL;

	if (IMDRelation::ErelstorageExternal == rel_storage_type)
	{
		GPOS_ASSERT(false);
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
//		CTranslatorS62ToDXL::RetrieveRelColumns
//
//	@doc:
//		Get relation columns
//
//---------------------------------------------------------------------------
CMDColumnArray *
CTranslatorS62ToDXL::RetrieveRelColumns(
	CMemoryPool *mp, CMDAccessor *md_accessor, s62::PropertySchemaCatalogEntry *rel,
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
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, (OID) s62::LogicalTypeId::ID + LOGICAL_TYPE_BASE_ID);
		
		CMDColumn *md_col = GPOS_NEW(mp)
			CMDColumn(md_colname, -1/*att->attnum*/, mdid_col, -1/*att->atttypmod*/,
					  true /*!att->attnotnull, is_nullable*/, false /*att->attisdropped*/,
					  dxl_default_col_val /* default value */, 0, col_len);

		mdcol_array->Append(md_col);
	}

	ULONG attnum = 1; // start from 1 - refer pg_attribute.h
	
	for (ULONG ul = 0; ul < (ULONG) rel->GetNumberOfColumns(); ul++)
	{
		if (rel->GetType(ul) == s62::LogicalType::FORWARD_ADJLIST ||
			rel->GetType(ul) == s62::LogicalType::BACKWARD_ADJLIST) continue;

		CMDName *md_colname =
			CDXLUtils::CreateMDNameFromCharArray(mp, rel->GetPropertyKeyName(ul).c_str());

		// translate the default column value
		CDXLNode *dxl_default_col_val = NULL;

		ULONG prop_id = rel->GetPropKeyIDs()->at(ul);

		ULONG col_len = gpos::ulong_max;
		CMDIdGPDB *mdid_col = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral,
			(OID) rel->GetType(ul) + NUM_MAX_LOGICAL_TYPES * rel->GetExtraTypeInfo(ul) + LOGICAL_TYPE_BASE_ID);

		// Column width priority:
		// 1. If there is average width kept in the stats for that column, pick that value.
		// 2. If not, if it is a fixed length text type, pick the size of it. E.g if it is
		//    varchar(10), assign 10 as the column length.
		// 3. Else if it not dropped and a fixed length type such as int4, assign the fixed
		//    length.
		// 4. Otherwise, assign it to default column width which is 8.
		col_len = rel->GetTypeSize(ul);
		auto type_mod = rel->GetExtraTypeInfo(ul);
		CMDColumn *md_col = GPOS_NEW(mp)
			CMDColumn(md_colname, attnum++/*att->attnum*/, mdid_col, type_mod,
					  true /*!att->attnotnull, is_nullable*/, false /*att->attisdropped*/,
					  dxl_default_col_val /* default value */, prop_id, col_len);

		mdcol_array->Append(md_col);
	}

	return mdcol_array;
}

IMdIdArray *
CTranslatorS62ToDXL::RetrieveRelExternalPartitions(CMemoryPool *mp,
														OID rel_oid)
{
	IMdIdArray *external_partitions = GPOS_NEW(mp) IMdIdArray(mp);

	return external_partitions;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveIndex
//
//	@doc:
//		Retrieve an index from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDIndex *
CTranslatorS62ToDXL::RetrieveIndex(CMemoryPool *mp,
										CMDAccessor *md_accessor,
										IMDId *mdid_index)
{
	OID index_oid = CMDIdGPDB::CastMdid(mdid_index)->Oid();
	GPOS_ASSERT(0 != index_oid);
	IndexCatalogEntry *index_cat = s62::GetIndex(index_oid);

	if (NULL == index_cat)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid_index->GetBuffer());
	}

	const IMDRelation *md_rel = NULL;
	const PartitionCatalogEntry *part_cat = NULL;
	PropertySchemaCatalogEntry *ps_cat = NULL;
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

		idx_t psid = index_cat->GetPropertySchemaID();

		CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, psid);

		md_rel = md_accessor->RetrieveRel(mdid_rel);

		index_type = IMDIndex::EmdindBtree;
		mdid_item_type = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_ANY);
		if (IndexType::ART == index_cat->index_type)
		{
			index_type = IMDIndex::EmdindBtree;
		}
		else if (IndexType::FORWARD_CSR == index_cat->index_type)
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

		ps_cat = s62::GetRelation(psid);
		ULONG size = GPDXL_SYSTEM_COLUMNS + ps_cat->GetNumberOfColumns() + 1;

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
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	ULongPtrArray *included_cols = ComputeIncludedCols(mp, md_rel, index_type);
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

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::LookupLogicalIndexById
//
//	@doc:
//		Lookup an index given its id from the logical indexes structure
//
//---------------------------------------------------------------------------
LogicalIndexInfo *
CTranslatorS62ToDXL::LookupLogicalIndexById(
	LogicalIndexes *logical_indexes, OID oid)
{
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::ComputeIncludedCols
//
//	@doc:
//		Compute the included columns in an index
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorS62ToDXL::ComputeIncludedCols(CMemoryPool *mp,
											  const IMDRelation *md_rel,
											  IMDIndex::EmdindexType index_type
											  /*const PartitionCatalogEntry *part_cat*/)
{
	// TODO: 3/19/2012; currently we assume that all the columns
	// in the table are available from the index.

	ULongPtrArray *included_cols = GPOS_NEW(mp) ULongPtrArray(mp);
	const ULONG num_included_cols = md_rel->ColumnCount();
	if (index_type == IMDIndex::EmdindFwdAdjlist || index_type == IMDIndex::EmdindBwdAdjlist) 
	{
		// S62 fwd/bwd adjlist include only sid & tid columns
		GPOS_ASSERT(num_included_cols >= 2);
		included_cols->Append(GPOS_NEW(mp) ULONG(1));
		included_cols->Append(GPOS_NEW(mp) ULONG(2));
	}
	else
	{
		for (ULONG ul = 0; ul < num_included_cols; ul++)
		{
			included_cols->Append(GPOS_NEW(mp) ULONG(ul));
		}
	}

	return included_cols;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::GetAttributePosition
//
//	@doc:
//		Return the position of a given attribute
//
//---------------------------------------------------------------------------
ULONG
CTranslatorS62ToDXL::GetAttributePosition(INT attno,
											   ULONG *GetAttributePosition)
{
	ULONG idx = (ULONG)(GPDXL_SYSTEM_COLUMNS + attno);
	ULONG pos = GetAttributePosition[idx];
	GPOS_ASSERT(gpos::ulong_max != pos);

	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::PopulateAttnoPositionMap
//
//	@doc:
//		Populate the attribute to position mapping
//
//---------------------------------------------------------------------------
ULONG *
CTranslatorS62ToDXL::PopulateAttnoPositionMap(CMemoryPool *mp,
												   const IMDRelation *md_rel,
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
//		CTranslatorS62ToDXL::RetrieveType
//
//	@doc:
//		Retrieve a type from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDType *
CTranslatorS62ToDXL::RetrieveType(CMemoryPool *mp, IMDId *mdid)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != oid_type);
	IMDType *tmp;

	// check for supported base types
	switch (oid_type)
	{
		case GPDB_INT8_OID:
			return GPOS_NEW(mp) CMDTypeInt8GPDB(mp);
			break;

		case GPDB_BOOL:
			return GPOS_NEW(mp) CMDTypeBoolGPDB(mp);

		case GPDB_OID_OID:
			D_ASSERT(false);
			break;
	}

	// // get type name
	CMDName *mdname = GetTypeName(mp, mdid);

	BOOL is_fixed_length = false;
	ULONG length = 0;
	INT gpdb_length = 0;

	is_fixed_length = s62::isTypeFixedLength(oid_type);
	length = s62::GetTypeSize(oid_type);
	if (is_fixed_length) {
		gpdb_length = length;
	} else {
		gpdb_length = -1;
	}

	BOOL is_passed_by_value = true;

	// collect ids of different comparison operators for types
	CMDIdGPDB *mdid_op_eq =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, s62::GetComparisonOperator(oid_type, oid_type, CmptEq)/*ptce->eq_opr*/);
	CMDIdGPDB *mdid_op_neq = GPOS_NEW(mp)
		CMDIdGPDB(IMDId::EmdidGeneral, s62::GetComparisonOperator(oid_type, oid_type, CmptNEq)/*gpdb::GetInverseOp(ptce->eq_opr)*/);
	CMDIdGPDB *mdid_op_lt =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, s62::GetComparisonOperator(oid_type, oid_type, CmptLT)/*ptce->lt_opr*/);
	CMDIdGPDB *mdid_op_leq = GPOS_NEW(mp)
		CMDIdGPDB(IMDId::EmdidGeneral, s62::GetComparisonOperator(oid_type, oid_type, CmptLEq)/*gpdb::GetInverseOp(ptce->gt_opr)*/);
	CMDIdGPDB *mdid_op_gt =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, s62::GetComparisonOperator(oid_type, oid_type, CmptGT)/*ptce->gt_opr*/);
	CMDIdGPDB *mdid_op_geq = GPOS_NEW(mp)
		CMDIdGPDB(IMDId::EmdidGeneral, (OID) s62::GetComparisonOperator(oid_type, oid_type, CmptGEq)/*gpdb::GetInverseOp(ptce->lt_opr)*/);
	CMDIdGPDB *mdid_op_cmp =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, 0/*ptce->cmp_proc*/);
	BOOL is_hashable = true;
	BOOL is_merge_joinable = true;
	BOOL is_composite_type = false;
	BOOL is_text_related_type = false;

	// get standard aggregates
	CMDIdGPDB *mdid_min = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, s62::GetAggregate("min", oid_type, 1));
	CMDIdGPDB *mdid_max = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, s62::GetAggregate("max", oid_type, 1));
	CMDIdGPDB *mdid_avg = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, s62::GetAggregate("avg", oid_type, 1));
	CMDIdGPDB *mdid_sum = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, s62::GetAggregate("sum", oid_type, 1));

	// count aggregate is the same for all types
	CMDIdGPDB *mdid_count =
	 	GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, 2147/*COUNT_ANY_OID*/);

	// check if type is composite
	CMDIdGPDB *mdid_type_relid = NULL;

	// get array type mdid
	CMDIdGPDB *mdid_type_array = GPOS_NEW(mp)
	 	CMDIdGPDB(IMDId::EmdidGeneral, 0/*gpdb::GetArrayType(oid_type)*/);

	OID distr_opfamily = 0;

	BOOL is_redistributable = false;
	CMDIdGPDB *mdid_distr_opfamily = NULL;

	CMDIdGPDB *mdid_legacy_distr_opfamily = NULL;

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
//		CTranslatorS62ToDXL::RetrieveScOp
//
//	@doc:
//		Retrieve a scalar operator from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDScalarOpGPDB *
CTranslatorS62ToDXL::RetrieveScOp(CMemoryPool *mp, IMDId *mdid)
{
	OID op_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != op_oid);

	// get operator name
	string name_str = s62::GetOpName(op_oid);
	CHAR *name = std::strcpy(new char[name_str.length() + 1], name_str.c_str()); 

	if (NULL == name)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(mp, name);

	OID left_oid = InvalidOid;
	OID right_oid = InvalidOid;

	// get operator argument types
	s62::GetOpInputTypes(op_oid, &left_oid, &right_oid);

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
	CmpType cmpt = (CmpType) s62::GetComparisonType(op_oid);
	IMDType::ECmpType cmp_type = ParseCmpType(cmpt);

	// get func oid
	OID func_oid = s62::GetOpFunc(op_oid);
	GPOS_ASSERT(InvalidOid != func_oid);

	CMDIdGPDB *mdid_func =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, func_oid);

	// get result type
	OID result_oid = LOGICAL_TYPE_BASE_ID + (OID) LogicalTypeId::BOOLEAN;

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);

	// get commutator and inverse
	CMDIdGPDB *mdid_commute_opr = NULL;

	OID commute_oid = s62::GetCommutatorOp(op_oid);

	if (InvalidOid != commute_oid)
	{
		mdid_commute_opr =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, commute_oid);
	}

	CMDIdGPDB *m_mdid_inverse_opr = NULL;

	OID inverse_oid = s62::GetInverseOp(op_oid);

	if (InvalidOid != inverse_oid)
	{
		m_mdid_inverse_opr =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, inverse_oid);
	}

	BOOL returns_null_on_null_input = true;
	BOOL is_ndv_preserving = true;

	CMDIdGPDB *mdid_hash_opfamily = NULL;
	OID distr_opfamily;
	if (InvalidOid != distr_opfamily)
	{
		mdid_hash_opfamily =
			GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, distr_opfamily);
	}

	CMDIdGPDB *mdid_legacy_hash_opfamily = NULL;
	OID legacy_distr_opfamily;
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
//		CTranslatorS62ToDXL::LookupFuncProps
//
//	@doc:
//		Lookup function properties
//
//---------------------------------------------------------------------------
void
CTranslatorS62ToDXL::LookupFuncProps(
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
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveFunc
//
//	@doc:
//		Retrieve a function from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDFunctionGPDB *
CTranslatorS62ToDXL::RetrieveFunc(CMemoryPool *mp, IMDId *mdid)
{
	OID func_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != func_oid);

	// get aggfunc catalog entry
	s62::ScalarFunctionCatalogEntry *scalar_func_cat =
		s62::GetScalarFunc(func_oid);

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
	idx_t scalar_func_idx = s62::GetScalarFuncIndex(func_oid);
	GPOS_ASSERT(scalar_func_cat->functions->functions.size() > scalar_func_idx);
	OID result_oid = LOGICAL_TYPE_BASE_ID + (OID) scalar_func_cat->functions->functions[scalar_func_idx].return_type.id();
	
	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);

	IMdIdArray *arg_type_mdids = NULL;

	IMDFunction::EFuncStbl stability = IMDFunction::EfsImmutable;
	IMDFunction::EFuncDataAcc access = IMDFunction::EfdaNoSQL;
	BOOL is_strict = true;
	BOOL returns_set = true;
	BOOL is_ndv_preserving = true;
	BOOL is_allowed_for_PS = false;

	mdid->AddRef();
	CMDFunctionGPDB *md_func = GPOS_NEW(mp) CMDFunctionGPDB(
		mp, mdid, mdname, result_type_mdid, arg_type_mdids, returns_set,
		stability, access, is_strict, is_ndv_preserving, is_allowed_for_PS);

	return md_func;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveAgg
//
//	@doc:
//		Retrieve an aggregate from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDAggregateGPDB *
CTranslatorS62ToDXL::RetrieveAgg(CMemoryPool *mp, IMDId *mdid)
{

	OID agg_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != agg_oid);

	// get aggfunc catalog entry
	s62::AggregateFunctionCatalogEntry *agg_func_cat =
		s62::GetAggFunc(agg_oid);

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
	idx_t agg_func_idx = s62::GetAggFuncIndex(agg_oid);
	GPOS_ASSERT(agg_func_cat->functions->functions.size() > agg_func_idx);
	OID result_oid = LOGICAL_TYPE_BASE_ID + (OID) agg_func_cat->functions->functions[agg_func_idx].return_type.id();

	GPOS_ASSERT(InvalidOid != result_oid);

	CMDIdGPDB *result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);
	IMDId *intermediate_result_type_mdid =
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, result_oid);

	mdid->AddRef();

	BOOL is_ordered = false;

	// GPDB does not support splitting of ordered aggs and aggs without a
	// combine function
	BOOL is_splittable = false;

	// cannot use hash agg for ordered aggs or aggs without a combine func
	// due to the fact that hashAgg may spill
	BOOL is_hash_agg_capable = true;

	CMDAggregateGPDB *pmdagg = GPOS_NEW(mp) CMDAggregateGPDB(
		mp, mdid, mdname, result_type_mdid, intermediate_result_type_mdid,
		is_ordered, is_splittable, is_hash_agg_capable);
	return pmdagg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveTrigger
//
//	@doc:
//		Retrieve a trigger from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDTriggerGPDB *
CTranslatorS62ToDXL::RetrieveTrigger(CMemoryPool *mp, IMDId *mdid)
{
	D_ASSERT(false);
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveCheckConstraints
//
//	@doc:
//		Retrieve a check constraint from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDCheckConstraintGPDB *
CTranslatorS62ToDXL::RetrieveCheckConstraints(CMemoryPool *mp,
												   CMDAccessor *md_accessor,
												   IMDId *mdid)
{
	OID check_constraint_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	GPOS_ASSERT(InvalidOid != check_constraint_oid);

	// get name of the check constraint
	CHAR *name;
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
	OID rel_oid;
	GPOS_ASSERT(InvalidOid != rel_oid);
	CMDIdGPDB *mdid_rel = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, rel_oid);

	// translate the check constraint expression
	CDXLNode *scalar_dxlnode;

	mdid->AddRef();

	return GPOS_NEW(mp)
		CMDCheckConstraintGPDB(mp, mdid, mdname, mdid_rel, scalar_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::GetTypeName
//
//	@doc:
//		Retrieve a type's name from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDName *
CTranslatorS62ToDXL::GetTypeName(CMemoryPool *mp, IMDId *mdid)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->Oid();

	GPOS_ASSERT(InvalidOid != oid_type);

	CHAR *typename_str = std::strcpy(new char[s62::GetTypeName(oid_type).length() + 1], s62::GetTypeName(oid_type).c_str());
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
//		CTranslatorS62ToDXL::GetFuncStability
//
//	@doc:
//		Get function stability property from the GPDB character representation
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncStbl
CTranslatorS62ToDXL::GetFuncStability(CHAR c)
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
//		CTranslatorS62ToDXL::GetEFuncDataAccess
//
//	@doc:
//		Get function data access property from the GPDB character representation
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncDataAcc
CTranslatorS62ToDXL::GetEFuncDataAccess(CHAR c)
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
//		CTranslatorS62ToDXL::RetrieveAggIntermediateResultType
//
//	@doc:
//		Retrieve the type id of an aggregate's intermediate results
//
//---------------------------------------------------------------------------
IMDId *
CTranslatorS62ToDXL::RetrieveAggIntermediateResultType(CMemoryPool *mp,
															IMDId *mdid)
{
	OID agg_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	OID intermediate_type_oid;

	GPOS_ASSERT(InvalidOid != agg_oid);
	s62::AggregateFunctionCatalogEntry *agg_func_cat = s62::GetAggFunc(agg_oid);
	intermediate_type_oid = GPDB_BOOL;

	return GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, intermediate_type_oid);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveRelStats
//
//	@doc:
//		Retrieve relation statistics from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorS62ToDXL::RetrieveRelStats(CMemoryPool *mp, IMDId *mdid)
{
	CMDIdRelStats *m_rel_stats_mdid = CMDIdRelStats::CastMdid(mdid);
	IMDId *mdid_rel = m_rel_stats_mdid->GetRelMdId();
	OID rel_oid = CMDIdGPDB::CastMdid(mdid_rel)->Oid();

	PropertySchemaCatalogEntry *rel = s62::GetRelation(rel_oid);
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
		CHAR *relname = std::strcpy(new char[rel->GetName().length() + 1], rel->GetName().c_str());
		CWStringDynamic *relname_str =
			CDXLUtils::CreateDynamicStringFromCharArray(mp, relname);
		mdname = GPOS_NEW(mp) CMDName(mp, relname_str);
		// CMDName ctor created a copy of the string
		GPOS_DELETE(relname_str);

		num_rows = rel->GetNumberOfRowsApproximately();

		m_rel_stats_mdid->AddRef();
	}
	GPOS_CATCH_EX(ex)
	{
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
CTranslatorS62ToDXL::RetrieveColStats(CMemoryPool *mp,
										   CMDAccessor *md_accessor,
										   IMDId *mdid)
{
	CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);
	IMDId *mdid_rel = mdid_col_stats->GetRelMdId();
	ULONG pos = mdid_col_stats->Position();
	OID rel_oid = CMDIdGPDB::CastMdid(mdid_rel)->Oid();

	s62::PropertySchemaCatalogEntry *rel = s62::GetRelation(rel_oid);
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

	num_rows = rel->GetNumberOfRowsApproximately();

	// extract column name and type
	CMDName *md_colname =
		GPOS_NEW(mp) CMDName(mp, md_col->Mdname().GetMDName());
	OID att_type = CMDIdGPDB::CastMdid(md_col->MdidType())->Oid();

	CDXLBucketArray *dxl_stats_bucket_array = GPOS_NEW(mp) CDXLBucketArray(mp);

	if (0 > attno)
	{
		mdid_col_stats->AddRef();
		return GenerateStatsForSystemCols(mp, rel_oid, mdid_col_stats,
										  md_colname, att_type, attno,
										  dxl_stats_bucket_array, num_rows);
	}

	// null frequency and NDV
	CDouble null_freq(0.0);
	int null_ndv = 0;

	// column width
	idx_t width_penalty = 1;
	CDouble width = CDouble(width_penalty * s62::GetTypeSize(att_type));
	if (rel->is_fake) {
		// TODO: change hard coded value
		// 500 means the number of schemas.
		// This is for scaling the width for the temporal table
		width = width / 500;
	}

	// calculate total number of distinct values
	CDouble num_distinct(1.0);
	num_distinct = CDouble(s62::GetNDV(rel, attno));
	num_distinct = num_distinct.Ceil();

	BOOL is_dummy_stats = false;

	// histogram values extracted from the pg_statistic tuple for a given column
	AttStatsSlot hist_slot;
	s62::GetHistogramInfo(rel, attno, &hist_slot);

	if (InvalidOid != hist_slot.valuetype && hist_slot.valuetype != att_type)
	{
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
		if (rel->is_fake) {
			// TODO: change hard coded value
			// 500 means the number of schemas.
			// This is for scaling the width for the temporal table
			col_width = col_width / 500;
		}

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
//              CTranslatorS62ToDXL::GenerateStatsForSystemCols
//
//      @doc:
//              Generate statistics for the system level columns
//
//---------------------------------------------------------------------------
CDXLColStats *
CTranslatorS62ToDXL::GenerateStatsForSystemCols(
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
				distinct_remaining = CDouble(0.0);
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
//     CTranslatorS62ToDXL::RetrieveNumChildPartitions
//
//  @doc:
//      For non-leaf partition tables return the number of child partitions
//      else return 1
//
//---------------------------------------------------------------------------
ULONG
CTranslatorS62ToDXL::RetrieveNumChildPartitions(OID rel_oid)
{
	GPOS_ASSERT(InvalidOid != rel_oid);

	ULONG num_part_tables = 1;

	return num_part_tables;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveCast
//
//	@doc:
//		Retrieve a cast function from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorS62ToDXL::RetrieveCast(CMemoryPool *mp, IMDId *mdid)
{
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveScCmp
//
//	@doc:
//		Retrieve a scalar comparison from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorS62ToDXL::RetrieveScCmp(CMemoryPool *mp, IMDId *mdid)
{
	CMDIdScCmp *mdid_scalar_cmp = CMDIdScCmp::CastMdid(mdid);
	IMDId *mdid_left = mdid_scalar_cmp->GetLeftMdid();
	IMDId *mdid_right = mdid_scalar_cmp->GetRightMdid();

	IMDType::ECmpType cmp_type = mdid_scalar_cmp->ParseCmpType();

	OID left_oid = CMDIdGPDB::CastMdid(mdid_left)->Oid();
	OID right_oid = CMDIdGPDB::CastMdid(mdid_right)->Oid();
	CmpType cmpt = (CmpType) GetComparisonType(cmp_type);

	OID scalar_cmp_oid = s62::GetComparisonOperator(left_oid, right_oid, cmpt);

	if (InvalidOid == scalar_cmp_oid)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound,
				   mdid->GetBuffer());
	}

	string name_str = s62::GetOpName(scalar_cmp_oid);
	CHAR *name = std::strcpy(new char[name_str.length() + 1], name_str.c_str());

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
//		CTranslatorS62ToDXL::TransformStatsToDXLBucketArray
//
//	@doc:
//		transform stats from pg_stats form to optimizer's preferred form
//
//---------------------------------------------------------------------------
CDXLBucketArray *
CTranslatorS62ToDXL::TransformStatsToDXLBucketArray(
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
//		CTranslatorS62ToDXL::TransformMcvToOrcaHistogram
//
//	@doc:
//		Transform gpdb's mcv info to optimizer histogram
//
//---------------------------------------------------------------------------
CHistogram *
CTranslatorS62ToDXL::TransformMcvToOrcaHistogram(
	CMemoryPool *mp, const IMDType *md_type, const Datum *mcv_values,
	const float4 *mcv_frequencies, ULONG num_mcv_values)
{
	IDatumArray *datums = GPOS_NEW(mp) IDatumArray(mp);
	CDoubleArray *freqs = GPOS_NEW(mp) CDoubleArray(mp);

	GPOS_ASSERT(num_mcv_values == 0); // S62 we do not have mvc now

	CHistogram *hist = CStatisticsUtils::TransformMCVToHist(
		mp, md_type, datums, freqs, num_mcv_values);

	datums->Release();
	freqs->Release();
	return hist;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::TransformHistToOrcaHistogram
//
//	@doc:
//		Transform GPDB's hist info to optimizer's histogram
//
//---------------------------------------------------------------------------
CHistogram *CTranslatorS62ToDXL::TransformHistToOrcaHistogram(
    CMemoryPool *mp, const IMDType *md_type, const Datum *hist_values,
    const Datum *hist_freq_values, ULONG num_hist_values, CDouble num_distinct,
    CDouble hist_freq)
{
    GPOS_ASSERT(1 < num_hist_values);

	const ULONG num_buckets = num_hist_values - 1;
	CDouble distinct_per_bucket = num_distinct / CDouble(num_buckets);
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
//		CTranslatorS62ToDXL::TransformHistogramToDXLBucketArray
//
//	@doc:
//		Histogram to array of dxl buckets
//
//---------------------------------------------------------------------------
CDXLBucketArray *
CTranslatorS62ToDXL::TransformHistogramToDXLBucketArray(
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
//		CTranslatorS62ToDXL::RetrieveRelStorageType
//
//	@doc:
//		Get relation storage type
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype
CTranslatorS62ToDXL::RetrieveRelStorageType(CHAR storage_type)
{
	IMDRelation::Erelstoragetype rel_storage_type =
		IMDRelation::ErelstorageSentinel;

	rel_storage_type = IMDRelation::ErelstorageHeap;

	return rel_storage_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrievePartKeysAndTypes
//
//	@doc:
//		Get partition keys and types for relation or NULL if relation not partitioned.
//		Caller responsible for closing the relation if an exception is raised
//
//---------------------------------------------------------------------------
void
CTranslatorS62ToDXL::RetrievePartKeysAndTypes(CMemoryPool *mp,
												   Relation rel, OID oid,
												   ULongPtrArray **part_keys,
												   CharPtrArray **part_types)
{
	GPOS_ASSERT(NULL != rel);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::ConstructAttnoMapping
//
//	@doc:
//		Construct a mapping for GPDB attnos to positions in the columns array
//
//---------------------------------------------------------------------------
ULONG *
CTranslatorS62ToDXL::ConstructAttnoMapping(CMemoryPool *mp,
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
//		CTranslatorS62ToDXL::RetrieveRelKeysets
//
//	@doc:
//		Get key sets for relation
//
//---------------------------------------------------------------------------
ULongPtr2dArray *
CTranslatorS62ToDXL::RetrieveRelKeysets(CMemoryPool *mp, OID oid,
											 BOOL should_add_default_keys,
											 BOOL is_partitioned,
											 ULONG *attno_mapping)
{
	ULongPtr2dArray *key_sets = GPOS_NEW(mp) ULongPtr2dArray(mp);
	return key_sets;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::NormalizeFrequencies
//
//	@doc:
//		Sometimes a set of frequencies can add up to more than 1.0.
//		Fix these cases
//
//---------------------------------------------------------------------------
void
CTranslatorS62ToDXL::NormalizeFrequencies(float4 *freqs, ULONG length,
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
//		CTranslatorS62ToDXL::IsIndexSupported
//
//	@doc:
//		Check if index type is supported
//
//---------------------------------------------------------------------------
BOOL
CTranslatorS62ToDXL::IsIndexSupported(IndexCatalogEntry *index_cat)
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RelHasSystemColumns
//
//	@doc:
//		Does given relation type have system columns.
//		Currently only regular relations, sequences, toast values relations and
//		AO segment relations have system columns
//
//---------------------------------------------------------------------------
BOOL
CTranslatorS62ToDXL::RelHasSystemColumns(char rel_kind)
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::ParseCmpType
//
//	@doc:
//		Translate GPDB comparison types into optimizer comparison types
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CTranslatorS62ToDXL::ParseCmpType(ULONG cmpt)
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
//		CTranslatorS62ToDXL::GetComparisonType
//
//	@doc:
//		Translate optimizer comparison types into GPDB comparison types
//
//---------------------------------------------------------------------------
ULONG
CTranslatorS62ToDXL::GetComparisonType(IMDType::ECmpType cmp_type)
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
//		CTranslatorS62ToDXL::RetrieveIndexOpFamilies
//
//	@doc:
//		Retrieve the opfamilies for the keys of the given index
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorS62ToDXL::RetrieveIndexOpFamilies(CMemoryPool *mp,
												  IMDId *mdid_index)
{
	IMdIdArray *input_col_mdids = GPOS_NEW(mp) IMdIdArray(mp);

	OID op_family_logical_oid = s62::GetComparisonOperator(
		(idx_t)LogicalTypeId::ID + LOGICAL_TYPE_BASE_ID,
		(idx_t)LogicalTypeId::ID + LOGICAL_TYPE_BASE_ID,
		CmptEq);
	OID op_family_oid = s62::GetOpFamiliesForScOp(op_family_logical_oid);
	input_col_mdids->Append(
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, op_family_oid));

	return input_col_mdids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorS62ToDXL::RetrieveScOpOpFamilies
//
//	@doc:
//		Retrieve the families for the keys of the given scalar operator
//
//---------------------------------------------------------------------------
IMdIdArray *
CTranslatorS62ToDXL::RetrieveScOpOpFamilies(CMemoryPool *mp,
												 IMDId *mdid_scalar_op)
{
	OID op_family_oid =
		s62::GetOpFamiliesForScOp(CMDIdGPDB::CastMdid(mdid_scalar_op)->Oid());
	IMdIdArray *input_col_mdids = GPOS_NEW(mp) IMdIdArray(mp);

	input_col_mdids->Append(
		GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, op_family_oid));

	return input_col_mdids;
}

IMDId *
CTranslatorS62ToDXL::AddVirtualTable(CMemoryPool *mp, IMDId *mdid, IMdIdArray *pdrgmdid) {
	// Convert IMdIdArray to array of OIDs
	ULONG size = pdrgmdid->Size();
	uint32_t original_vtbl_oid = CMDIdGPDB::CastMdid(mdid)->Oid();
	uint32_t *oid_array = new uint32_t[size];
	
	for (ULONG i = 0; i < size; i++) {
		IMDId *graphlet_mdid = (*pdrgmdid)[i];
		oid_array[i] = CMDIdGPDB::CastMdid(graphlet_mdid)->Oid();
	}
	ULONG virtual_table_oid = s62::AddVirtualTable(original_vtbl_oid, oid_array, size);

	CMDIdGPDB *new_mdid = GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, virtual_table_oid, 0, 0);
	return new_mdid;
}

// EOF
