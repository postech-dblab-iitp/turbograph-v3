// //---------------------------------------------------------------------------
// //	Greenplum Database
// //	Copyright (C) 2011 EMC Corp.
// //
// //	@filename:
// //		CMDProviderRelcache.cpp
// //
// //	@doc:
// //		Implementation of a relcache-based metadata provider, which uses GPDB's
// //		relcache to lookup objects given their ids.
// //
// //	@test:
// //
// //
// //---------------------------------------------------------------------------

// extern "C" {
// #include "optimizer/orca/postgres.h"
// }
// #include "gpopt/mdcache/CMDAccessor.h"
// #include "optimizer/orca/gpopt/relcache/CMDProviderRelcache.h"
// #include "optimizer/orca/gpopt/translate/CTranslatorRelcacheToDXL.h"
// #include "naucrates/dxl/CDXLUtils.h"
// #include "naucrates/exception.h"

// using namespace gpos;
// using namespace gpdxl;
// using namespace gpmd;

// CMDProviderRelcache::CMDProviderRelcache(CMemoryPool *mp) : m_mp(mp)
// {
// 	GPOS_ASSERT(NULL != m_mp);
// }

// //---------------------------------------------------------------------------
// //	@function:
// //		CMDProviderRelcache::GetMDObjDXLStr
// //
// //	@doc:
// //		Returns the DXL of the requested object in the provided memory pool
// //
// //---------------------------------------------------------------------------
// CWStringBase *
// CMDProviderRelcache::GetMDObjDXLStr(CMemoryPool *mp, CMDAccessor *md_accessor,
// 									IMDId *md_id,
// 									IMDCacheObject::Emdtype mdtype) const
// {
// 	IMDCacheObject *md_obj = CTranslatorRelcacheToDXL::RetrieveObject(
// 		mp, md_accessor, md_id, mdtype);

// 	GPOS_ASSERT(NULL != md_obj);

// 	CWStringDynamic *str = CDXLUtils::SerializeMDObj(
// 		m_mp, md_obj, true /*fSerializeHeaders*/, false /*findent*/);

// 	// cleanup DXL object
// 	md_obj->Release();

// 	return str;
// }

// // EOF
