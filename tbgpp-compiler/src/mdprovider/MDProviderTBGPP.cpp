// extern "C" {
// #include "postgres.h"
// }
#include "gpopt/mdcache/CMDAccessor.h"
#include "mdprovider/MDProviderTBGPP.h"
// #include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "translate/CTranslatorTBGPPToDXL.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

MDProviderTBGPP::MDProviderTBGPP(CMemoryPool *mp) : m_mp(mp)
{
	GPOS_ASSERT(NULL != m_mp);
}

CWStringBase *
MDProviderTBGPP::GetMDObjDXLStr(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMDId *md_id,
									IMDCacheObject::Emdtype mdtype) const
{
	IMDCacheObject *md_obj = CTranslatorTBGPPToDXL::RetrieveObject(
		mp, md_accessor, md_id, mdtype);

	GPOS_ASSERT(NULL != md_obj);

	CWStringDynamic *str = CDXLUtils::SerializeMDObj(
		m_mp, md_obj, true /*fSerializeHeaders*/, false /*findent*/);

	// cleanup DXL object
	md_obj->Release();

	return str;
}

// EOF
