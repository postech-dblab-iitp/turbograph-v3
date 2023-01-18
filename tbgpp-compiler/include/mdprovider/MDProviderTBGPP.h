#ifndef MDProviderTBGPP_H
#define MDProviderTBGPP_H

#include "gpos/base.h"
#include "gpos/string/CWStringBase.h"

#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"

// fwd decl
namespace gpopt
{
class CMDAccessor;
}

namespace gpmd
{
using namespace gpos;

class MDProviderTBGPP : public IMDProvider
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// private copy ctor
	MDProviderTBGPP(const MDProviderTBGPP &);

public:
	// ctor/dtor
	explicit MDProviderTBGPP(CMemoryPool *mp);

	~MDProviderTBGPP()
	{
	}

	// returns the DXL string of the requested metadata object
	virtual CWStringBase *GetMDObjDXLStr(CMemoryPool *mp,
										 CMDAccessor *md_accessor, IMDId *md_id,
										 IMDCacheObject::Emdtype mdtype) const;

	// return the mdid for the requested type
	virtual IMDId *
	MDId(CMemoryPool *mp, CSystemId sysid, IMDType::ETypeInfo type_info) const
	{
		return GetGPDBTypeMdid(mp, sysid, type_info);
	}
};
}  // namespace gpmd



#endif	// !MDProviderTBGPP_H

// EOF
