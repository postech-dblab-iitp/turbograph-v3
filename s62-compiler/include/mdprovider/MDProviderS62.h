#ifndef MDProviderS62_H
#define MDProviderS62_H

#include "gpos/base.h"
#include "gpos/string/CWStringBase.h"

#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDProvider.h"

#include <unordered_map>
#include <vector>

// fwd decl
namespace gpopt
{
class CMDAccessor;
}

namespace gpmd
{
using namespace gpos;

// arrays of OID
typedef CDynamicPtrArray<OID, CleanupDelete> OIDArray;

class MDProviderS62 : public IMDProvider
{
   private:
    // memory pool
    CMemoryPool *m_mp;

    // private copy ctor
    MDProviderS62(const MDProviderS62 &);

    // hash table for virtual tables
    std::unordered_map<uint64_t,
                       std::vector<std::pair<IMDId *, std::vector<OID>>>>
        m_virtual_tables;

    // hash function for mdid array
    uint64_t hash_mdid(std::vector<OID> &oids);

   public:
    // ctor/dtor
    explicit MDProviderS62(CMemoryPool *mp);

    ~MDProviderS62() {}

    // returns the DXL string of the requested metadata object
    virtual CWStringBase *GetMDObjDXLStr(CMemoryPool *mp,
                                         CMDAccessor *md_accessor, IMDId *md_id,
                                         IMDCacheObject::Emdtype mdtype) const;

    // return the mdid for the requested type
    virtual IMDId *MDId(CMemoryPool *mp, CSystemId sysid,
                        IMDType::ETypeInfo type_info) const
    {
        return GetGPDBTypeMdid(mp, sysid, type_info);
    }

    virtual IMDId *AddVirtualTable(CMemoryPool *mp, IMDId *mdid,
                                   IMdIdArray *pdrgmdid);

    void AddVirtualTable(std::vector<uint64_t> &oids, uint64_t virtual_table_oid);

	bool CheckVirtualTableExists(std::vector<uint64_t> &oids, uint64_t &virtual_table_oid);
};
}  // namespace gpmd



#endif	// !MDProviderS62_H

// EOF
