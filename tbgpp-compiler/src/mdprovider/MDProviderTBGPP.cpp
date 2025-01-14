// extern "C" {
// #include "postgres.h"
// }
#include "gpopt/mdcache/CMDAccessor.h"
#include "mdprovider/MDProviderTBGPP.h"
// #include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "translate/CTranslatorTBGPPToDXL.hpp"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

uint64_t MDProviderTBGPP::hash_mdid(std::vector<OID> &oids)
{
	uint64_t hash = 0;
	for (ULONG i = 0; i < oids.size(); i++)
	{
		hash ^= std::hash<uint64_t>()(oids[i]) + (hash << 6) + (hash >> 2);
	}
	return hash;
}

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

IMDId *MDProviderTBGPP::AddVirtualTable(CMemoryPool *mp, IMDId *mdid,
                                        IMdIdArray *pdrgmdid)
{
    std::cout << "Adding virtual table, hash table size: "
              << m_virtual_tables.size() << std::endl;

    // Convert IMdIdArray to vector of OIDs and sort
    std::vector<OID> oids;
    for (ULONG i = 0; i < pdrgmdid->Size(); i++) {
        IMDId *graphlet_mdid = (*pdrgmdid)[i];
        oids.push_back(CMDIdGPDB::CastMdid(graphlet_mdid)->Oid());
    }
    std::sort(oids.begin(), oids.end());

    uint64_t hash = hash_mdid(oids);
    auto iter = m_virtual_tables.find(hash);

    if (iter == m_virtual_tables.end()) {
        m_virtual_tables[hash] =
            std::vector<std::pair<IMDId *, std::vector<OID>>>();
    }
    else {
        // Check if we already have this exact virtual table
        for (const auto &pair : iter->second) {
            if (pair.second == oids) {
				std::cout << "Found existing virtual table for hash " << hash << std::endl;
                return pair.first;
            }
        }
    }

    IMDId *new_mdid =
        CTranslatorTBGPPToDXL::AddVirtualTable(mp, mdid, pdrgmdid);
    m_virtual_tables[hash].push_back(std::make_pair(new_mdid, std::move(oids)));

    return new_mdid;
}

bool MDProviderTBGPP::CheckVirtualTableExists(std::vector<uint64_t> &oids,
                                              uint64_t &virtual_table_oid)
{
    std::vector<OID> oids_vec;
    for (uint64_t oid : oids) {
        oids_vec.push_back(oid);
    }
    uint64_t hash = hash_mdid(oids_vec);
    auto iter = m_virtual_tables.find(hash);
    if (iter == m_virtual_tables.end()) {
        return false;
    }
    else {
        for (const auto &pair : iter->second) {
            if (pair.second == oids_vec) {
                std::cout << "Found existing virtual table for hash " << hash
                          << " with oid "
                          << CMDIdGPDB::CastMdid(pair.first)->Oid()
                          << std::endl;
                virtual_table_oid =
                    (uint64_t)CMDIdGPDB::CastMdid(pair.first)->Oid();
                return true;
            }
        }
    }
    return false;
}

void MDProviderTBGPP::AddVirtualTable(std::vector<uint64_t> &oids,
                                      uint64_t virtual_table_oid)
{
    std::vector<OID> oids_vec;
    for (uint64_t oid : oids) {
        oids_vec.push_back(oid);
    }
    uint64_t hash = hash_mdid(oids_vec);
    auto iter = m_virtual_tables.find(hash);
    if (iter == m_virtual_tables.end()) {
        m_virtual_tables[hash] =
            std::vector<std::pair<IMDId *, std::vector<OID>>>();

        IMDId *new_mdid =
            GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidRel, virtual_table_oid, 0, 0);
        m_virtual_tables[hash].push_back(
            std::make_pair(new_mdid, std::move(oids_vec)));
    }
    else {
        IMDId *new_mdid =
            GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidRel, virtual_table_oid, 0, 0);
        m_virtual_tables[hash].push_back(
            std::make_pair(new_mdid, std::move(oids_vec)));
    }
}

// EOF
