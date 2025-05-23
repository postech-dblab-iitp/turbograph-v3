//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDIdGPDB.h
//
//	@doc:
//		Class for representing id and version of metadata objects in GPDB
//---------------------------------------------------------------------------



#ifndef GPMD_CMDIdGPDB_H
#define GPMD_CMDIdGPDB_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDId.h"


// TODO:  - Feb 1, 2012; remove once system id is part of the mdid
#define GPMD_GPDB_SYSID GPOS_WSZ_LIT("GPDB")

namespace gpmd
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CMDIdGPDB
//
//	@doc:
//		Class for representing ids of GPDB metadata objects
//
//---------------------------------------------------------------------------
class CMDIdGPDB : public IMDId
{
protected:
	// source system id
	CSystemId m_sysid;

	// id from GPDB system catalog
	OID m_oid;

	// major version number
	ULONG m_major_version;

	// minor version number
	ULONG m_minor_version;

	// buffer for the serialized mdid
	WCHAR m_mdid_array[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// serialize mdid
	virtual void Serialize();

public:
	// ctors
	CMDIdGPDB(CSystemId sysid, OID oid);
	CMDIdGPDB(EMDIdType mdIdType, OID oid, ULONG version_major = 1,
			  ULONG version_minor = 0);

	// copy ctor
	explicit CMDIdGPDB(const CMDIdGPDB &mdidSource);

	virtual EMDIdType
	MdidType() const
	{
		return m_sysid.MdidType();
	}

	// string representation of mdid
	virtual const WCHAR *GetBuffer() const;

	// source system id
	virtual CSystemId
	Sysid() const
	{
		return m_sysid;
	}

	// oid
	virtual OID Oid() const;

	// major version
	virtual ULONG VersionMajor() const;

	// minor version
	virtual ULONG VersionMinor() const;

	// equality check
	virtual BOOL Equals(const IMDId *mdid) const;

	// computes the hash value for the metadata id
	virtual ULONG
	HashValue() const
	{
		return gpos::CombineHashes(
			MdidType(),
			gpos::CombineHashes(
				gpos::HashValue(&m_oid),
				gpos::CombineHashes(gpos::HashValue(&m_major_version),
									gpos::HashValue(&m_minor_version))));
	}

	// is the mdid valid
	virtual BOOL IsValid() const;

	// serialize mdid in DXL as the value of the specified attribute
	virtual void Serialize(CXMLSerializer *xml_serializer,
						   const CWStringConst *pstrAttribute) const;

	// debug print of the metadata id
	virtual IOstream &OsPrint(IOstream &os) const;

	// const converter
	static const CMDIdGPDB *
	CastMdid(const IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid);

		return dynamic_cast<const CMDIdGPDB *>(mdid);
	}

	// non-const converter
	static CMDIdGPDB *
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid);

		return dynamic_cast<CMDIdGPDB *>(mdid);
	}

	// invalid mdid
	static CMDIdGPDB m_mdid_invalid_key;

	// int2 mdid
	static CMDIdGPDB m_mdid_int2;

	// int4 mdid
	static CMDIdGPDB m_mdid_int4;

	// int8 mdid
	static CMDIdGPDB m_mdid_int8;

	// oid mdid
	static CMDIdGPDB m_mdid_oid;

	// bool mdid
	static CMDIdGPDB m_mdid_bool;

	// numeric mdid
	static CMDIdGPDB m_mdid_numeric;

	// date mdid
	static CMDIdGPDB m_mdid_date;

	// time mdid
	static CMDIdGPDB m_mdid_time;

	// time with time zone mdid
	static CMDIdGPDB m_mdid_timeTz;

	// timestamp mdid
	static CMDIdGPDB m_mdid_timestamp;

	// timestamp with time zone mdid
	static CMDIdGPDB m_mdid_timestampTz;

	// absolute time mdid
	static CMDIdGPDB m_mdid_abs_time;

	// relative time mdid
	static CMDIdGPDB m_mdid_relative_time;

	// interval mdid
	static CMDIdGPDB m_mdid_interval;

	// time interval mdid
	static CMDIdGPDB m_mdid_time_interval;

	// char mdid
	static CMDIdGPDB m_mdid_char;

	// bpchar mdid
	static CMDIdGPDB m_mdid_bpchar;

	// varchar mdid
	static CMDIdGPDB m_mdid_varchar;

	// text mdid
	static CMDIdGPDB m_mdid_text;

	// name mdid
	static CMDIdGPDB m_mdid_name;

	// float4 mdid
	static CMDIdGPDB m_mdid_float4;

	// float8 mdid
	static CMDIdGPDB m_mdid_float8;

	// cash mdid
	static CMDIdGPDB m_mdid_cash;

	// inet mdid
	static CMDIdGPDB m_mdid_inet;

	// cidr mdid
	static CMDIdGPDB m_mdid_cidr;

	// macaddr mdid
	static CMDIdGPDB m_mdid_macaddr;

	// count(*) mdid
	static CMDIdGPDB m_mdid_count_star;

	// count(Any) mdid
	static CMDIdGPDB m_mdid_count_any;

	// uuid mdid
	static CMDIdGPDB m_mdid_uuid;

	// s62 ubigint mdid
	static CMDIdGPDB m_mdid_s62_ubigint;

	// s62 ID mdid
	static CMDIdGPDB m_mdid_s62_id;

	// s62 bigint mdid
	static CMDIdGPDB m_mdid_s62_bigint;

	// s62 PATH mdid
	static CMDIdGPDB m_mdid_s62_path;

	// unknown datatype mdid
	static CMDIdGPDB m_mdid_unknown;
};

}  // namespace gpmd



#endif	// !GPMD_CMDIdGPDB_H

// EOF
