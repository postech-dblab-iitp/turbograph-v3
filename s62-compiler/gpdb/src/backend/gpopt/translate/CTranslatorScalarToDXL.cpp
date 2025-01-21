//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CTranslatorScalarToDXL.cpp
//
//	@doc:
//		Implementing the methods needed to translate a GPDB Scalar Operation (in a Query / PlStmt object)
//		into a DXL trees
//
//	@test:
//
//---------------------------------------------------------------------------
extern "C" {
#include "postgres.h"
}

#include <vector>

#include "gpos/base.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/IMDType.h"

using namespace gpdxl;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorScalarToDXL::GetDatum
//
//	@doc:
//		Create CDXLDatum from GPDB datum
//---------------------------------------------------------------------------
CDXLDatum *
CTranslatorScalarToDXL::TranslateDatumToDXL(CMemoryPool *mp,
											const IMDType *md_type,
											INT type_modifier, BOOL is_null,
											ULONG len, Datum datum)
{
	return TranslateGenericDatumToDXL(mp, md_type, type_modifier, is_null,
										  len, datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorScalarToDXL::TranslateGenericDatumToDXL
//
//	@doc:
//		Translate a datum of generic type
//---------------------------------------------------------------------------
CDXLDatum *
CTranslatorScalarToDXL::TranslateGenericDatumToDXL(CMemoryPool *mp,
												   const IMDType *md_type,
												   INT type_modifier,
												   BOOL is_null, ULONG len,
												   Datum datum)
{
	CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
	CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

	BYTE *bytes = ExtractByteArrayFromDatum(mp, md_type, is_null, len, datum);
	ULONG length = 0;
	if (!is_null)
	{
        length = len;
	}

	CDouble double_value(0);


	LINT lint_value = 0;
	if (CMDTypeGenericGPDB::HasByte2IntMapping(md_type))
	{
		IMDId *base_mdid = GPOS_NEW(mp)
			CMDIdGPDB(IMDId::EmdidGeneral, mdid->Oid()/*gpdb::GetBaseType(mdid->Oid())*/);
		// base_mdid is used for text related domain types
		lint_value = ExtractLintValueFromDatum(md_type, is_null, bytes, length,
											   base_mdid);
	}

	return CMDTypeGenericGPDB::CreateDXLDatumVal(
		mp, mdid, md_type, type_modifier, is_null, bytes, length, lint_value,
		double_value);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorScalarToDXL::ExtractByteArrayFromDatum
//
//	@doc:
//		Extract the byte array value of the datum. The result is NULL if datum is NULL
//---------------------------------------------------------------------------
BYTE *
CTranslatorScalarToDXL::ExtractByteArrayFromDatum(CMemoryPool *mp,
												  const IMDType *md_type,
												  BOOL is_null, ULONG len,
												  Datum datum)
{
	ULONG length = 0;
	BYTE *bytes = NULL;

	if (is_null)
	{
		return bytes;
	}

    length = len;
	GPOS_ASSERT(length > 0);

	bytes = GPOS_NEW_ARRAY(mp, BYTE, length);

	if (md_type->IsPassedByValue())
	{
		GPOS_ASSERT(length <= ULONG(sizeof(Datum)));
		clib::Memcpy(bytes, &datum, length);
	}
	else
	{
		// clib::Memcpy(bytes, gpdb::PointerFromDatum(datum), length);
	}

	return bytes;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorScalarToDXL::ExtractLintValueFromDatum
//
//	@doc:
//		Extract the long int value of a datum
//---------------------------------------------------------------------------
LINT
CTranslatorScalarToDXL::ExtractLintValueFromDatum(const IMDType *md_type,
												  BOOL is_null, BYTE *bytes,
												  ULONG length,
												  IMDId *base_mdid)
{
	IMDId *mdid = md_type->MDId();
	GPOS_ASSERT(CMDTypeGenericGPDB::HasByte2IntMapping(md_type));

	LINT lint_value = 0;
	if (is_null)
	{
		return lint_value;
	}

	if (mdid->Equals(&CMDIdGPDB::m_mdid_cash) ||
		mdid->Equals(&CMDIdGPDB::m_mdid_date))
	{
		GPOS_ASSERT(false);
	}
	else
	{
		// use hash value
		if (is_null)
		{
			lint_value = gpos::HashValue<LINT>(&lint_value);
		}
		else
		{
			if (mdid->Equals(&CMDIdGPDB::m_mdid_s62_ubigint) ||
				mdid->Equals(&CMDIdGPDB::m_mdid_s62_bigint) ||
				mdid->Equals(&CMDIdGPDB::m_mdid_s62_id)) {
				clib::Memcpy(&lint_value, bytes, length);
			} else {
				GPOS_ASSERT(false);
			}
		}
	}

	return lint_value;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorScalarToDXL::TranslateDatumToDXL
//
//	@doc:
//		Create IDatum from GPDB datum
//---------------------------------------------------------------------------
IDatum *
CTranslatorScalarToDXL::CreateIDatumFromGpdbDatum(CMemoryPool *mp,
												  const IMDType *md_type,
												  BOOL is_null,
												  Datum gpdb_datum)
{
	ULONG length = md_type->Length();
	GPOS_ASSERT(is_null || length > 0);

	CDXLDatum *datum_dxl = CTranslatorScalarToDXL::TranslateDatumToDXL(
		mp, md_type, gpmd::default_type_modifier, is_null, length, gpdb_datum);
	IDatum *datum = md_type->GetDatumForDXLDatum(mp, datum_dxl);
	datum_dxl->Release();
	return datum;
}

// EOF
