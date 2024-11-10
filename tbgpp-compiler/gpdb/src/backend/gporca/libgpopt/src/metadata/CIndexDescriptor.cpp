//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CIndexDescriptor.cpp
//
//	@doc:
//		Implementation of index description
//---------------------------------------------------------------------------

#include "gpopt/metadata/CIndexDescriptor.h"

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CIndexDescriptor);

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::CIndexDescriptor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CIndexDescriptor::CIndexDescriptor(
	CMemoryPool *mp, IMDId *pmdidIndex, const CName &name,
	CColumnDescriptorArray *pdrgcoldescKeyCols,
	CColumnDescriptorArray *pdrgcoldescIncludedCols, BOOL is_clustered,
	IMDIndex::EmdindexType index_type)
	: m_pmdidIndex(pmdidIndex),
	  m_name(mp, name),
	  m_pdrgpcoldescKeyCols(pdrgcoldescKeyCols),
	  m_pdrgpcoldescIncludedCols(pdrgcoldescIncludedCols),
	  m_clustered(is_clustered),
	  m_index_type(index_type),
	  m_instance_descriptor(false)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(pmdidIndex->IsValid());
	GPOS_ASSERT(NULL != pdrgcoldescKeyCols);
	GPOS_ASSERT(NULL != pdrgcoldescIncludedCols);
}

CIndexDescriptor::CIndexDescriptor(
	CMemoryPool *mp, IMDId *pmdidIndex, const CName &name,
	CColumnDescriptorArray *pdrgcoldescKeyCols,
	CColumnDescriptorArray *pdrgcoldescIncludedCols, BOOL is_clustered,
	IMDIndex::EmdindexType index_type, BOOL is_instance_descriptor,
	IMdIdArray *table_ids_in_group)
	: m_pmdidIndex(pmdidIndex),
	  m_name(mp, name),
	  m_pdrgpcoldescKeyCols(pdrgcoldescKeyCols),
	  m_pdrgpcoldescIncludedCols(pdrgcoldescIncludedCols),
	  m_clustered(is_clustered),
	  m_index_type(index_type),
	  m_instance_descriptor(is_instance_descriptor)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(pmdidIndex->IsValid());
	GPOS_ASSERT(NULL != pdrgcoldescKeyCols);
	GPOS_ASSERT(NULL != pdrgcoldescIncludedCols);

	if (is_instance_descriptor) {
		GPOS_ASSERT(NULL != table_ids_in_group);
		m_table_ids_in_group = GPOS_NEW(mp) IMdIdArray(mp);

		for (ULONG ul = 0; ul < table_ids_in_group->Size(); ul++)
		{
			IMDId *table_mdid = (*table_ids_in_group)[ul];
			GPOS_ASSERT(NULL != table_mdid && table_mdid->IsValid());
			table_mdid->AddRef();
			m_table_ids_in_group->Append(table_mdid);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::~CIndexDescriptor
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CIndexDescriptor::~CIndexDescriptor()
{
	m_pmdidIndex->Release();

	m_pdrgpcoldescKeyCols->Release();
	m_pdrgpcoldescIncludedCols->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Keys
//
//	@doc:
//		number of key columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::Keys() const
{
	return m_pdrgpcoldescKeyCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::UlIncludedColumns
//
//	@doc:
//		Number of included columns
//
//---------------------------------------------------------------------------
ULONG
CIndexDescriptor::UlIncludedColumns() const
{
	// array allocated in ctor
	GPOS_ASSERT(NULL != m_pdrgpcoldescIncludedCols);

	return m_pdrgpcoldescIncludedCols->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::Pindexdesc
//
//	@doc:
//		Create the index descriptor from the table descriptor and index
//		information from the catalog
//
//---------------------------------------------------------------------------
CIndexDescriptor *
CIndexDescriptor::Pindexdesc(CMemoryPool *mp, const CTableDescriptor *ptabdesc,
							 const IMDIndex *pmdindex)
{
	CWStringConst strIndexName(mp, pmdindex->Mdname().GetMDName()->GetBuffer());

	CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->OrgPdrgpcoldesc();

	pmdindex->MDId()->AddRef();

	// array of index column descriptors
	CColumnDescriptorArray *pdrgcoldescKey =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);

	for (ULONG ul = 0; ul < pmdindex->Keys(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescKey->Append(pcoldesc);
	}

	// array of included column descriptors
	CColumnDescriptorArray *pdrgcoldescIncluded =
		GPOS_NEW(mp) CColumnDescriptorArray(mp);
	for (ULONG ul = 0; ul < pmdindex->IncludedCols(); ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		pcoldesc->AddRef();
		pdrgcoldescIncluded->Append(pcoldesc);
	}


	// create the index descriptors
	CIndexDescriptor *pindexdesc;
	if (ptabdesc->IsInstanceDescriptor()) {
		pindexdesc = GPOS_NEW(mp) CIndexDescriptor(
			mp, pmdindex->MDId(), CName(&strIndexName), pdrgcoldescKey,
			pdrgcoldescIncluded, pmdindex->IsClustered(), pmdindex->IndexType(),
			ptabdesc->IsInstanceDescriptor(), ptabdesc->GetTableIdsInGroup());
	} else {
		pindexdesc = GPOS_NEW(mp) CIndexDescriptor(
			mp, pmdindex->MDId(), CName(&strIndexName), pdrgcoldescKey,
			pdrgcoldescIncluded, pmdindex->IsClustered(), pmdindex->IndexType());
	}
	return pindexdesc;
}

BOOL
CIndexDescriptor::SupportsIndexOnlyScan() const
{
	return (m_index_type == IMDIndex::EmdindBtree ||
			m_index_type == IMDIndex::EmdindFwdAdjlist ||
			m_index_type == IMDIndex::EmdindBwdAdjlist);
}

//---------------------------------------------------------------------------
//	@function:
//		CIndexDescriptor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CIndexDescriptor::OsPrint(IOstream &os) const
{
	m_name.OsPrint(os);
	os << ": (Keys :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescKeyCols,
							   m_pdrgpcoldescKeyCols->Size());
	os << "); ";

	os << "(Included Columns :";
	CUtils::OsPrintDrgPcoldesc(os, m_pdrgpcoldescIncludedCols,
							   m_pdrgpcoldescIncludedCols->Size());
	os << ")";

	os << " [ Clustered :";
	if (m_clustered)
	{
		os << "true";
	}
	else
	{
		os << "false";
	}
	os << " ]";
	return os;
}

// EOF
