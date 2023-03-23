//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalIndexPathScan.cpp
//
//	@doc:
//		Implementation of index scan operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalIndexPathScan.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathScan::CPhysicalIndexPathScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalIndexPathScan::CPhysicalIndexPathScan(CMemoryPool *mp,
						CIndexDescriptorArray *pindexdescArray,
					   CTableDescriptorArray *ptabdescArray,
					   ULONG ulOriginOpId,
					   const CName *pnameAlias,
					   CColRefArray *pdrgpcrOutput,
					   COrderSpec *pos,
						INT path_join_lower_bound,
				 	    INT path_join_upper_bound
					   )
	: CPhysical(mp),
	  m_pnameAlias(pnameAlias),
	  m_pindexdesc(pindexdescArray),
	  m_ptabdesc(ptabdescArray),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pds(NULL),
	  m_ulOriginOpId(ulOriginOpId),
	  m_pos(pos),
	  path_join_lower_bound(path_join_lower_bound),
	  path_join_upper_bound(path_join_upper_bound)
{
	GPOS_ASSERT(NULL != pindexdescArray);
	GPOS_ASSERT(NULL != pos);

	// TODO derive from 
	m_pds = GPOS_NEW(m_mp) CDistributionSpecSingleton();
	m_pds->AddRef();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathScan::~CPhysicalIndexPathScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalIndexPathScan::~CPhysicalIndexPathScan()
{
	m_pindexdesc->Release();
	m_pos->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalIndexPathScan::EpetOrder(CExpressionHandle &,	// exprhdl
							  const CEnfdOrder *peo) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	if (peo->FCompatible(m_pos))
	{
		// required order is already established by the index
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

CDistributionSpec *
CPhysicalIndexPathScan::PdsDerive(CMemoryPool *, CExpressionHandle &) const
{
	m_pds->AddRef();
	return m_pds;
}


BOOL
CPhysicalIndexPathScan::FProvidesReqdCols(CExpressionHandle &,  // exprhdl
								 CColRefSet *pcrsRequired,
								 ULONG	// ulOptReq
) const
{
	GPOS_ASSERT(NULL != pcrsRequired);

	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	pcrs->Include(m_pdrgpcrOutput);

	BOOL result = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return result;
}



//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathScan::HashValue
//
//	@doc:
//		Combine pointers for table descriptor, index descriptor and Eop
//
//---------------------------------------------------------------------------
ULONG
CPhysicalIndexPathScan::HashValue() const
{
	// ULONG ulHash = gpos::CombineHashes(
	// 	COperator::HashValue(),
	// 	gpos::CombineHashes(m_pindexdesc->MDId()->HashValue(),
	// 						gpos::HashPtr<CTableDescriptor>(m_ptabdesc)));
	// ulHash =
	// 	gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));
	// return ulHash;

	return CUtils::UlHashColArray(m_pdrgpcrOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathScan::Matches
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalIndexPathScan::Matches(COperator * ) const
{
	return false;
	//return CUtils::FMatchIndex(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalIndexPathScan::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	// TODO
	os << SzId() << " ";
	// // index name
	// os << "  Index Name: (";
	// m_pindexdesc->Name().OsPrint(os);
	// // table name
	// os << ")";
	// os << ", Table Name: (";
	// m_ptabdesc->Name().OsPrint(os);
	// os << ")";
	os << " Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "]";
	os << ", Lower Bound: " << path_join_lower_bound << ", Upper Bound: " << path_join_upper_bound << " ";

	return os;
}

// EOF
