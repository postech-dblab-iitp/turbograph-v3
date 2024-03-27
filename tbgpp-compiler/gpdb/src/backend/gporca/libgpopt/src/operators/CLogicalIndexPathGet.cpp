//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIndexPathGet.cpp
//
//	@doc:
//		Implementation of basic index access
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalIndexPathGet.h"

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::CLogicalIndexPathGet
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalIndexPathGet::CLogicalIndexPathGet(CMemoryPool *mp)
	: CLogical(mp),
	  m_pindexdesc(NULL),
	  m_ptabdesc(NULL),
	  m_ulOriginOpId(gpos::ulong_max),
	  m_pnameAlias(NULL),
	  m_pdrgpcrOutput(NULL),
	  m_pcrsOutput(NULL),
	  m_pos(NULL),
	  m_pcrsDist(NULL)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::CLogicalIndexPathGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalIndexPathGet::CLogicalIndexPathGet(CMemoryPool *mp,
					const std::vector<const IMDIndex *> pmdindexArray,
					 CTableDescriptorArray *ptabdescArray,
					 ULONG ulOriginOpId,
					 const CName *pnameAlias,
					 CColRefArray *pdrgpcrOutput,
					 INT path_join_lower_bound,
				 	 INT path_join_upper_bound
					 )
	: CLogical(mp),
	  m_pindexdesc(NULL),
	  m_ptabdesc(ptabdescArray),
	  m_ulOriginOpId(ulOriginOpId),
	  m_pnameAlias(pnameAlias),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pcrsOutput(NULL),
	  m_pcrsDist(NULL),
	  path_join_lower_bound(path_join_lower_bound),
	  path_join_upper_bound(path_join_upper_bound)
{
	GPOS_ASSERT(NULL != ptabdescArray);
	GPOS_ASSERT( pmdindexArray.size() == ptabdescArray->Size() );
	GPOS_ASSERT(NULL != pnameAlias);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	// create the index descriptor
	m_pindexdesc = GPOS_NEW(mp) CIndexDescriptorArray(mp);
	m_pindexdesc->AddRef();
	for(ULONG idx = 0; idx < pmdindexArray.size(); idx++ ) {
		CIndexDescriptor *desc = CIndexDescriptor::Pindexdesc(mp, ptabdescArray->operator[](idx), pmdindexArray[idx]);
		desc->AddRef();
		m_pindexdesc->Append(desc);
	}

	// compute the order spec
	m_pos = GPOS_NEW(mp) COrderSpec(mp);	// no orderspec
	m_pos->AddRef();

	// create a set representation of output columns
	m_pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrOutput);
	// no distribution
	m_pcrsDist = NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::~CLogicalIndexPathGet
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalIndexPathGet::~CLogicalIndexPathGet()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pindexdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pcrsOutput);
	CRefCount::SafeRelease(m_pos);
	CRefCount::SafeRelease(m_pcrsDist);

	GPOS_DELETE(m_pnameAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalIndexPathGet::HashValue() const
{
	ULONG ulHash = CUtils::UlHashColArray(m_pdrgpcrOutput);
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalIndexPathGet::Matches(COperator *) const
{
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalIndexPathGet::PopCopyWithRemappedColumns(CMemoryPool *,
											 UlongToColRefMap *,
											 BOOL )
{
	GPOS_ASSERT(false);
	// CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	// const IMDIndex *pmdindex = md_accessor->RetrieveIndex(m_pindexdesc->MDId());

	// CColRefArray *pdrgpcrOutput = NULL;
	// if (must_exist)
	// {
	// 	pdrgpcrOutput =
	// 		CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	// }
	// else
	// {
	// 	pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
	// 										 colref_mapping, must_exist);
	// }
	// CName *pnameAlias = GPOS_NEW(mp) CName(mp, *m_pnameAlias);

	// m_ptabdesc->AddRef();

	// return GPOS_NEW(mp) CLogicalIndexPathGet(
	// 	mp, m_pindexdesc, m_ptabdesc, m_ulOriginOpId, m_pnameAlias, m_pdrgpcrOutput, path_join_lower_bound, path_join_upper_bound);

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalIndexPathGet::DeriveOutputColumns(CMemoryPool *mp,
									  CExpressionHandle &  // exprhdl
)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pdrgpcrOutput);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::DeriveOuterReferences
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalIndexPathGet::DeriveOuterReferences(CMemoryPool *mp,
										CExpressionHandle &exprhdl)
{
	return PcrsDeriveOuterIndexGet(mp, exprhdl);
}

CKeyCollection *
CLogicalIndexPathGet::DeriveKeyCollection(CMemoryPool *,
									  CExpressionHandle &  // exprhdl
) const
{
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::FInputOrderSensitive
//
//	@doc:
//		Is input order sensitive
//
//---------------------------------------------------------------------------
BOOL
CLogicalIndexPathGet::FInputOrderSensitive() const
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalIndexPathGet::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfIndexPathGet2IndexPathScan);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalIndexPathGet::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   IStatisticsArray *stats_ctxt) const
{
	return CStatisticsUtils::DeriveStatsForIndexPathGet(mp, exprhdl, stats_ctxt);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexPathGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalIndexPathGet::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " ";
	// TODO writeme
	// // index name
	// os << "  Index Name: (";
	// m_pindexdesc->Name().OsPrint(os);
	// // table alias name
	// os << ")";
	// os << ", Table Name: (";
	// m_pnameAlias->OsPrint(os);
	// os << ")";
	// os << ", Columns: [";
	// CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	// os << "]";

	return os;
}

// EOF
