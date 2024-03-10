//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CLogicalShortestPath.cpp
//
//	@doc:
//		Implementation of logical ShortestPath operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalShortestPath.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::CLogicalShortestPath
//
//	@doc:
//		ctor for xform pattern
//
//---------------------------------------------------------------------------
CLogicalShortestPath::CLogicalShortestPath(CMemoryPool *mp)
	: CLogicalUnary(mp),
	  m_pnameAlias(NULL),
	  m_ptabdescArray(NULL),
	  m_srccr(NULL),
	  m_destcr(NULL)
{

}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::CLogicalShortestPath
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalShortestPath::CLogicalShortestPath(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptorArray *ptabdescArray,
				CColRef *srccr, 
				CColRef *destcr)
	: CLogicalUnary(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdescArray(ptabdescArray),
	  m_srccr(srccr),
	  m_destcr(destcr)
{
	GPOS_ASSERT(NULL != srccr);
	GPOS_ASSERT(NULL != destcr);
	m_pcrsLocalUsed->Include(m_srccr);
	m_pcrsLocalUsed->Include(m_destcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::~CLogicalShortestPath
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalShortestPath::~CLogicalShortestPath()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalShortestPath::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	pcrs->Include(exprhdl.DeriveOutputColumns(0));
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAgg::DeriveOuterReferences
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalShortestPath::DeriveOuterReferences(CMemoryPool *mp,
									 CExpressionHandle &exprhdl)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_srccr);
	pcrs->Include(m_destcr);

	CColRefSet *outer_refs =
		CLogical::DeriveOuterReferences(mp, exprhdl, pcrs);
	pcrs->Release();

	return outer_refs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::DeriveNotNullColumns
//
//	@doc:
//		Derive not null columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalShortestPath::DeriveNotNullColumns(CMemoryPool *mp,
									CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	pcrs->Include(m_srccr);
	pcrs->Include(m_destcr);
	pcrs->Intersection(exprhdl.DeriveNotNullColumns(0));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalShortestPath::DerivePropertyConstraint(CMemoryPool *mp,
										CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_srccr);
	pcrs->Include(m_destcr);

	// get the constraints on the src dst columns only
	CPropConstraint *ppc =
		PpcDeriveConstraintRestrict(mp, exprhdl, pcrs);
	pcrs->Release();

	return ppc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalShortestPath::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
						CColRefSet *pcrsInput, ULONG child_index) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_srccr);
	pcrs->Include(m_destcr);
	pcrs->Union(exprhdl.DeriveUsedColumns(1));
	
	CColRefSet *pcrsRequired =
		PcrsReqdChildStats(mp, exprhdl, pcrsInput, pcrs, child_index);
	pcrs->Release();

	return pcrsRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalShortestPath::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							IStatisticsArray *	// not used
) const
{
	return PstatsDeriveDummy(mp, exprhdl, CStatistics::DefaultRelationRows);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalShortestPath::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalShortestPath *popShortest = reinterpret_cast<CLogicalShortestPath *>(pop);

	return popShortest->PnameAlias() == m_pnameAlias &&
		   CColRef::Equals(m_srccr, popShortest->PcrSource()) &&
		   CColRef::Equals(m_destcr, popShortest->PcrDestination());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::PxfsCandidates
//
//	@doc:
//		Compute candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalShortestPath::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementShortestPath);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalShortestPath::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalShortestPath::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << " Src Col: [";
	m_srccr->OsPrint(os);
	os << "]"
	   << " Dst Col: [";
	m_destcr->OsPrint(os);
	os << "]";
	os << " )";
	return os;
}


// EOF
