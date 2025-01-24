//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CLogicalAllShortestPath.cpp
//
//	@doc:
//		Implementation of logical AllShortestPath operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalAllShortestPath.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::CLogicalAllShortestPath
//
//	@doc:
//		ctor for xform pattern
//
//---------------------------------------------------------------------------
CLogicalAllShortestPath::CLogicalAllShortestPath(CMemoryPool *mp)
	: CLogicalUnary(mp),
	  m_pnameAlias(NULL),
	  m_ptabdescArray(NULL),
	  m_srccr(NULL),
	  m_destcr(NULL),
	  m_path_lower_bound(0),
	  m_path_upper_bound(-1)
{

}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::CLogicalAllShortestPath
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalAllShortestPath::CLogicalAllShortestPath(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptorArray *ptabdescArray,
				CColRef *srccr, 
				CColRef *destcr,
				INT	lower_bound,
				INT upper_bound)
	: CLogicalUnary(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdescArray(ptabdescArray),
	  m_srccr(srccr),
	  m_destcr(destcr),
	  m_path_lower_bound(lower_bound),
	  m_path_upper_bound(upper_bound)
{
	GPOS_ASSERT(NULL != srccr);
	GPOS_ASSERT(NULL != destcr);
	m_pcrsLocalUsed->Include(m_srccr);
	m_pcrsLocalUsed->Include(m_destcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::~CLogicalAllShortestPath
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CLogicalAllShortestPath::~CLogicalAllShortestPath()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalAllShortestPath::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
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
CLogicalAllShortestPath::DeriveOuterReferences(CMemoryPool *mp,
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
//		CLogicalAllShortestPath::DeriveNotNullColumns
//
//	@doc:
//		Derive not null columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalAllShortestPath::DeriveNotNullColumns(CMemoryPool *mp,
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
//		CLogicalAllShortestPath::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalAllShortestPath::DerivePropertyConstraint(CMemoryPool *mp,
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
//		CLogicalAllShortestPath::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalAllShortestPath::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
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
//		CLogicalAllShortestPath::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalAllShortestPath::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							IStatisticsArray *	// not used
) const
{
	return PstatsDeriveDummy(mp, exprhdl, CStatistics::DefaultRelationRows);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalAllShortestPath::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalAllShortestPath *popShortest = reinterpret_cast<CLogicalAllShortestPath *>(pop);

	return popShortest->PnameAlias() == m_pnameAlias &&
		   CColRef::Equals(m_srccr, popShortest->PcrSource()) &&
		   CColRef::Equals(m_destcr, popShortest->PcrDestination());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::PxfsCandidates
//
//	@doc:
//		Compute candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalAllShortestPath::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementAllShortestPath);
	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalAllShortestPath::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalAllShortestPath::OsPrint(IOstream &os) const
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
