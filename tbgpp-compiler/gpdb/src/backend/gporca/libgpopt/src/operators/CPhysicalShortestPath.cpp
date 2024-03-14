//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalShortestPath.cpp
//
//	@doc:
//		Implementation of shortest path operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalShortestPath.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/base/CDistributionSpecAny.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::CPhysicalShortestPath
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalShortestPath::CPhysicalShortestPath(CMemoryPool *mp)
	: CPhysical(mp)
{
	GPOS_ASSERT(NULL != mp);
}
//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::CPhysicalShortestPath
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalShortestPath::CPhysicalShortestPath(CMemoryPool *mp, const CName *pnameAlias,
			CTableDescriptorArray *ptabdescArray,
			CColRef *srccr, 
			CColRef *destcr,
			INT path_lower_bound,
			INT path_upper_bound)
	: CPhysical(mp),
	m_pnameAlias(pnameAlias),
	m_ptabdescArray(ptabdescArray),
	m_srccr(srccr),
	m_destcr(destcr),
	m_path_lower_bound(path_lower_bound),
	m_path_upper_bound(path_upper_bound)
{
	
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::~CPhysicalShortestPath
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalShortestPath::~CPhysicalShortestPath()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalShortestPath::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PcrsRequired
//
//	@doc:
//		Columns required by CPhysicalShortestPath's relational child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalShortestPath::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CColRefSet *pcrsRequired, ULONG child_index,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Union(pcrsRequired);

	CColRefSet *prcsOutput = PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, 1);
	pcrs->Release();

	return prcsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalShortestPath::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 COrderSpec *posRequired, ULONG child_index,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

CDistributionSpec *
CPhysicalShortestPath::PdsRequired(CMemoryPool * mp, CExpressionHandle &,
							CDistributionSpec *, ULONG, CDrvdPropArray *,
							ULONG) const
{
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalShortestPath::PrsRequired(CMemoryPool *mp,
							 CExpressionHandle &,	// exprhdl
							 CRewindabilitySpec *,	// prsRequired
							 ULONG,				// child_index
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
) const
{
	GPOS_ASSERT(NULL != mp);

	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
										   CRewindabilitySpec::EmhtNoMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalShortestPath::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CPartitionPropagationSpec *pppsRequired,
							 ULONG
#ifdef GPOS_DEBUG
								 child_index
#endif
							 ,
							 CDrvdPropArray *,	//pdrgpdpCtxt
							 ULONG				//ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired);

	// limit should not push predicate below it as it will generate wrong results
	// for example, the following two queries are not equivalent.
	// Q1: select * from (select * from foo order by a limit 1) x where x.a = 10
	// Q2: select * from (select * from foo where a = 10 order by a limit 1) x

	return CPhysical::PppsRequiredPushThruUnresolvedUnary(
		mp, exprhdl, pppsRequired, CPhysical::EppcProhibited, NULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalShortestPath::PcteRequired(CMemoryPool *,		   //mp,
							 CExpressionHandle &,  //exprhdl,
							 CCTEReq *pcter,
							 ULONG
#ifdef GPOS_DEBUG
								 child_index
#endif
							 ,
							 CDrvdPropArray *,	//pdrgpdpCtxt,
							 ULONG				//ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalShortestPath::FProvidesReqdCols(CExpressionHandle &exprhdl,
								  CColRefSet *pcrsRequired,
								  ULONG	 // ulOptReq
) const
{
	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));

	BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalShortestPath::PosDerive(CMemoryPool *,  // mp
						   CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalShortestPath::PdsDerive(CMemoryPool *,  // mp
						   CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalShortestPath::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLimit::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalShortestPath::EpetOrder(CExpressionHandle &,	// exprhdl
						  const CEnfdOrder *peo) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalShortestPath::EpetRewindability(CExpressionHandle &,		  // exprhdl
								  const CEnfdRewindability *  // per
) const
{
	// rewindability is preserved on operator's output
	return CEnfdProp::EpetOptional;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalShortestPath::OsPrint
//
//	@doc:
//		Print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalShortestPath::OsPrint(IOstream &os) const
{
	os << SzId() << "( ";
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
