//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalPathJoin.cpp
//
//	@doc:
//		Implementation of inner join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalPathJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "gpopt/operators/CLogicalLeftOuterJoin.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalPathJoin::CLogicalPathJoin
//
//	@doc:
//		ctor
//		Note: 04/09/2009 - ; so far inner join doesn't have any specific
//			members, hence, no need for a separate pattern ctor
//
//---------------------------------------------------------------------------
CLogicalPathJoin::CLogicalPathJoin(CMemoryPool *mp,
										INT path_join_lower_bound,
										INT path_join_upper_bound,
									 CXform::EXformId origin_xform)
	: CLogicalJoin(mp, origin_xform),
	path_join_lower_bound(path_join_lower_bound),
	path_join_upper_bound(path_join_upper_bound)
{
	GPOS_ASSERT(NULL != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalPathJoin::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalPathJoin::DeriveMaxCard(CMemoryPool *,	 // mp
								 CExpressionHandle &exprhdl) const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalPathJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalPathJoin::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfPathJoin2IndexPathApply);

	// (void) xform_set->ExchangeSet(CXform::ExfJoinCommutativity);	// path join not commu and assoc.
	// (void) xform_set->ExchangeSet(CXform::ExfJoinAssociativity);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalPathJoin::FFewerConj
//
//	@doc:
//		Compare two innerJoin group expressions, test whether the first one
//		has less join predicates than the second one. This is used to
//		prioritize innerJoin with less predicates for stats derivation
//
//---------------------------------------------------------------------------
BOOL
CLogicalPathJoin::FFewerConj(CMemoryPool *mp, CGroupExpression *pgexprFst,
							  CGroupExpression *pgexprSnd)
{
	if (NULL == pgexprFst || NULL == pgexprSnd)
	{
		return false;
	}

	if (COperator::EopLogicalPathJoin != pgexprFst->Pop()->Eopid() ||
		COperator::EopLogicalPathJoin != pgexprSnd->Pop()->Eopid())
	{
		return false;
	}

	// third child must be the group for join conditions
	CGroup *pgroupScalarFst = (*pgexprFst)[2];
	CGroup *pgroupScalarSnd = (*pgexprSnd)[2];
	GPOS_ASSERT(pgroupScalarFst->FScalar());
	GPOS_ASSERT(pgroupScalarSnd->FScalar());

	CExpressionArray *pdrgpexprConjFst = CPredicateUtils::PdrgpexprConjuncts(
		mp, pgroupScalarFst->PexprScalarRep());
	CExpressionArray *pdrgpexprConjSnd = CPredicateUtils::PdrgpexprConjuncts(
		mp, pgroupScalarSnd->PexprScalarRep());

	ULONG ulConjFst = pdrgpexprConjFst->Size();
	ULONG ulConjSnd = pdrgpexprConjSnd->Size();

	pdrgpexprConjFst->Release();
	pdrgpexprConjSnd->Release();

	return ulConjFst < ulConjSnd;
}

// EOF
