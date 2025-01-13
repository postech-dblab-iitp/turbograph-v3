//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDPCoalescing.cpp
//
//	@doc:
//		Implementation of dynamic programming-based join order generation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrderDPCoalescing.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalUnionAll.h"

using namespace gpopt;

#define GPOPT_DP_JOIN_ORDERING_TOPK 10

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDPCoalescing::SComponentPair::SComponentPair(CBitSet *pbsFst, CBitSet *pbsSnd)
	: m_pbsFst(pbsFst), m_pbsSnd(pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);
	GPOS_ASSERT(pbsFst->IsDisjoint(pbsSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDPCoalescing::SComponentPair::HashValue(const SComponentPair *pcomppair)
{
	GPOS_ASSERT(NULL != pcomppair);

	return CombineHashes(pcomppair->m_pbsFst->HashValue(),
						 pcomppair->m_pbsSnd->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDPCoalescing::SComponentPair::Equals(const SComponentPair *pcomppairFst,
									 const SComponentPair *pcomppairSnd)
{
	GPOS_ASSERT(NULL != pcomppairFst);
	GPOS_ASSERT(NULL != pcomppairSnd);

	return pcomppairFst->m_pbsFst->Equals(pcomppairSnd->m_pbsFst) &&
		   pcomppairFst->m_pbsSnd->Equals(pcomppairSnd->m_pbsSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDPCoalescing::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::CJoinOrderDPCoalescing
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDPCoalescing::CJoinOrderDPCoalescing(CMemoryPool *mp,
						   CExpressionArray *pdrgpexprComponents,
						   CExpressionArray *pdrgpexprConjuncts)
	: CJoinOrder(mp, pdrgpexprComponents, pdrgpexprConjuncts,
				 false /* m_include_loj_childs */)
{
	m_phmcomplink = GPOS_NEW(mp) ComponentPairToExpressionMap(mp);
	m_phmbsexpr = GPOS_NEW(mp) BitSetToExpressionMap(mp);
	m_phmexprcost = GPOS_NEW(mp) ExpressionToCostMap(mp);
	m_pdrgpexprTopKOrders = GPOS_NEW(mp) CExpressionArray(mp);
	m_pexprDummy = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp));

#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		GPOS_ASSERT(NULL != m_rgpcomp[ul]->m_pexpr->Pstats() &&
					"stats were not derived on input component");
	}
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::~CJoinOrderDPCoalescing
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDPCoalescing::~CJoinOrderDPCoalescing()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	m_phmcomplink->Release();
	m_phmbsexpr->Release();
	m_phmexprcost->Release();
	m_pdrgpexprTopKOrders->Release();
	m_pexprDummy->Release();
#endif	// GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::AddJoinOrder
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDPCoalescing::AddJoinOrder(CExpression *pexprJoin, CDouble dCost)
{
	GPOS_ASSERT(NULL != pexprJoin);
	GPOS_ASSERT(NULL != m_pdrgpexprTopKOrders);

	// length of the array will not be more than 10
	INT ulResults = m_pdrgpexprTopKOrders->Size();
	INT iReplacePos = -1;
	BOOL fAddJoinOrder = false;
	if (ulResults < GPOPT_DP_JOIN_ORDERING_TOPK)
	{
		// we have less than K results, always add the given expression
		fAddJoinOrder = true;
	}
	else
	{
		CDouble dmaxCost = 0.0;
		// we have stored K expressions, evict worst expression
		for (INT ul = 0; ul < ulResults; ul++)
		{
			CExpression *pexpr = (*m_pdrgpexprTopKOrders)[ul];
			CDouble *pd = m_phmexprcost->Find(pexpr);
			GPOS_ASSERT(NULL != pd);

			if (dmaxCost < *pd && dCost < *pd)
			{
				// found a worse expression
				dmaxCost = *pd;
				fAddJoinOrder = true;
				iReplacePos = ul;
			}
		}
	}

	if (fAddJoinOrder)
	{
		pexprJoin->AddRef();
		if (iReplacePos > -1)
		{
			m_pdrgpexprTopKOrders->Replace((ULONG) iReplacePos, pexprJoin);
		}
		else
		{
			m_pdrgpexprTopKOrders->Append(pexprJoin);
		}

		InsertExpressionCost(pexprJoin, dCost, false /*fValidateInsert*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprLookup
//
//	@doc:
//		Lookup best join order for given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprLookup(CBitSet *pbs)
{
	// if set has size 1, return expression directly
	if (1 == pbs->Size())
	{
		CBitSetIter bsi(*pbs);
		(void) bsi.Advance();

		return m_rgpcomp[bsi.Bit()]->m_pexpr;
	}

	// otherwise, return expression by looking up DP table
	return m_phmbsexpr->Find(pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	if (!pbsFst->IsDisjoint(pbsSnd) || 0 == pbsFst->Size() ||
		0 == pbsSnd->Size())
	{
		// components must be non-empty and disjoint
		return NULL;
	}

	CExpression *pexprPred = NULL;
	SComponentPair *pcomppair = NULL;

	// lookup link map
	for (ULONG ul = 0; ul < 2; ul++)
	{
		pbsFst->AddRef();
		pbsSnd->AddRef();
		pcomppair = GPOS_NEW(m_mp) SComponentPair(pbsFst, pbsSnd);
		pexprPred = m_phmcomplink->Find(pcomppair);
		if (NULL != pexprPred)
		{
			pcomppair->Release();
			if (m_pexprDummy == pexprPred)
			{
				return NULL;
			}
			return pexprPred;
		}

		// try again after swapping sets
		if (0 == ul)
		{
			pcomppair->Release();
			std::swap(pbsFst, pbsSnd);
		}
	}

	// could not find link in the map, construct it from edge set
	pexprPred = PexprBuildPred(pbsFst, pbsSnd);
	if (NULL == pexprPred)
	{
		m_pexprDummy->AddRef();
		pexprPred = m_pexprDummy;
	}

	// store predicate in link map
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmcomplink->Insert(pcomppair, pexprPred);
	GPOS_ASSERT(fInserted);

	if (m_pexprDummy != pexprPred)
	{
		return pexprPred;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprJoin
//
//	@doc:
//		Join expressions in the given two sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprJoin(CBitSet *pbsFst, CBitSet *pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	CExpression *pexprFst = PexprLookup(pbsFst);
	GPOS_ASSERT(NULL != pexprFst);

	CExpression *pexprSnd = PexprLookup(pbsSnd);
	GPOS_ASSERT(NULL != pexprSnd);

	CExpression *pexprScalar = PexprPred(pbsFst, pbsSnd);
	GPOS_ASSERT(NULL != pexprScalar);

	pexprFst->AddRef();
	pexprSnd->AddRef();
	pexprScalar->AddRef();

	return CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprFst, pexprSnd,
													   pexprScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDPCoalescing::DeriveStats(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy != pexpr && NULL == pexpr->Pstats())
	{
		CExpressionHandle exprhdl(m_mp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveStats(m_mp, m_mp, NULL /*prprel*/, NULL /*stats_ctxt*/);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::InsertExpressionCost
//
//	@doc:
//		Add expression to cost map
//
//---------------------------------------------------------------------------
void
CJoinOrderDPCoalescing::InsertExpressionCost(
	CExpression *pexpr, CDouble dCost,
	BOOL fValidateInsert  // if true, insertion must succeed
)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy == pexpr)
	{
		// ignore dummy expression
		return;
	}

	if (!fValidateInsert && NULL != m_phmexprcost->Find(pexpr))
	{
		// expression already exists in cost map
		return;
	}

	pexpr->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmexprcost->Insert(pexpr, GPOS_NEW(m_mp) CDouble(dCost));
	GPOS_ASSERT(fInserted);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprJoin
//
//	@doc:
//		Join expressions in the given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprJoin(CBitSet *pbs)
{
	GPOS_ASSERT(2 == pbs->Size());

	CBitSetIter bsi(*pbs);
	(void) bsi.Advance();
	ULONG ulCompFst = bsi.Bit();
	(void) bsi.Advance();
	ULONG ulCompSnd = bsi.Bit();
	GPOS_ASSERT(!bsi.Advance());

	CBitSet *pbsFst = GPOS_NEW(m_mp) CBitSet(m_mp);
	(void) pbsFst->ExchangeSet(ulCompFst);
	CBitSet *pbsSnd = GPOS_NEW(m_mp) CBitSet(m_mp);
	(void) pbsSnd->ExchangeSet(ulCompSnd);
	CExpression *pexprScalar = PexprPred(pbsFst, pbsSnd);
	pbsFst->Release();
	pbsSnd->Release();

	if (NULL == pexprScalar)
	{
		return NULL;
	}

	CExpression *pexprLeft = m_rgpcomp[ulCompFst]->m_pexpr;
	CExpression *pexprRight = m_rgpcomp[ulCompSnd]->m_pexpr;
	pexprLeft->AddRef();
	pexprRight->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		m_mp, pexprLeft, pexprRight, pexprScalar);

	DeriveStats(pexprJoin);
	// store solution in DP table
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmbsexpr->Insert(pbs, pexprJoin);
	GPOS_ASSERT(fInserted);

	return pexprJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprBestJoinOrderDP
//
//	@doc:
//		Find the best join order of a given set of elements using dynamic
//		programming;
//		given a set of elements (e.g., {A, B, C}), we find all possible splits
//		of the set (e.g., {A}, {B, C}) where at least one edge connects the
//		two subsets resulting from the split,
//		for each split, we find the best join orders of left and right subsets
//		recursively,
//		the function finds the split with the least cost, and stores the join
//		of its two subsets as the best join order of the given set
//
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprBestJoinOrderDP(CBitSet *pbs	 // set of elements to be joined
)
{
	CDouble dMinCost(0.0);
	CExpression *pexprResult = NULL;

	CBitSetArray *pdrgpbsSubsets = PdrgpbsSubsets(m_mp, pbs);
	const ULONG ulSubsets = pdrgpbsSubsets->Size();
	for (ULONG ul = 0; ul < ulSubsets; ul++)
	{
		CBitSet *pbsCurrent = (*pdrgpbsSubsets)[ul];
		CBitSet *pbsRemaining = GPOS_NEW(m_mp) CBitSet(m_mp, *pbs);
		pbsRemaining->Difference(pbsCurrent);

		// check if subsets are connected with one or more edges
		CExpression *pexprPred = PexprPred(pbsCurrent, pbsRemaining);
		if (NULL != pexprPred)
		{
			// compute solutions of left and right subsets recursively
			CExpression *pexprLeft = PexprBestJoinOrder(pbsCurrent);
			CExpression *pexprRight = PexprBestJoinOrder(pbsRemaining);

			if (NULL != pexprLeft && NULL != pexprRight)
			{
				// we found solutions of left and right subsets, we check if
				// this gives a better solution for the input set
				CExpression *pexprJoin = PexprJoin(pbsCurrent, pbsRemaining);
				CDouble dCost = DCost(pexprJoin);

				if (NULL == pexprResult || dCost < dMinCost)
				{
					// this is the first solution, or we found a better solution
					dMinCost = dCost;
					CRefCount::SafeRelease(pexprResult);
					pexprJoin->AddRef();
					pexprResult = pexprJoin;
				}

				if (m_ulComps == pbs->Size())
				{
					AddJoinOrder(pexprJoin, dCost);
				}

				pexprJoin->Release();
			}
		}
		pbsRemaining->Release();
	}
	pdrgpbsSubsets->Release();

	// store solution in DP table
	if (NULL == pexprResult)
	{
		m_pexprDummy->AddRef();
		pexprResult = m_pexprDummy;
	}

	DeriveStats(pexprResult);
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmbsexpr->Insert(pbs, pexprResult);
	GPOS_ASSERT(fInserted);

	// add expression cost to cost map
	InsertExpressionCost(pexprResult, dMinCost, false /*fValidateInsert*/);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::GenerateSubsets
//
//	@doc:
//		Generate all subsets of given array of elements
//
//---------------------------------------------------------------------------
void
CJoinOrderDPCoalescing::GenerateSubsets(CMemoryPool *mp, CBitSet *pbsCurrent,
							  ULONG *pulElems, ULONG size, ULONG ulIndex,
							  CBitSetArray *pdrgpbsSubsets)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(ulIndex <= size);
	GPOS_ASSERT(NULL != pbsCurrent);
	GPOS_ASSERT(NULL != pulElems);
	GPOS_ASSERT(NULL != pdrgpbsSubsets);

	if (ulIndex == size)
	{
		pdrgpbsSubsets->Append(pbsCurrent);
		return;
	}

	CBitSet *pbsCopy = GPOS_NEW(mp) CBitSet(mp, *pbsCurrent);
#ifdef GPOS_DEBUG
	BOOL fSet =
#endif	// GPOS_DEBUG
		pbsCopy->ExchangeSet(pulElems[ulIndex]);
	GPOS_ASSERT(!fSet);

	GenerateSubsets(mp, pbsCopy, pulElems, size, ulIndex + 1, pdrgpbsSubsets);
	GenerateSubsets(mp, pbsCurrent, pulElems, size, ulIndex + 1,
					pdrgpbsSubsets);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PdrgpbsSubsets
//
//	@doc:
//		 Driver of subset generation
//
//---------------------------------------------------------------------------
CBitSetArray *
CJoinOrderDPCoalescing::PdrgpbsSubsets(CMemoryPool *mp, CBitSet *pbs)
{
	const ULONG size = pbs->Size();
	ULONG *pulElems = GPOS_NEW_ARRAY(mp, ULONG, size);
	ULONG ul = 0;
	CBitSetIter bsi(*pbs);
	while (bsi.Advance())
	{
		pulElems[ul++] = bsi.Bit();
	}

	CBitSet *pbsCurrent = GPOS_NEW(mp) CBitSet(mp);
	CBitSetArray *pdrgpbsSubsets = GPOS_NEW(mp) CBitSetArray(mp);
	GenerateSubsets(mp, pbsCurrent, pulElems, size, 0, pdrgpbsSubsets);
	GPOS_DELETE_ARRAY(pulElems);

	return pdrgpbsSubsets;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDPCoalescing::DCost(CExpression *pexpr)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	CDouble *pd = m_phmexprcost->Find(pexpr);
	if (NULL != pd)
	{
		// stop recursion if cost was already cashed
		return *pd;
	}

	CDouble dCost(0.0);
	const ULONG arity = pexpr->Arity();
	if (0 == arity)
	{
		if (NULL == pexpr->Pstats())
		{
			GPOS_RAISE(
				CException::ExmaInvalid, CException::ExmiAssert,
				GPOS_WSZ_LIT("stats were not derived on an input component"));
		}

		// leaf operator, use its estimated number of rows as cost
		// dCost = CDouble(pexpr->Pstats()->Rows());
		dCost = 0.0;
	}
	else
	{
		// inner join operator, sum-up cost of its children
		DOUBLE rgdRows[2] = {0.0, 0.0};
		for (ULONG ul = 0; ul < arity - 1; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];

			// call function recursively to find child cost
			dCost = dCost + DCost(pexprChild);
			DeriveStats(pexprChild);
			rgdRows[ul] = pexprChild->Pstats()->Rows().Get();
		}

		// add inner join local cost
		dCost = dCost + (rgdRows[0] + rgdRows[1]);
	}

	return dCost;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PbsCovered
//
//	@doc:
//		Return a subset of the given set covered by one or more edges
//
//---------------------------------------------------------------------------
CBitSet *
CJoinOrderDPCoalescing::PbsCovered(CBitSet *pbsInput)
{
	GPOS_ASSERT(NULL != pbsInput);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pbsInput->ContainsAll(pedge->m_pbs))
		{
			pbs->Union(pedge->m_pbs);
		}
	}

	return pbs;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprCross
//
//	@doc:
//		Generate cross product for the given components
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprCross(CBitSet *pbs)
{
	GPOS_ASSERT(NULL != pbs);

	CExpression *pexpr = PexprLookup(pbs);
	if (NULL != pexpr)
	{
		// join order is already created
		return pexpr;
	}

	CBitSetIter bsi(*pbs);
	(void) bsi.Advance();
	CExpression *pexprComp = m_rgpcomp[bsi.Bit()]->m_pexpr;
	pexprComp->AddRef();
	CExpression *pexprCross = pexprComp;
	while (bsi.Advance())
	{
		pexprComp = m_rgpcomp[bsi.Bit()]->m_pexpr;
		pexprComp->AddRef();
		pexprCross = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
			m_mp, pexprComp, pexprCross,
			CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/));
	}

	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmbsexpr->Insert(pbs, pexprCross);
	GPOS_ASSERT(fInserted);

	return pexprCross;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprJoinCoveredSubsetWithUncoveredSubset
//
//	@doc:
//		Join a covered subset with uncovered subset
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprJoinCoveredSubsetWithUncoveredSubset(CBitSet *pbs,
														CBitSet *pbsCovered,
														CBitSet *pbsUncovered)
{
	GPOS_ASSERT(NULL != pbs);
	GPOS_ASSERT(NULL != pbsCovered);
	GPOS_ASSERT(NULL != pbsUncovered);
	GPOS_ASSERT(pbsCovered->IsDisjoint(pbsUncovered));
	GPOS_ASSERT(pbs->ContainsAll(pbsCovered));
	GPOS_ASSERT(pbs->ContainsAll(pbsUncovered));

	// find best join order for covered subset
	CExpression *pexprJoin = PexprBestJoinOrder(pbsCovered);
	if (NULL == pexprJoin)
	{
		return NULL;
	}

	// create a cross product for uncovered subset
	CExpression *pexprCross = PexprCross(pbsUncovered);

	// join the results with a cross product
	pexprJoin->AddRef();
	pexprCross->AddRef();
	CExpression *pexprResult = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		m_mp, pexprJoin, pexprCross,
		CPredicateUtils::PexprConjunction(m_mp, NULL));
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmbsexpr->Insert(pbs, pexprResult);
	GPOS_ASSERT(fInserted);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprBestJoinOrder
//
//	@doc:
//		find best join order for a given set of elements;
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprBestJoinOrder(CBitSet *pbs)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(NULL != pbs);

	// start by looking-up cost in the DP map
	CExpression *pexpr = PexprLookup(pbs);

	if (pexpr == m_pexprDummy)
	{
		// no join order could be created
		return NULL;
	}

	if (NULL != pexpr)
	{
		// join order is found by looking up map
		return pexpr;
	}

	// find maximal covered subset
	CBitSet *pbsCovered = PbsCovered(pbs);
	if (0 == pbsCovered->Size())
	{
		// set is not covered, return a cross product
		pbsCovered->Release();

		return PexprCross(pbs);
	}

	if (!pbsCovered->Equals(pbs))
	{
		// create a cross product for uncovered subset
		CBitSet *pbsUncovered = GPOS_NEW(m_mp) CBitSet(m_mp, *pbs);
		pbsUncovered->Difference(pbsCovered);
		CExpression *pexprResult = PexprJoinCoveredSubsetWithUncoveredSubset(
			pbs, pbsCovered, pbsUncovered);
		pbsCovered->Release();
		pbsUncovered->Release();

		return pexprResult;
	}
	pbsCovered->Release();

	// if set has size 2, there is only one possible solution
	if (2 == pbs->Size())
	{
		return PexprJoin(pbs);
	}

	// otherwise, compute best join order using dynamic programming
	CExpression *pexprBestJoinOrder = PexprBestJoinOrderDP(pbs);
	if (pexprBestJoinOrder == m_pexprDummy)
	{
		// no join order could be created
		return NULL;
	}

	return pexprBestJoinOrder;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd)
{
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pbs->ContainsAll(pedge->m_pbs) &&
			!pbsFst->IsDisjoint(pedge->m_pbs) &&
			!pbsSnd->IsDisjoint(pedge->m_pbs))
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				pbsEdges->ExchangeSet(ul);
			GPOS_ASSERT(!fSet);
		}
	}
	pbs->Release();

	CExpression *pexprPred = NULL;
	if (0 < pbsEdges->Size())
	{
		CExpressionArray *pdrgpexpr = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CBitSetIter bsi(*pbsEdges);
		while (bsi.Advance())
		{
			ULONG ul = bsi.Bit();
			SEdge *pedge = m_rgpedge[ul];
			pedge->m_pexpr->AddRef();
			pdrgpexpr->Append(pedge->m_pexpr);
		}

		pexprPred = CPredicateUtils::PexprConjunction(m_mp, pdrgpexpr);
	}

	pbsEdges->Release();
	return pexprPred;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::lCreateTableDescForRel
//
//	@doc:
//		Create table descriptor for relation
//
//---------------------------------------------------------------------------
CTableDescriptor *CJoinOrderDPCoalescing::CreateTableDescForVirtualTable(
    CTableDescriptor *ptabdesc, IMdIdArray *pdrgmdid)
{
	CTableDescriptor *new_ptabdesc;
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	if (pdrgmdid->Size() > 1) {
		// TODO add temporal catalog for virtual tables	
		IMDId *mdid = mda->AddVirtualTable(ptabdesc->MDId(), pdrgmdid);

		new_ptabdesc = GPOS_NEW(m_mp) CTableDescriptor(
			m_mp,
			mdid,
			ptabdesc->Name(), // TODO name change?
			ptabdesc->ConvertHashToRandom(),
			IMDRelation::EreldistrMasterOnly,
			IMDRelation::ErelstorageHeap,
			0
		);

		new_ptabdesc->SetInstanceDescriptor(true);
		for (ULONG ul = 0; ul < pdrgmdid->Size(); ul++) {
			new_ptabdesc->AddTableInTheGroup((*pdrgmdid)[ul]);
		}

		// Get column descriptors from original table descriptor
		CColumnDescriptorArray *org_pdrgpcoldesc = ptabdesc->OrgPdrgpcoldesc();
		CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
		GPOS_ASSERT(NULL != org_pdrgpcoldesc);
		GPOS_ASSERT(NULL != pdrgpcoldesc);

		// Add each column descriptor to the new table descriptor
		for (ULONG ul = 0; ul < org_pdrgpcoldesc->Size(); ul++)
		{
			CColumnDescriptor *pcoldesc = (*org_pdrgpcoldesc)[ul];
			pcoldesc->AddRef();
			new_ptabdesc->AddColumn(pcoldesc);
		}

		new_ptabdesc->SetPdrgpcoldesc(pdrgpcoldesc);
	} else {
		GPOS_ASSERT(pdrgmdid->Size() == 1);

		// don't need to create new table descriptor. get the existing table ID
		IMDId *mdid = (*pdrgmdid)[0];
		new_ptabdesc = GPOS_NEW(m_mp) CTableDescriptor(
			m_mp,
			mdid,
			ptabdesc->Name(), // TODO name change?
			ptabdesc->ConvertHashToRandom(),
			IMDRelation::EreldistrMasterOnly,
			IMDRelation::ErelstorageHeap,
			0
		);

		// Get column descriptors from original table descriptor
		CColumnDescriptorArray *org_pdrgpcoldesc = ptabdesc->OrgPdrgpcoldesc();
		CColumnDescriptorArray *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
		GPOS_ASSERT(NULL != org_pdrgpcoldesc);
		GPOS_ASSERT(NULL != pdrgpcoldesc);

		// Add each column descriptor to the new table descriptor
		for (ULONG ul = 0; ul < org_pdrgpcoldesc->Size(); ul++)
		{
			CColumnDescriptor *pcoldesc = (*org_pdrgpcoldesc)[ul];
			pcoldesc->AddRef();
			new_ptabdesc->AddColumn(pcoldesc);
		}

		new_ptabdesc->SetPdrgpcoldesc(pdrgpcoldesc);
	}

	new_ptabdesc->AddRef();
	return new_ptabdesc;
}

void CJoinOrderDPCoalescing::SplitUnionAll(CExpression *pexpr, ULONG ulTarget,
                                           SComponent **splitted_components,
                                           BOOL is_first_time)
{
    // TODO: update m_rgpcomp, ...
    GPOS_ASSERT(NULL != pexpr);

	// If first time, initialize split state
    if (is_first_time) {
        m_ulSplitIndex = 0;
    }
    else {
        // Move to next split configuration
        m_ulSplitIndex++;
    }

    CExpression *scan_expr = FindLogicalGetExpr(pexpr);
	GPOS_ASSERT(scan_expr != NULL);

	// Get table descriptor from LogicalGet
	CTableDescriptor *ptabdesc = CLogicalGet::PopConvert(scan_expr->Pop())->Ptabdesc();

	// Get table ids from table descriptor
	IMdIdArray *pimdidarray = ptabdesc->GetTableIdsInGroup();
	
	// Get number of tables in the virtual table
	ULONG ulTables = pimdidarray->Size();
	GPOS_ASSERT(ulTables > 1);
	
	// Create arrays to hold table descriptors for each group
	IMdIdArray *pdrgmdidFirst = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	IMdIdArray *pdrgmdidSecond = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	
	// Split table IDs into two groups
	SplitGraphlets(pimdidarray, ulTables, pdrgmdidFirst, pdrgmdidSecond, m_ulSplitIndex);
	
	// Create new virtual tables for each group
    CTableDescriptor *ptabdescFirst =
        CreateTableDescForVirtualTable(ptabdesc, pdrgmdidFirst);
    
    CTableDescriptor *ptabdescSecond =
        CreateTableDescForVirtualTable(ptabdesc, pdrgmdidSecond);
	
    // Get output columns from original LogicalGet
    CLogicalGet *plogicalget = CLogicalGet::PopConvert(scan_expr->Pop());
    CColRefArray *pdrgpcrOutput = plogicalget->PdrgpcrOutput();

    std::wstring w_alias1 = L"VirtualTable1";
	std::wstring w_alias2 = L"VirtualTable2";
	CWStringConst strAlias1(w_alias1.c_str());
	CWStringConst strAlias2(w_alias2.c_str());

	pdrgpcrOutput->AddRef();
    CExpression *pexprFirstVirtual = GPOS_NEW(m_mp) CExpression(
        m_mp, GPOS_NEW(m_mp) CLogicalGet(
                  m_mp, GPOS_NEW(m_mp) CName(m_mp, CName(&strAlias1)),
                  ptabdescFirst, pdrgpcrOutput));

	pdrgpcrOutput->AddRef();
    CExpression *pexprSecondVirtual = GPOS_NEW(m_mp) CExpression(
        m_mp, GPOS_NEW(m_mp) CLogicalGet(
                  m_mp, GPOS_NEW(m_mp) CName(m_mp, CName(&strAlias2)),
                  ptabdescSecond, pdrgpcrOutput));

    // Create new expressions by replacing the original LogicalGet with the new virtual table LogicalGets
    CExpression *pexprFirstComponent =
        PexprCopyWithNewChildren(m_mp, pexpr, scan_expr, pexprFirstVirtual);
    CExpression *pexprSecondComponent =
        PexprCopyWithNewChildren(m_mp, pexpr, scan_expr, pexprSecondVirtual);

	
    splitted_components[0] = GPOS_NEW(m_mp) SComponent(m_mp, pexprFirstComponent);
    splitted_components[1] =
        GPOS_NEW(m_mp) SComponent(m_mp, pexprSecondComponent);

	splitted_components[0]->m_pbs = m_rgpcomp[ulTarget]->m_pbs;
	splitted_components[0]->m_edge_set = m_rgpcomp[ulTarget]->m_edge_set;
	splitted_components[1]->m_pbs = m_rgpcomp[ulTarget]->m_pbs;
	splitted_components[1]->m_edge_set = m_rgpcomp[ulTarget]->m_edge_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprCopyWithNewChildren
//
//	@doc:
//		Copy expression tree and replace scan_expr with new_expr
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderDPCoalescing::PexprCopyWithNewChildren(
    CMemoryPool *mp, CExpression *pexpr, CExpression *target_expr,
    CExpression *new_expr)
{
    GPOS_ASSERT(NULL != pexpr);
    GPOS_ASSERT(NULL != target_expr);
    GPOS_ASSERT(NULL != new_expr);

    // If current expression matches scan_expr, return new_expr
    if (pexpr == target_expr) {
        new_expr->AddRef();
        return new_expr;
    }

    // Copy operator
    COperator *pop = pexpr->Pop();
    pop->AddRef();

    // Recursively process children
    CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
    const ULONG ulArity = pexpr->Arity();

    for (ULONG ul = 0; ul < ulArity; ul++) {
        CExpression *pexprChild = (*pexpr)[ul];
        CExpression *pexprNewChild =
            PexprCopyWithNewChildren(mp, pexprChild, target_expr, new_expr);
        pdrgpexpr->Append(pexprNewChild);
    }

    return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::SplitGraphlets
//
//	@doc:
//		Split graphlets into two groups
//
//---------------------------------------------------------------------------
void CJoinOrderDPCoalescing::SplitGraphlets(IMdIdArray *pimdidarray,
                                            ULONG ulTables,
                                            IMdIdArray *pdrgmdidFirst,
                                            IMdIdArray *pdrgmdidSecond,
											ULONG ulSplitIndex)
{
    // Split tables into two groups // TODO change split method
    // ULONG ulFirstGroupSize = ulTables / 2;
	ULONG ulFirstGroupSize = 2;

    // Split tables between the two groups
    for (ULONG ul = ulSplitIndex; ul < ulTables + ulSplitIndex; ul++) {
		ULONG ulIndex = ul % ulTables;
        IMDId *pmdid = (*pimdidarray)[ulIndex];
        pmdid->AddRef();

        if (ul - ulSplitIndex < ulFirstGroupSize) {
            pdrgmdidFirst->Append(pmdid);
        }
        else {
            pdrgmdidSecond->Append(pmdid);
        }
    }
}

CExpression *CJoinOrderDPCoalescing::ProcessUnionAllComponents(CDouble &dCost)
{
    CExpression *pexprResultUnionAll = NULL;

    // Iterate through components
    for (ULONG ul = 0; ul < m_ulComps; ul++) {
        // Get component
        SComponent *pcomp = m_rgpcomp[ul];
        if (NULL == pcomp) {
            continue;
        }

		// CWStringDynamic str(m_mp, L"\n");
		// COstreamString oss(&str);
		// pcomp->m_pexpr->OsPrint(oss);
		// GPOS_TRACE(str.GetBuffer());

		CExpression *pexpr = FindLogicalGetExpr(pcomp->m_pexpr);
		if (pexpr == NULL) {
			continue;
		}

        // Get table descriptor
        CTableDescriptor *ptabdesc =
            CLogicalGet::PopConvert(pexpr->Pop())->Ptabdesc();
        if (NULL == ptabdesc) {
            continue;
        }

        // Check if table descriptor is instance descriptor
        if (!ptabdesc->IsInstanceDescriptor()) {
            continue;
        }
        
		CExpression *pexprGOO = 
			BuildQueryGraphAndRunGOO(pcomp->m_pexpr, ul, dCost);
		
		if (pexprGOO == NULL) {
			continue;
		}

		CDouble dCostGOO = DCost(pexprGOO);

		// Update best plan if cost is lower
		if (dCostGOO < dCost) {
			if (pexprResultUnionAll != NULL) {
				pexprResultUnionAll->Release();
			}
			pexprResultUnionAll = pexprGOO;
			dCost = dCostGOO;
		}
		else {
			pexprGOO->Release();
		}
    }

	// if (pexprResultUnionAll != NULL) {
	// 	CWStringDynamic str(m_mp, L"\n");
	// 	COstreamString oss(&str);
	// 	pexprResultUnionAll->OsPrint(oss);
	// 	GPOS_TRACE(str.GetBuffer());
	// }
	
	return pexprResultUnionAll;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::PexprExpand()
{
	// Calculate edge selectivity
	CalcEdgeSelectivity(&m_pdrgdSelectivity);

	// Run GOO for original query graph
    CExpression *pexprResult =
        RunGOO(m_ulComps, m_rgpcomp, m_ulEdges, m_rgpedge, m_pdrgdSelectivity);

    // compute cost for pexprResult
	CDouble dCost = DCost(pexprResult);

    // Split one by one
    CExpression *pexprResultUnionAll =
        ProcessUnionAllComponents(dCost);

    if (NULL != pexprResultUnionAll)
	{
		pexprResultUnionAll->AddRef();
		return pexprResultUnionAll;
	} else {
		return pexprResult;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::CalcEdgeSelectivity
//
//	@doc:
//		Calculate selectivity for an edge
//
//---------------------------------------------------------------------------
void
CJoinOrderDPCoalescing::CalcEdgeSelectivity(CDoubleArray **pdrgdSelectivity)
{
	*pdrgdSelectivity = GPOS_NEW(m_mp) CDoubleArray(m_mp);

	// for (ULONG ulComp = 0; ulComp < m_ulComps; ulComp++) {
	// 	SComponent *pcomp = m_rgpcomp[ulComp];
	// 	CWStringDynamic strComp(m_mp, L"\n");
	// 	COstreamString ossComp(&strComp);
	// 	pcomp->m_pexpr->OsPrint(ossComp);
	// 	GPOS_TRACE(strComp.GetBuffer());
	// }

	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		SEdge *pedge = m_rgpedge[ulEdge];

		// Get components connected by this edge
		CBitSetIter bsi(*pedge->m_pbs);
		(void) bsi.Advance();
		ULONG ulFirst = bsi.Bit();
		(void) bsi.Advance();
		ULONG ulSecond = bsi.Bit();

		SComponent *comp1 = m_rgpcomp[ulFirst];
		SComponent *comp2 = m_rgpcomp[ulSecond];

		GPOS_ASSERT(IsValidJoinCombination(comp1, comp2));

		CJoinOrder::SComponent *compTemp = PcompCombine(comp1, comp2);

		GPOS_ASSERT(!CUtils::FCrossJoin(compTemp->m_pexpr));
		
		DeriveStats(compTemp->m_pexpr);
		CDouble dRows = compTemp->m_pexpr->Pstats()->Rows();

		// Get row counts for components
		CDouble dRowsFirst = m_rgpcomp[ulFirst]->m_pexpr->Pstats()->Rows();
		CDouble dRowsSecond = m_rgpcomp[ulSecond]->m_pexpr->Pstats()->Rows();

		// Get predicate selectivity from stats
		CDouble dSelectivity = dRows / (dRowsFirst * dRowsSecond);
		
		// Clamp selectivity to valid range [0,1]
		dSelectivity = std::min(CDouble(1.0), 
									std::max(CDouble(0.0), dSelectivity));

		(*pdrgdSelectivity)->Append(GPOS_NEW(m_mp) CDouble(dSelectivity));
	}
}

void CJoinOrderDPCoalescing::UpdateEdgeSelectivity(
    ULONG ulTarget, CDoubleArray *pdrgdSelectivity, ULONG ul,
    SComponent **splitted_components)
{
    if (pdrgdSelectivity == NULL) {
		pdrgdSelectivity = GPOS_NEW(m_mp) CDoubleArray(m_mp);
	}
	
	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		SEdge *pedge = m_rgpedge[ulEdge];

		// Get components connected by this edge
		CBitSetIter bsi(*pedge->m_pbs);
		(void) bsi.Advance();
		ULONG ulFirst = bsi.Bit();
		(void) bsi.Advance();
		ULONG ulSecond = bsi.Bit();

		if (ulFirst != ulTarget && ulSecond != ulTarget) {
			continue;
		}

		SComponent *comp1, *comp2;
		if (ulFirst == ulTarget) {
			comp1 = splitted_components[ul];
			comp2 = m_rgpcomp[ulSecond];
		}
		else {
			comp1 = m_rgpcomp[ulFirst];
			comp2 = splitted_components[ul];
		}

		GPOS_ASSERT(IsValidJoinCombination(comp1, comp2));

		CJoinOrder::SComponent *compTemp = PcompCombine(comp1, comp2);

		// CWStringDynamic str(m_mp, L"\n");
		// COstreamString oss(&str);
		// compTemp->m_pexpr->OsPrint(oss);
		// GPOS_TRACE(str.GetBuffer());

		GPOS_ASSERT(!CUtils::FCrossJoin(compTemp->m_pexpr));
		
		DeriveStats(compTemp->m_pexpr);
		CDouble dRows = compTemp->m_pexpr->Pstats()->Rows();

		// Get row counts for components
		CDouble dRowsFirst = comp1->m_pexpr->Pstats()->Rows();
		CDouble dRowsSecond = comp2->m_pexpr->Pstats()->Rows();

		// Get predicate selectivity from stats
		CDouble dSelectivity = dRows / (dRowsFirst * dRowsSecond);
		
		// Clamp selectivity to valid range [0,1]
		dSelectivity = std::min(CDouble(1.0), 
									std::max(CDouble(0.0), dSelectivity));

		pdrgdSelectivity->Replace(ulEdge, GPOS_NEW(m_mp) CDouble(dSelectivity));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::Find
//
//	@doc:
//		Find set representative with path compression
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDPCoalescing::Find(ULONG *parent, ULONG x)
{
	if (parent[x] != x)
	{
		parent[x] = Find(parent, parent[x]); // Path compression
	}
	return parent[x];
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::Union
//
//	@doc:
//		Union two sets by rank
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDPCoalescing::Union(ULONG *parent, ULONG *rank, ULONG x, ULONG y)
{
	ULONG px = Find(parent, x);
	ULONG py = Find(parent, y);
	ULONG root;

	if (rank[px] < rank[py])
	{
		parent[px] = py;
		root = py;
	}
	else if (rank[px] > rank[py])
	{
		parent[py] = px;
		root = px;
	}
	else
	{
		parent[py] = px;
		rank[px]++;
		root = px;
	}

	return root;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::GOO
//
//	@doc:
//		Greedy Operator Ordering (GOO) algorithm implementation using UnionFind
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderDPCoalescing::RunGOO(ULONG ulComps, SComponent **rgpcomp,
                                            ULONG ulEdges, SEdge **rgpedge,
                                            CDoubleArray *pdrgdSelectivity,
											SComponent **splitted_components,
											ULONG ulTarget, ULONG ulSplit)
{
    // Initialize arrays for GOO algorithm
	CDoubleArray *size = GPOS_NEW(m_mp) CDoubleArray(m_mp);
	CExpressionArray *tree = GPOS_NEW(m_mp) CExpressionArray(m_mp);
	
	// Initialize UnionFind data structure
	ULONG *parent = GPOS_NEW_ARRAY(m_mp, ULONG, ulComps);
	ULONG *rank = GPOS_NEW_ARRAY(m_mp, ULONG, ulComps);
	
	// Initialize sizes, trees and UnionFind sets
	if (splitted_components != NULL) {
		for (ULONG ul = 0; ul < ulComps; ul++) {
			SComponent *comp;
			if (ul == ulTarget) {
				comp = splitted_components[ulSplit];
			} else {
				comp = rgpcomp[ul];
			}

			CDouble rows = comp->m_pexpr->Pstats()->Rows();
			size->Append(GPOS_NEW(m_mp) CDouble(rows.Get()));
			comp->m_pexpr->AddRef();
			tree->Append(comp->m_pexpr);

			// CWStringDynamic strExpr(m_mp, L"\n");
			// COstreamString ossExpr(&strExpr);
			// comp->m_pexpr->OsPrint(ossExpr);
			// GPOS_TRACE(strExpr.GetBuffer());

			// Initialize each element as its own set
			parent[ul] = ul;
			rank[ul] = 0;
		}
	} else {
		for (ULONG ul = 0; ul < ulComps; ul++)
		{
			SComponent *comp = rgpcomp[ul];

			CDouble rows = comp->m_pexpr->Pstats()->Rows();
			size->Append(GPOS_NEW(m_mp) CDouble(rows.Get()));
			comp->m_pexpr->AddRef();
			tree->Append(comp->m_pexpr);
			
			// Initialize each element as its own set
			parent[ul] = ul;
			rank[ul] = 0;
		}
	}

    // Main GOO loop
    ULONG numSets = ulComps;
    while (numSets > 1) {
        // Find minimum cost join pair
        CDouble dMinCost(0.0);
        ULONG ulMinI = 0;
        ULONG ulMinJ = 0;
        BOOL fFirst = true;

        // Iterate through all edges to find minimum cost join
        for (ULONG ulEdge = 0; ulEdge < ulEdges; ulEdge++) {
            SEdge *pedge = rgpedge[ulEdge];
            // Get components connected by this edge
            CBitSetIter bsi(*pedge->m_pbs);
            (void)bsi.Advance();
            ULONG ulFirst = bsi.Bit();
            (void)bsi.Advance();
            ULONG ulSecond = bsi.Bit();

            // Skip if vertices already in same set
            ULONG ulFirstRoot = Find(parent, ulFirst);
            ULONG ulSecondRoot = Find(parent, ulSecond);
            if (ulFirstRoot == ulSecondRoot) {
                continue;
            }

            // Use pre-calculated edge selectivity
            CDouble selectivity = (*pdrgdSelectivity)[ulEdge]->Get();

            // Calculate join size
            CDouble dCost = (*size)[ulFirstRoot]->Get() *
                            (*size)[ulSecondRoot]->Get() * selectivity.Get();

            if (fFirst || dCost < dMinCost) {
                dMinCost = dCost;
				if ((*size)[ulFirstRoot]->Get() > (*size)[ulSecondRoot]->Get()) {
					ulMinI = ulFirstRoot;
					ulMinJ = ulSecondRoot;
				} else {
					ulMinI = ulSecondRoot;
					ulMinJ = ulFirstRoot;
				}
                fFirst = false;
            }
        }

        // Create join between selected tables
		CBitSet *pbsFirst = GPOS_NEW(m_mp) CBitSet(m_mp);
		CBitSet *pbsSecond = GPOS_NEW(m_mp) CBitSet(m_mp);
		
		// Add all elements that have ulMinI as root to pbsFirst
		for (ULONG ul = 0; ul < ulComps; ul++) {
			if (Find(parent, ul) == ulMinI) {
				(void) pbsFirst->ExchangeSet(ul);
			}
		}
		
		// Add all elements that have ulMinJ as root to pbsSecond  
		for (ULONG ul = 0; ul < ulComps; ul++) {
			if (Find(parent, ul) == ulMinJ) {
				(void) pbsSecond->ExchangeSet(ul);
			}
		}
		
		CExpression *pexprPred = PexprPred(pbsFirst, pbsSecond);
		pbsFirst->Release();
		pbsSecond->Release();
		if (NULL == pexprPred)
		{
			pexprPred = CUtils::PexprScalarConstBool(m_mp, true /*value*/);
		}

		// Get expressions from tree array and increase their reference counts
		// CExpression *pexprLeft = (*tree)[ulMinI];
		// CExpression *pexprRight = (*tree)[ulMinJ]; 
		CExpression *pexprLeft = (*tree)[ulMinJ];
		CExpression *pexprRight = (*tree)[ulMinI]; 
		pexprLeft->AddRef();
		pexprRight->AddRef();

		// Create join expression using the referenced expressions
		CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
			m_mp, pexprLeft, pexprRight, pexprPred);

		// CWStringDynamic strLeft(m_mp, L"\n");
		// COstreamString ossLeft(&strLeft);
		// pexprLeft->OsPrint(ossLeft);
		// GPOS_TRACE(strLeft.GetBuffer());

		// CWStringDynamic strRight(m_mp, L"\n");
		// COstreamString ossRight(&strRight);
		// pexprRight->OsPrint(ossRight);
		// GPOS_TRACE(strRight.GetBuffer());

		// CWStringDynamic strJoin(m_mp, L"\n");
		// COstreamString ossJoin(&strJoin);
		// pexprJoin->OsPrint(ossJoin);
		// GPOS_TRACE(strJoin.GetBuffer());
		
		// Union the sets
		ULONG new_root = Union(parent, rank, ulMinI, ulMinJ);
		numSets--;

		// Update size and tree arrays
		GPOS_ASSERT(new_root == ulMinI || new_root == ulMinJ);
		size->Replace(new_root, GPOS_NEW(m_mp) CDouble(dMinCost.Get()));
		tree->Replace(new_root, pexprJoin);
    }

    // Get final join tree (root will be at index of set representative)
	ULONG rootIndex = Find(parent, 0);
	CExpression *pexprResult = (*tree)[rootIndex];
	pexprResult->AddRef();

	// CWStringDynamic str(m_mp, L"\n");
	// COstreamString oss(&str);
	// pexprResult->OsPrint(oss);
	// GPOS_TRACE(str.GetBuffer());

	// Cleanup
	size->Release();
	GPOS_DELETE_ARRAY(parent);
	GPOS_DELETE_ARRAY(rank);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::BuildQueryGraphAndRunGOO
//
//	@doc:
//		Build query graph and run GOO
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPCoalescing::BuildQueryGraphAndRunGOO(CExpression *pexpr, ULONG ulTarget, CDouble dBestCost)
{
    // pexpr = logicalget expression
    // Try different union all splits
    ULONG num_query_graphs = 2;
    SComponent **splitted_components = NULL;
    splitted_components = GPOS_NEW_ARRAY(m_mp, SComponent *, num_query_graphs);

	CExpression *pexprResult = NULL;
    const ULONG ulMaxTrySplit = 1;
    for (ULONG ulTrySplit = 0; ulTrySplit < ulMaxTrySplit; ulTrySplit++) {
        SplitUnionAll(pexpr, ulTarget, splitted_components, ulTrySplit == 0);

		CExpressionArray *pexprArray = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CColRefArray *pdrgpcrOutput = GPOS_NEW(m_mp) CColRefArray(m_mp);
    	CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(m_mp) CColRef2dArray(m_mp);
        CDouble total_cost = 0;
        for (ULONG ul = 0; ul < num_query_graphs; ul++) {
            UpdateEdgeSelectivity(ulTarget, m_pdrgdSelectivity, ul,
                                  splitted_components);

            // Run GOO on current split
            CExpression *pexprGOO = RunGOO(m_ulComps, m_rgpcomp, m_ulEdges,
                                           m_rgpedge, m_pdrgdSelectivity,
										   splitted_components, ulTarget,
										   ul);
            CDouble dCostGOO = DCost(pexprGOO);
			total_cost = total_cost + dCostGOO;
			pexprArray->Append(pexprGOO);
            pdrgdrgpcrInput->Append(
                pexprGOO->DeriveOutputColumns()->Pdrgpcr(m_mp));
        }

        if (total_cost < dBestCost) {
			pdrgpcrOutput->AppendArray(
                (*pexprArray)[0]->DeriveOutputColumns()->Pdrgpcr(m_mp));
			pdrgpcrOutput->AddRef();
			pdrgdrgpcrInput->AddRef();
            pexprResult = GPOS_NEW(m_mp) CExpression(
                m_mp,
                GPOS_NEW(m_mp)
                    CLogicalUnionAll(m_mp, pdrgpcrOutput, pdrgdrgpcrInput),
                pexprArray);
        }
    }

    return pexprResult;
}

CExpression *CJoinOrderDPCoalescing::FindLogicalGetExpr(CExpression *pexpr) {
	// Base case: if expression is NULL, return NULL
	if (NULL == pexpr)
	{
		return NULL;
	}

	// Check if current node is LogicalGet
	if (COperator::EopLogicalGet == pexpr->Pop()->Eopid())
	{
		return pexpr;
	}

	// Recursively check children
	const ULONG ulArity = pexpr->Arity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = FindLogicalGetExpr((*pexpr)[ul]);
		if (NULL != pexprChild)
		{
			return pexprChild;
		}
	}

	// No LogicalGet found in this subtree
	return NULL;
}

FORCE_GENERATE_DBGSTR(gpopt::CJoinOrderDPCoalescing);

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPCoalescing::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDPCoalescing::OsPrint(IOstream &os) const
{
	// increase GPOS_LOG_MESSAGE_BUFFER_SIZE in file ILogger.h if the output of this method gets truncated
	CHashMapIter<CBitSet, CExpression, UlHashBitSet, FEqualBitSet,
				 CleanupRelease<CBitSet>, CleanupRelease<CExpression> >
		bitset_to_expr_map_iterator(m_phmbsexpr);
	CPrintPrefix pref(NULL, "      ");

	while (bitset_to_expr_map_iterator.Advance())
	{
		CDouble *cost =
			m_phmexprcost->Find(bitset_to_expr_map_iterator.Value());

		os << "Bitset: ";
		bitset_to_expr_map_iterator.Key()->OsPrint(os);
		os << std::endl;
		if (NULL != cost)
		{
			os << "Cost: " << *cost << std::endl;
		}
		else
		{
			os << "Cost: None" << std::endl;
		}
		os << "Best expression: " << std::endl;
		bitset_to_expr_map_iterator.Value()->OsPrintExpression(os, &pref);
	}

	for (ULONG k = 0; k < m_pdrgpexprTopKOrders->Size(); k++)
	{
		CDouble *cost = m_phmexprcost->Find((*m_pdrgpexprTopKOrders)[k]);

		os << "Best top-level expression [" << k << "]: " << std::endl;
		if (NULL != cost)
		{
			os << "Cost: " << *cost << std::endl;
		}
		else
		{
			os << "Cost: None" << std::endl;
		}
		(*m_pdrgpexprTopKOrders)[k]->OsPrintExpression(os, &pref);
	}
	os << std::endl;

	return os;
}
