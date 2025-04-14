//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderGEM.cpp
//
//	@doc:
//		Implementation of dynamic programming-based join order generation
//---------------------------------------------------------------------------

#include "gpopt/xforms/CJoinOrderGEM.h"

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalUnionAll.h"

using namespace gpopt;

#define SPLIT_TIME_LIMIT 10
#define COMPONENT_TIME_LIMIT 100

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::SComponentPair::SComponentPair(CBitSet *pbsFst, CBitSet *pbsSnd)
	: m_pbsFst(pbsFst), m_pbsSnd(pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);
	GPOS_ASSERT(pbsFst->IsDisjoint(pbsSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderGEM::SComponentPair::HashValue(const SComponentPair *pcomppair)
{
	GPOS_ASSERT(NULL != pcomppair);

	return CombineHashes(pcomppair->m_pbsFst->HashValue(),
						 pcomppair->m_pbsSnd->HashValue());
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderGEM::SComponentPair::Equals(const SComponentPair *pcomppairFst,
									 const SComponentPair *pcomppairSnd)
{
	GPOS_ASSERT(NULL != pcomppairFst);
	GPOS_ASSERT(NULL != pcomppairSnd);

	return pcomppairFst->m_pbsFst->Equals(pcomppairSnd->m_pbsFst) &&
		   pcomppairFst->m_pbsSnd->Equals(pcomppairSnd->m_pbsSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::CJoinOrderGEM
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::CJoinOrderGEM(CMemoryPool *mp,
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
//		CJoinOrderGEM::~CJoinOrderGEM
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderGEM::~CJoinOrderGEM()
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
//		CJoinOrderGEM::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd)
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
//		CJoinOrderGEM::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderGEM::DeriveStats(CExpression *pexpr)
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
//		CJoinOrderGEM::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderGEM::DCost(CExpression *pexpr)
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
//		CJoinOrderGEM::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd)
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
//		CJoinOrderGEM::lCreateTableDescForRel
//
//	@doc:
//		Create table descriptor for relation
//
//---------------------------------------------------------------------------
CTableDescriptor *CJoinOrderGEM::CreateTableDescForVirtualTable(
    CTableDescriptor *ptabdesc, IMdIdArray *pdrgmdid)
{
	CTableDescriptor *new_ptabdesc;
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	if (pdrgmdid->Size() > 1) {
		IMDId *mdid = mda->AddVirtualTable(ptabdesc->MDId(), pdrgmdid);

		new_ptabdesc = GPOS_NEW(m_mp) CTableDescriptor(
			m_mp,
			mdid,
			ptabdesc->Name(),
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
			ptabdesc->Name(),
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

void CJoinOrderGEM::SplitUnionAll(CExpression *pexpr, ULONG ulTarget,
                                           SComponent **splitted_components,
                                           BOOL is_first_time)
{
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
//		CJoinOrderGEM::PexprCopyWithNewChildren
//
//	@doc:
//		Copy expression tree and replace scan_expr with new_expr
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderGEM::PexprCopyWithNewChildren(
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
//		CJoinOrderGEM::SplitGraphlets
//
//	@doc:
//		Split graphlets into two groups
//
//---------------------------------------------------------------------------
void CJoinOrderGEM::SplitGraphlets(IMdIdArray *pimdidarray,
                                            ULONG ulTables,
                                            IMdIdArray *pdrgmdidFirst,
                                            IMdIdArray *pdrgmdidSecond,
											ULONG ulSplitIndex)
{
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

CExpression *CJoinOrderGEM::ProcessUnionAllComponents(CDouble &dCost)
{
    CExpression *pexprResultUnionAll = NULL;
	CDouble dTotalTime = 0;

    // Iterate through components
    for (ULONG ul = 0; ul < m_ulComps; ul++) {
		CTimerUser timerComponent;
		timerComponent.Restart();

        // Get component
        SComponent *pcomp = m_rgpcomp[ul];
        if (NULL == pcomp) {
            continue;
        }

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

        // CDouble dComponentTime(timerComponent.ElapsedUS() /
        //                        CDouble(GPOS_USEC_IN_MSEC));
        // dTotalTime = CDouble(dTotalTime.Get() + dComponentTime.Get());
        // if (dTotalTime.Get() > COMPONENT_TIME_LIMIT) {
        //     break;
        // }
    }
	
	return pexprResultUnionAll;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::PexprExpand()
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
//		CJoinOrderGEM::CalcEdgeSelectivity
//
//	@doc:
//		Calculate selectivity for an edge
//
//---------------------------------------------------------------------------
void
CJoinOrderGEM::CalcEdgeSelectivity(CDoubleArray **pdrgdSelectivity)
{
	*pdrgdSelectivity = GPOS_NEW(m_mp) CDoubleArray(m_mp);

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

void CJoinOrderGEM::UpdateEdgeSelectivity(
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
//		CJoinOrderGEM::Find
//
//	@doc:
//		Find set representative with path compression
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderGEM::Find(ULONG *parent, ULONG x)
{
	if (parent[x] != x)
	{
		parent[x] = Find(parent, parent[x]); // Path compression
	}
	return parent[x];
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::Union
//
//	@doc:
//		Union two sets by rank
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderGEM::Union(ULONG *parent, ULONG *rank, ULONG x, ULONG y)
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
//		CJoinOrderGEM::GOO
//
//	@doc:
//		Greedy Operator Ordering (GOO) algorithm implementation using UnionFind
//
//---------------------------------------------------------------------------
CExpression *CJoinOrderGEM::RunGOO(ULONG ulComps, SComponent **rgpcomp,
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
		CExpression *pexprLeft = (*tree)[ulMinJ];
		CExpression *pexprRight = (*tree)[ulMinI]; 
		pexprLeft->AddRef();
		pexprRight->AddRef();

		// Create join expression using the referenced expressions
		CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
			m_mp, pexprLeft, pexprRight, pexprPred);
		
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

	// Cleanup
	size->Release();
	GPOS_DELETE_ARRAY(parent);
	GPOS_DELETE_ARRAY(rank);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::BuildQueryGraphAndRunGOO
//
//	@doc:
//		Build query graph and run GOO
//---------------------------------------------------------------------------
CExpression *
CJoinOrderGEM::BuildQueryGraphAndRunGOO(CExpression *pexpr, ULONG ulTarget, CDouble dBestCost)
{
    // pexpr = logicalget expression
    // Try different union all splits
    ULONG num_query_graphs = 2;
    SComponent **splitted_components = NULL;
    splitted_components = GPOS_NEW_ARRAY(m_mp, SComponent *, num_query_graphs);

	CExpression *pexprResult = NULL;
    const ULONG ulMaxTrySplit = 10;
	CDouble dTotalTime = 0;
    for (ULONG ulTrySplit = 0; ulTrySplit < ulMaxTrySplit; ulTrySplit++) {
		CTimerUser timerSplit;
		timerSplit.Restart();
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
            CDouble dCostGOO = DCost(pexprGOO) * 0.5;
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
		
		// CDouble dSplitTime(timerSplit.ElapsedUS() / CDouble(GPOS_USEC_IN_MSEC));
		// dTotalTime = CDouble(dTotalTime.Get() + dSplitTime.Get());
		
		// if (dTotalTime.Get() > SPLIT_TIME_LIMIT) {
		// 	break;
		// }
    }

    return pexprResult;
}

CExpression *CJoinOrderGEM::FindLogicalGetExpr(CExpression *pexpr) {
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

CExpression *CJoinOrderGEM::PushJoinBelowUnionAll(CExpression *pexpr) const
{
	CMemoryPool *mp = this->m_mp;

	// extract components
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	CExpression *pexprUnionAll, *pexprOther;

	// This is used to preserve the join order, which we leave
	// for other xforms to optimize
	BOOL isLeftChildUnion;

	if (COperator::EopLogicalUnionAll == pexprLeft->Pop()->Eopid())
	{
		pexprUnionAll = pexprLeft;
		pexprOther = pexprRight;
		isLeftChildUnion = true;
	}
	else
	{
		pexprUnionAll = pexprRight;
		pexprOther = pexprLeft;
		isLeftChildUnion = false;
	}

	CLogicalUnionAll *popUnionAll =
		CLogicalUnionAll::PopConvert(pexprUnionAll->Pop());
	CColRef2dArray *union_input_columns = popUnionAll->PdrgpdrgpcrInput();

	if (!popUnionAll->CanPushJoinBelowUnionAll()) 
	{
		return pexpr;
	}

	// used for alternative union all expression
	CColRef2dArray *input_columns = GPOS_NEW(mp) CColRef2dArray(mp);
	CExpressionArray *join_array = GPOS_NEW(mp) CExpressionArray(mp);

	CColRefArray *other_colref_array =
		pexprOther->DeriveOutputColumns()->Pdrgpcr(mp);
	CColRefArray *colref_array_from = GPOS_NEW(mp) CColRefArray(mp);

	// Iterate through all union all children
	const ULONG arity = pexprUnionAll->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexprUnionAll)[ul];
		CColRefArray *child_colref_array = (*union_input_columns)[ul];

		CExpression *pexprLeftChild, *pexprRightChild, *pexprRemappedScalar,
			*pexprRemappedOther, *join_expr;

        if (ul == 0)
        {
            // The 1st child is special
            // The join table and condition can be readily used
            // and doesn't require remapping
            pexprRemappedScalar = pexprScalar;
            pexprRemappedOther = pexprOther;
            pexprRemappedScalar->AddRef();
            pexprRemappedOther->AddRef();
            // We append the output columns from the 1st union all child,
            // and from the other table, and use them as the source
            // of column remapping
            colref_array_from->AppendArray(child_colref_array);
            colref_array_from->AppendArray(other_colref_array);
            input_columns->Append(colref_array_from);
        }
        else
        {
            CColRefArray *colref_array_to = GPOS_NEW(mp) CColRefArray(mp);
            // We append the output columns from the 2nd (and onward)
            // union all child, and a copy of the other table's output
            // columns, and use them as the destination of column
            // remapping
            colref_array_to->AppendArray(child_colref_array);
            colref_array_to->AppendArray(other_colref_array);
            input_columns->Append(colref_array_to);
            UlongToColRefMap *colref_mapping =
                CUtils::PhmulcrMapping(mp, colref_array_from, colref_array_to);
            // Create a copy of the join condition with remapped columns,
            // and a copy of the other expression with remapped columns
            pexprRemappedScalar = pexprScalar->PexprCopyWithRemappedColumns(
                mp, colref_mapping, true /*must_exist*/);
            pexprRemappedOther = pexprOther;
            pexprRemappedOther->AddRef();
            colref_mapping->Release();
        }

		// Preserve the join order
		if (isLeftChildUnion)
		{
			pexprLeftChild = pexprChild;
			pexprRightChild = pexprRemappedOther;
			pexprLeftChild->AddRef();
		}
		else
		{
			pexprLeftChild = pexprRemappedOther;
			pexprRightChild = pexprChild;
			pexprRightChild->AddRef();
		}


		BOOL isOuterJoin =
			pexpr->Pop()->Eopid() == COperator::EopLogicalLeftOuterJoin;
		if (isOuterJoin)
		{
			join_expr = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(
				mp, pexprLeftChild, pexprRightChild, pexprRemappedScalar);
		}
		else
		{
			join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
				mp, pexprLeftChild, pexprRightChild, pexprRemappedScalar);
		}
		join_array->Append(join_expr);
	}
	other_colref_array->Release();

	CColRefArray *output_columns = pexpr->DeriveOutputColumns()->Pdrgpcr(mp);
	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, output_columns, input_columns),
		join_array);

	return pexprAlt;
}

FORCE_GENERATE_DBGSTR(gpopt::CJoinOrderGEM);

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderGEM::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderGEM::OsPrint(IOstream &os) const
{
	return os;
}
