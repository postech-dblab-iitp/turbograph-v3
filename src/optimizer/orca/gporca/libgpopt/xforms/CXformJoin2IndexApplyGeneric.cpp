//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (c) 2020 VMware and affiliates, Inc.
//
// CXformJoin2IndexApplyGeneric.cpp
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformJoin2IndexApplyGeneric.h"

#include "gpos/common/CAutoRef.h"

#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CScalarIdent.h"

#include "gpopt/operators/CLogicalPathJoin.h"
#include "gpopt/operators/CLogicalPathGet.h"
#include "gpopt/operators/CLogicalIndexPathGet.h"
#include "gpopt/operators/CLogicalIndexPathApply.h"

#include <vector>

using namespace gpmd;
using namespace gpopt;


// Can transform left outer join to left outer index apply?
// For hash distributed table, we can do outer index apply only
// when the inner columns used in the join condition contains
// the inner distribution key set. Master only table is ok to
// transform to outer index apply, but random table is not.
// Because if the inner is random distributed, there is no way
// to redistribute outer child to match inner on the join keys.
BOOL
CXformJoin2IndexApplyGeneric::FCanLeftOuterIndexApply(
	CMemoryPool *mp, CExpression *pexprInner, CExpression *pexprScalar,
	CTableDescriptor *ptabDesc, const CColRefSet *pcrsDist) const
{
	GPOS_ASSERT(m_fOuterJoin);
	IMDRelation::Ereldistrpolicy ereldist = ptabDesc->GetRelDistribution();

	if (ereldist == IMDRelation::EreldistrRandom)
		return false;
	else if (ereldist == IMDRelation::EreldistrMasterOnly)
		return true;

	// now consider hash distributed table
	CColRefSet *pcrsInnerOutput = pexprInner->DeriveOutputColumns();
	CColRefSet *pcrsScalarExpr = pexprScalar->DeriveUsedColumns();
	CColRefSet *pcrsInnerRefs = GPOS_NEW(mp) CColRefSet(mp, *pcrsScalarExpr);
	pcrsInnerRefs->Intersection(pcrsInnerOutput);

	// Distribution key set of inner GET must be subset of inner columns used in
	// the left outer join condition, but doesn't need to be equal.
	BOOL fCanOuterIndexApply = pcrsInnerRefs->ContainsAll(pcrsDist);
	pcrsInnerRefs->Release();
	if (fCanOuterIndexApply)
	{
		CColRefSet *pcrsEquivPredInner = GPOS_NEW(mp) CColRefSet(mp);
		// extract array of join predicates from join condition expression
		CExpressionArray *pdrgpexpr =
			CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
		for (ULONG ul = 0; ul < pdrgpexpr->Size(); ul++)
		{
			CExpression *pexprPred = (*pdrgpexpr)[ul];
			CColRefSet *pcrsPred = pexprPred->DeriveUsedColumns();

			// if it doesn't have equi-join predicate on the distribution key,
			// we can't transform to left outer index apply, because only
			// redistribute motion is allowed for outer child of join with
			// hash distributed inner child.
			// consider R LOJ S (both distribute by a and have index on a)
			// with the predicate S.a = R.a and S.a > R.b, left outer index
			// apply is still applicable.
			if (!pcrsPred->IsDisjoint(pcrsDist) &&
				CPredicateUtils::IsEqualityOp(pexprPred))
			{
				pcrsEquivPredInner->Include(pcrsPred);
			}
		}
		fCanOuterIndexApply = pcrsEquivPredInner->ContainsAll(pcrsDist);
		pcrsEquivPredInner->Release();
		pdrgpexpr->Release();
	}

	return fCanOuterIndexApply;
}

CXform::EXformPromise
CXformJoin2IndexApplyGeneric::Exfp(CExpressionHandle &exprhdl) const
{
	if (0 == exprhdl.DeriveUsedColumns(2)->Size() ||
		exprhdl.DeriveHasSubquery(2) || exprhdl.HasOuterRefs() ||
		2 <
			exprhdl.DeriveJoinDepth(
				1))	 // S62 allows inner depth 1 & 2 for union-all
		// 1 !=
		// 	exprhdl.DeriveJoinDepth(
		// 		1))	 // inner is definitely not a single get (with optional select/project/grby)
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

// actual transform
void
CXformJoin2IndexApplyGeneric::Transform(CXformContext *pxfctxt,
										CXformResult *pxfres,
										CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	/* S62 check if variable length join */
	if (pexprInner->Pop()->Eopid() == COperator::EopLogicalPathGet) {
		TransformApplyOnPathGet(pxfctxt, pxfres, pexpr);
		return;
	}

	// all predicates that could be used as index predicates, this includes the
	// join predicates and selection predicates of selects right above the get
	CExpression *pexprAllPredicates = pexprScalar;

	// a select node that sits right on top of the get node (if it exists, NULL otherwise)
	CExpression *selectThatIsParentOfGet = NULL;

	// the logical get node (dynamic or regular get) at the bottom of the inner tree
	CExpression *pexprGet = NULL;

	// the logical union all node
	CExpression *pexprUnionAll = NULL;

	// the highest node of the right child that gets inserted above the index get
	// into the alternative, or NULL if there is no such node
	// (this is a project, GbAgg or a select node above a project or GbAgg)
	CExpression *nodesToInsertAboveIndexGet = NULL;

	// the cut-off point for "nodesAboveIndexGet", this node is below nodesAboveIndexGet
	// but it doesn't get inserted into the alternative anymore
	// (or NULL, if nodesAboveIndexGet == NULL)
	CExpression *endOfNodesToInsertAboveIndexGet = NULL;

	// Example:
	//
	//      Join (with join preds)
	//      /   \                        .
	//   Leaf   Select (not used as an index pred)  <== nodesToInsertAboveIndexGet
	//            \                      .
	//            GbAgg
	//              \                    .
	//              Project
	//                \                  .
	//               Select (index/residual preds)  <== selectThatIsParentOfGet,
	//                  \                               endOfNodesToInsertAboveIndexGet
	//                  Get                         <== pexprGet
	//
	// Generated alternative:
	//
	//      Apply                                   <== new apply node (inner/outer)
	//      /   \                        .
	//   Leaf   select (not used as an index pred)  \\ .
	//            \                                 || this stack of unary nodes is
	//            GbAgg                             || transferred from the pattern
	//              \                               || above (unchanged)
	//              Project                         //
	//                \                  .
	//               IndexGet                       <== new IndexGet node with
	//                                                  index/residual preds

	// info on the get node (a get node or a dynamic get)
	CTableDescriptor *ptabdescInner = NULL;
	const CColRefSet *distributionCols = NULL;
	CLogicalDynamicGet *popDynamicGet = NULL;
	CAutoRef<CColRefSet> groupingColsToCheck;
	BOOL hasUnionAllOverLogicalGet = false;
	BOOL hasSelectAboveGet = false;

	// walk down the right child tree, accepting some unary operators
	// like project and GbAgg and select, until we find a logical get
	for (CExpression *pexprCurrInnerChild = pexprInner; NULL == pexprGet;
		 pexprCurrInnerChild =
			 (NULL == pexprGet ? (*pexprCurrInnerChild)[0] : NULL))
	{
		switch (pexprCurrInnerChild->Pop()->Eopid())
		{
			case COperator::EopLogicalUnionAll:
				// S62 handle this case totally specially, using different set of internal functions
				//return TransformOnUnionAllInner(pxfctxt, pxfres, pexpr);
			{
				if (((*pexprCurrInnerChild)[0]->Pop()->Eopid() == COperator::EopLogicalProjectColumnar) &&
					((*(*pexprCurrInnerChild)[0])[0]->Pop()->Eopid() == COperator::EopLogicalGet)) {
					pexprUnionAll = pexprCurrInnerChild;
					pexprGet = (*(*pexprCurrInnerChild)[0])[0]; // TODO S62 currently consider only first Get
					CLogicalGet *popGet =
						CLogicalGet::PopConvert(pexprGet->Pop());

					ptabdescInner = popGet->Ptabdesc();
					distributionCols = popGet->PcrsDist();

					if (NULL != groupingColsToCheck.Value() &&
						!groupingColsToCheck->ContainsAll(distributionCols))
					{
						// the grouping columns are not a superset of the distribution columns
						return;
					}
					
					hasUnionAllOverLogicalGet = true;
				} else if (((*pexprCurrInnerChild)[0]->Pop()->Eopid() == COperator::EopLogicalProjectColumnar) &&
					((*(*pexprCurrInnerChild)[0])[0]->Pop()->Eopid() == COperator::EopLogicalSelect) &&
					((*(*(*pexprCurrInnerChild)[0])[0])[0]->Pop()->Eopid() == COperator::EopLogicalGet)) {
					pexprUnionAll = pexprCurrInnerChild;
					pexprGet = (*(*(*pexprCurrInnerChild)[0])[0])[0]; // TODO S62 currently consider only first Get
					CLogicalGet *popGet =
						CLogicalGet::PopConvert(pexprGet->Pop());

					ptabdescInner = popGet->Ptabdesc();
					distributionCols = popGet->PcrsDist();

					if (NULL != groupingColsToCheck.Value() &&
						!groupingColsToCheck->ContainsAll(distributionCols))
					{
						// the grouping columns are not a superset of the distribution columns
						return;
					}
					
					hasUnionAllOverLogicalGet = true;
					hasSelectAboveGet = true;
				}
			}
			break;
			case COperator::EopLogicalSelect:
				// if the select pred has a subquery, don't generate alternatives
				if ((*pexprCurrInnerChild)[1]->DeriveHasSubquery())
				{
					return;
				}
				// this might be a select on top of a get, unless proven otherwise
				selectThatIsParentOfGet = pexprCurrInnerChild;
				break;

			case COperator::EopLogicalGbAgg:
			case COperator::EopLogicalProject:
			case COperator::EopLogicalProjectColumnar:
				// We tolerate these operators in the tree (with some conditions, see below) and will
				// just copy them into the result of the transform, any selects above this node won't
				// be used for index predicates.
				{
					if ((*pexprCurrInnerChild)[1]->DeriveHasSubquery())
					{
						return;
					}

					CColRefSet *joinPredUsedCols = GPOS_NEW(mp)
						CColRefSet(mp, *(pexprScalar->DeriveUsedColumns()));

					joinPredUsedCols->Exclude(
						pexprOuter->DeriveOutputColumns());
					joinPredUsedCols->Exclude(
						(*pexprCurrInnerChild)[0]->DeriveOutputColumns());
					BOOL joinPredUsesProjectedColumnsRowMajor =
						(0 < joinPredUsedCols->Size());
					

					if (pexprCurrInnerChild->Pop()->Eopid() != COperator::EopLogicalProjectColumnar
						&& joinPredUsesProjectedColumnsRowMajor)
					{
						// For COperator::EopLogicalGbAgg and COperator::EopLogicalProject
						// The join predicate uses columns that neither come from the outer table
						// nor from the child of this node, therefore it must reference columns that
						// are produced by pexprCurrInnerChild. Note that in the future we could
						// also try to split off the join preds and any select preds above this node
						// that can be applied to the get.
						joinPredUsedCols->Release();
						return;
					}

					// Logic for columnar project - projecting ident column is allowed for index join
					if( pexprCurrInnerChild->Pop()->Eopid() == COperator::EopLogicalProjectColumnar ) {
						// even for outer columns, 
						CExpression *pexprProjList = pexprCurrInnerChild->operator[](1);
						pexprProjList->AddRef();
						for(ULONG elem_idx = 0; elem_idx < pexprProjList->Arity(); elem_idx ++ ){
							CExpression *pexprProjElem = pexprProjList->operator[](elem_idx);
							CExpression *pexprScalarExpr = pexprProjElem->operator[](0);
							if( pexprScalarExpr->Pop()->Eopid() == COperator::EopScalarIdent ) {
								// when lower expr is Ident, exclude colref of ProjElement
								joinPredUsedCols->Exclude( ((CScalarProjectElement*)pexprProjElem->Pop())->Pcr() );
							}
						}
						BOOL joinPredUsesProjectedColumnsColumnar = (0 < joinPredUsedCols->Size());
						if( joinPredUsesProjectedColumnsColumnar ) {
							return;
						}
					}

					joinPredUsedCols->Release();

					if (COperator::EopLogicalGbAgg ==
						pexprCurrInnerChild->Pop()->Eopid())
					{
						CLogicalGbAgg *grbyAggOp = CLogicalGbAgg::PopConvert(
							pexprCurrInnerChild->Pop());

						GPOS_ASSERT(NULL != grbyAggOp);
						if (NULL != grbyAggOp->Pdrgpcr() &&
							0 < grbyAggOp->Pdrgpcr()->Size())
						{
							// This has grouping cols. We can only do an index join with a groupby
							// on the inner side if the grouping columns are a superset of the
							// distribution columns. This way, we can put a groupby locally on top
							// of each of the gets on every segment.
							CColRefSet *groupingCols = GPOS_NEW(mp)
								CColRefSet(mp, grbyAggOp->Pdrgpcr());

							// if there are multiple groupbys, then check the intersection of their grouping cols
							groupingCols->Intersection(
								groupingColsToCheck.Value());
							CRefCount::SafeRelease(groupingColsToCheck.Value());
							groupingColsToCheck = groupingCols;

							if (0 == groupingCols->Size())
							{
								// grouping columns don't intersect, give up
								return;
							}
						}
						else
						{
							// This is an aggregate. We won't be able to split it into tasks
							// that are co-located to the gets on the individual segments, so
							// don't allow the index join transformation.
							return;
						}
					}
					selectThatIsParentOfGet = NULL;
				}
				break;

			case COperator::EopLogicalGet:
			{
				CLogicalGet *popGet =
					CLogicalGet::PopConvert(pexprCurrInnerChild->Pop());

				ptabdescInner = popGet->Ptabdesc();
				distributionCols = popGet->PcrsDist();
				pexprGet = pexprCurrInnerChild;

				if (NULL != groupingColsToCheck.Value() &&
					!groupingColsToCheck->ContainsAll(distributionCols))
				{
					// the grouping columns are not a superset of the distribution columns
					return;
				}
			}
			break;

			case COperator::EopLogicalDynamicGet:
			{
				popDynamicGet =
					CLogicalDynamicGet::PopConvert(pexprCurrInnerChild->Pop());
				ptabdescInner = popDynamicGet->Ptabdesc();
				distributionCols = popDynamicGet->PcrsDist();
				pexprGet = pexprCurrInnerChild;
			}
			break;

			default:
				// in all other cases, the expression does not conform to our
				// expectations and we won't generate an alternative
				return;
		}
	}

	// handle the select node with additional candidates for index preds, if it exists
	if (NULL != selectThatIsParentOfGet)
	{
		pexprAllPredicates = CPredicateUtils::PexprConjunction(
			mp, pexprAllPredicates, (*selectThatIsParentOfGet)[1]);
	}
	else
	{
		// In the "if" case above, CPredicateUtils::PexprConjunction does an AddRef on
		// pexprAllPredicates and returns a new expression. Here, we just do the AddRef,
		// since CreateHomogeneousIndexApplyAlternatives consumes a ref on pexprAllPredicates.
		pexprAllPredicates->AddRef();
	}

	// determine the set of nodes that need to be copied from pexprInner to the alternative
	if (pexprInner != pexprGet && pexprInner != selectThatIsParentOfGet)
	{
		// yes, there are additional nodes beyond a get with an optional select
		nodesToInsertAboveIndexGet = pexprInner;

		if (NULL != selectThatIsParentOfGet)
		{
			// insert the right child nodes, up to but not including, the
			// select node above the get
			endOfNodesToInsertAboveIndexGet = selectThatIsParentOfGet;
		}
		else
		{
			// insert all right child nodes above the get node
			if (hasUnionAllOverLogicalGet) {
				endOfNodesToInsertAboveIndexGet = pexprUnionAll;
			} else {
				endOfNodesToInsertAboveIndexGet = pexprGet;
			}
		}
	}

	if (m_fOuterJoin &&
		!FCanLeftOuterIndexApply(mp, pexprGet, pexprScalar, ptabdescInner,
								 distributionCols))
	{
		// It is a left outer join, but we can't do outer index apply,
		// stop transforming and return immediately.
		pexprAllPredicates->Release();
		return;
	}

	if (hasUnionAllOverLogicalGet) {
		CreateHomogeneousIndexApplyAlternativesUnionAll(
			mp, pexpr->Pop(), pexprOuter, pexprUnionAll, pexprAllPredicates,
			pexprScalar, nodesToInsertAboveIndexGet, endOfNodesToInsertAboveIndexGet,
			ptabdescInner, popDynamicGet, pxfres,
			(m_generateBitmapPlans ? IMDIndex::EmdindBitmap
								: IMDIndex::EmdindBtree),
			hasSelectAboveGet);
	} else {
		// insert the btree or bitmap alternatives
		CreateHomogeneousIndexApplyAlternatives(
			mp, pexpr->Pop(), pexprOuter, pexprGet, pexprAllPredicates, pexprScalar,
			nodesToInsertAboveIndexGet, endOfNodesToInsertAboveIndexGet,
			ptabdescInner, popDynamicGet, pxfres,
			(m_generateBitmapPlans ? IMDIndex::EmdindBitmap
								: IMDIndex::EmdindBtree));
	}

	CRefCount::SafeRelease(pexprAllPredicates);
}

void
CXformJoin2IndexApplyGeneric::TransformApplyOnPathGet(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const {

	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	GPOS_ASSERT(pexprInner->Pop()->Eopid() == COperator::EopLogicalPathGet);

	CLogicalPathJoin *op = (CLogicalPathJoin *) pexpr->Pop();

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsScalarExpr = NULL;
	CColRefSet *outer_refs = NULL;
	CColRefSet *pcrsReqd = NULL;
	ComputeColumnSets(mp, pexprInner, pexprScalar, &pcrsScalarExpr, &outer_refs,
					  &pcrsReqd);
	
	CColRefArray *colref_array = outer_refs->Pdrgpcr(mp);	// TODO this is wrong
	colref_array->AddRef();
	pexprScalar->AddRef();

	// generate index scan
	CExpression *pexprGenratedInnerIndexScan;
	{
		CLogicalPathGet *path_op = (CLogicalPathGet *) pexprInner->Pop();

		// find indexes
		CColRefArray *pdrgpcrOutput = path_op->PdrgpcrOutput();
		pdrgpcrOutput->AddRef();

		GPOS_ASSERT(path_op->PtabdescArray()->Size() == 1);
		// TODO currently only one index
		CTableDescriptor* first_table_desc = path_op->PtabdescArray()->operator[](0);

		CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDRelation *pmdrel = md_accessor->RetrieveRel(first_table_desc->MDId());
		const ULONG ulIndices = first_table_desc->IndexCount();

		std::vector<const IMDIndex *> pmdindexarray;
		for (ULONG ul = 0; ul < ulIndices; ul++)
		{
			IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
			const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);

			// find index having key as "_sid"
			if (pmdindex->IndexType() == IMDIndex::EmdindFwdAdjlist) {
				pmdindexarray.push_back(pmdindex);
			}
			// if( pmdindex->KeyAt(0) == 1 ) {
			// 	pmdindexarray.push_back(pmdindex);
			// }
		}
		GPOS_ASSERT(pmdindexarray.size() == 1 );	// currently one index
		CColRefSet *pcrsIndexCols =
			CXformUtils::PcrsIndexKeys(mp, pdrgpcrOutput, pmdindexarray[0], pmdrel);
		if (pcrsScalarExpr->IsDisjoint(pcrsIndexCols))
		{
			return;
		}

		pexprScalar->AddRef();
		pexprGenratedInnerIndexScan = 
			GPOS_NEW(mp) CExpression(
				mp,
				GPOS_NEW(mp) CLogicalIndexPathGet(
					mp, 
					pmdindexarray,
					path_op->PtabdescArray(),
					op->UlOpId(),
					GPOS_NEW(mp) CName(mp, path_op->Name()),
					pdrgpcrOutput,
					path_op->LowerBound(),
					path_op->UpperBound()
				),
				pexprScalar
			);
	}

	// return indexouterapply
	pexprOuter->AddRef();
	CExpression *pexprIndexApply = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalIndexPathApply(
				mp,
				op->LowerBound(),
				op->UpperBound(),
				colref_array,
				false,	// currently, outerjoin is false.
				pexprScalar),
		pexprOuter, pexprGenratedInnerIndexScan,
		CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/));	// scalarconst(1)

	pxfres->Add(pexprIndexApply);
}


// S62 this enables index join on union all inner expression
// void CXformJoin2IndexApplyGeneric::TransformOnUnionAllInner(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) {

// 	CMemoryPool *mp = pxfctxt->Pmp();

// 	// extract components
// 	CExpression *pexprOuter = (*pexpr)[0];
// 	CExpression *pexprInner = (*pexpr)[1];
// 	CExpression *pexprScalar = (*pexpr)[2];

// 	// all predicates that could be used as index predicates, this includes the
// 	// join predicates and selection predicates of selects right above the get
// 	CExpression *pexprAllPredicates = pexprScalar;

// 	// a select node that sits right on top of the get node (if it exists, NULL otherwise)
// 	CExpression *selectThatIsParentOfGet = NULL;

// 	// the logical get node (dynamic or regular get) at the bottom of the inner tree
// 	CExpression *pexprGet = NULL;

// 	// the highest node of the right child that gets inserted above the index get
// 	// into the alternative, or NULL if there is no such node
// 	// (this is a project, GbAgg or a select node above a project or GbAgg)
// 	CExpression *nodesToInsertAboveIndexGet = NULL;

// 	// the cut-off point for "nodesAboveIndexGet", this node is below nodesAboveIndexGet
// 	// but it doesn't get inserted into the alternative anymore
// 	// (or NULL, if nodesAboveIndexGet == NULL)
// 	CExpression *endOfNodesToInsertAboveIndexGet = NULL;

// 	// Example:
// 	//
// 	//      Join (with join preds)
// 	//      /   \                        .
// 	//   Leaf   Select (not used as an index pred)  <== nodesToInsertAboveIndexGet
// 	//            \                      .
// 	//            GbAgg
// 	//              \                    .
// 	//              Project
// 	//                \                  .
// 	//               Select (index/residual preds)  <== selectThatIsParentOfGet,
// 	//                  \                               endOfNodesToInsertAboveIndexGet
// 	//                  Get                         <== pexprGet
// 	//
// 	// Generated alternative:
// 	//
// 	//      Apply                                   <== new apply node (inner/outer)
// 	//      /   \                        .
// 	//   Leaf   select (not used as an index pred)  \\ .
// 	//            \                                 || this stack of unary nodes is
// 	//            GbAgg                             || transferred from the pattern
// 	//              \                               || above (unchanged)
// 	//              Project                         //
// 	//                \                  .
// 	//               IndexGet                       <== new IndexGet node with
// 	//                                                  index/residual preds

// 	// info on the get node (a get node or a dynamic get)
// 	CTableDescriptor *ptabdescInner = NULL;
// 	const CColRefSet *distributionCols = NULL;
// 	CLogicalDynamicGet *popDynamicGet = NULL;
// 	CAutoRef<CColRefSet> groupingColsToCheck;

// 	// flag to handle union all
// 	bool isUnionAllIncluded = false;

// 	// TODO implement recursive

// 	// TODO recursive inner function should be called for recursive manner.

// 	// if all childs possible, then add indexjoin plan


// }