#include "gpopt/xforms/CXformPathJoin2IndexPathApply.h"

#include "gpos/common/CAutoRef.h"

#include "gpopt/base/CUtils.h"

#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CScalarIdent.h"



#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDIndex.h"



using namespace gpmd;
using namespace gpopt;

CXform::EXformPromise
CXformPathJoin2IndexPathApply::Exfp(CExpressionHandle &exprhdl) const
{
	if (0 == exprhdl.DeriveUsedColumns(2)->Size() ||
		exprhdl.DeriveHasSubquery(2) || exprhdl.HasOuterRefs() ||
		1 !=
			exprhdl.DeriveJoinDepth(
				1))	 // inner is definitely not a single get (with optional select/project/grby)
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

void
CXformPathJoin2IndexPathApply::ComputeColumnSets(CMemoryPool *mp,
										 CExpression *pexprInner,
										 CExpression *pexprScalar,
										 CColRefSet **ppcrsScalarExpr,
										 CColRefSet **ppcrsOuterRefs,
										 CColRefSet **ppcrsReqd) const
{
	CColRefSet *pcrsInnerOutput = pexprInner->DeriveOutputColumns();
	*ppcrsScalarExpr = pexprScalar->DeriveUsedColumns();
	*ppcrsOuterRefs = GPOS_NEW(mp) CColRefSet(mp, **ppcrsScalarExpr);
	(*ppcrsOuterRefs)->Difference(pcrsInnerOutput);

	*ppcrsReqd = GPOS_NEW(mp) CColRefSet(mp);
	(*ppcrsReqd)->Include(pcrsInnerOutput);
	(*ppcrsReqd)->Include(*ppcrsScalarExpr);
	(*ppcrsReqd)->Difference(*ppcrsOuterRefs);
}

// actual transform
void
CXformPathJoin2IndexPathApply::Transform(CXformContext *pxfctxt,
										CXformResult *pxfres,
										CExpression *pexpr) const
{
	
	/*
		referred following functions
		// CXformJoin2IndexApplyGeneric::Transform(
		// CXformJoin2IndexApply::CreateHomogeneousIndexApplyAlternatives(
		// CXformJoin2IndexApply::CreateAlternativesForBtreeIndex(
	*/

	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));
		
	CMemoryPool *mp = pxfctxt->Pmp();
	
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	if( pexpr->Pop()->Eopid() != COperator::EopLogicalPathJoin ) {
		return;
	}

	GPOS_ASSERT(pexprInner->Pop()->Eopid() == COperator::EopLogicalGet);	// S62 for now no inner interim ops allowed : only outer PATHJOIN IndexGet only

	CLogicalPathJoin *op = (CLogicalPathJoin *) pexpr->Pop();

	BOOL isOuterJoin = false;	// S62VAR INNER JOIN ONLY
	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsScalarExpr = NULL;
	CColRefSet *outer_refs = NULL;
	CColRefSet *pcrsReqd = NULL;
	ComputeColumnSets(mp, pexprInner, pexprScalar, &pcrsScalarExpr, &outer_refs,
					  &pcrsReqd);
	
	CColRefArray *colref_array = outer_refs->Pdrgpcr(mp);
	CExpression *origJoinPred = pexprScalar;

	// generate index scan
	CExpression *pexprGenratedInnerIndexScan = pexprOuter;
	// {
	// 	CTableDescriptor *ptabdescInner = NULL;
	// 	CLogicalGet *popGet =
	// 		CLogicalGet::PopConvert(pexprInner->Pop());
	// 	ptabdescInner = popGet->Ptabdesc();
	// 	const ULONG ulIndices = ptabdescInner->IndexCount();
	// 	GPOS_ASSERT(ulIndices > 0);

	// 	// fetch index
	// 	const IMDIndex *pmdindex;
	// 	for (ULONG ul = 0; ul < ulIndices; ul++) {
	// 		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
	// 		pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
	// 		//pmdindex->
	// 		// TODO MUST select forward index
	// 			// refer to FIndexApplicable!!
	// 	}

	// 	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	// 	const IMDRelation *pmdrel = md_accessor->RetrieveRel(ptabdescInner->MDId());

	// 	// TODO can we directly use CXformUtils::PexprLogicalIndexGet( ??
	// 		// CExpression *pexprLogicalIndexGet =

	// 	// TODO set pexprGenratedInnerIndexScan = pexprLogicalIndexGet
	// }


	// generate index join 
	pexprOuter->AddRef();
	CExpression *pexprIndexApply = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp)
			CLogicalIndexPathApply(mp,
				op->LowerBound(),
				op->UpperBound(),
				colref_array, isOuterJoin, origJoinPred),
			pexprOuter,
			pexprGenratedInnerIndexScan,	// TODO inner is not  this inner.
			CPredicateUtils::PexprConjunction(mp, NULL /*pdrgpexpr*/)
		);
	pxfres->Add(pexprIndexApply);

	outer_refs->Release();
	pcrsReqd->Release();

}