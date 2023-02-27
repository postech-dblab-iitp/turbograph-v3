//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CXformCollapseProjectColumnar.cpp
//
//	@doc:
//		Implementation of the transform that collapses two cascaded project nodes
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformCollapseProjectColumnar.h"

#include "gpos/base.h"

//#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalProjectColumnar.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternTree.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseProjectColumnar::CXformCollapseProjectColumnar
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCollapseProjectColumnar::CXformCollapseProjectColumnar(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp),							// S62 columnar project
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp),						// S62 columnar project
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
				  ),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}



//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCollapseProjectColumnar::Exfp(CExpressionHandle &	 //exprhdl
) const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseProjectColumnar::Transform
//
//	@doc:
//		Actual transformation to collapse projects
//
//---------------------------------------------------------------------------
void
CXformCollapseProjectColumnar::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprCollapsed = CUtils::PexprCollapseColumnarProjects(mp, pexpr);

	if (NULL != pexprCollapsed)
	{
		pxfres->Add(pexprCollapsed);
	}
}

// EOF
