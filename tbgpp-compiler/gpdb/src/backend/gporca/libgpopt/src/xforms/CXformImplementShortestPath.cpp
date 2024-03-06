//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformImplementShortestPath.cpp
//
//	@doc:
//		Implementation of ShortestPath operator
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementShortestPath.h"

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalShortestPath.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalShortestPath.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementShortestPath::CXformImplementShortestPath
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementShortestPath::CXformImplementShortestPath(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalShortestPath(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // relational child
		  ))
{
}

CXform::EXformPromise
CXformImplementShortestPath::Exfp(CExpressionHandle &exprhdl) const
{
	// Although it is valid SQL for the limit/offset to be a subquery, Orca does
	// not support it
	(void) exprhdl;
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementShortestPath::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementShortestPath::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	// CLogicalShortestPath *popShortestPath = CLogicalShortestPath::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];

	// addref all components
	pexprRelational->AddRef();

	// assemble physical operator
	CExpression *pexprShortestPath = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalShortestPath(mp), pexprRelational);

	// add alternative to results
	pxfres->Add(pexprShortestPath);
}


// EOF
