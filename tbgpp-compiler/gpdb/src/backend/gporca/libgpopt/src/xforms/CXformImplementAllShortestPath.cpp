//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformImplementAllShortestPath.cpp
//
//	@doc:
//		Implementation of AllShortestPath operator
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementAllShortestPath.h"

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalAllShortestPath.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalAllShortestPath.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAllShortestPath::CXformImplementAllShortestPath
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementAllShortestPath::CXformImplementAllShortestPath(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalAllShortestPath(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

CXform::EXformPromise
CXformImplementAllShortestPath::Exfp(CExpressionHandle &exprhdl) const
{
	// Although it is valid SQL for the limit/offset to be a subquery, Orca does
	// not support it
	(void) exprhdl;
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementAllShortestPath::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementAllShortestPath::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
								CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalAllShortestPath *popAllShortestPath = CLogicalAllShortestPath::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all components
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// assemble physical operator
	CExpression *pexprAllShortestPath = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalAllShortestPath(mp, popAllShortestPath->PnameAlias(),
			popAllShortestPath->PtabdescArray(), 
			popAllShortestPath->PcrSource(), 
			popAllShortestPath->PcrDestination(),
			popAllShortestPath->PathLowerBound(),
			popAllShortestPath->PathUpperBound()), 
			pexprRelational, pexprScalar);

	// add alternative to results
	pxfres->Add(pexprAllShortestPath);
}


// EOF
