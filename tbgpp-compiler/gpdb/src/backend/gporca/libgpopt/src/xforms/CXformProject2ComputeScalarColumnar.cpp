//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformProject2ComputeScalarColumnarColumnar.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformProject2ComputeScalarColumnar.h"

#include "gpos/base.h"

//#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalProjectColumnar.h"
#include "gpopt/operators/CPatternLeaf.h"
//#include "gpopt/operators/CPhysicalComputeScalar.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformProject2ComputeScalarColumnar::CXformProject2ComputeScalarColumnar
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformProject2ComputeScalarColumnar::CXformProject2ComputeScalarColumnar(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // scalar child
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformProject2ComputeScalarColumnar::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformProject2ComputeScalarColumnar::Transform(CXformContext *pxfctxt,
									   CXformResult *pxfres,
									   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// assemble physical operator
	CExpression *pexprComputeScalar =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPhysicalComputeScalarColumnar(mp),
								 pexprRelational, pexprScalar);

	// add alternative to results
	pxfres->Add(pexprComputeScalar);
}


// EOF
