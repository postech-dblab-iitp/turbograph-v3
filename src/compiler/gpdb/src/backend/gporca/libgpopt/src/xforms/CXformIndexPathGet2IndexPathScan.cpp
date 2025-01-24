//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformIndexPathGet2IndexPathScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformIndexPathGet2IndexPathScan.h"

#include "gpos/base.h"

#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalIndexPathGet.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalIndexPathScan.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformIndexPathGet2IndexPathScan::CXformIndexPathGet2IndexPathScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformIndexPathGet2IndexPathScan::CXformIndexPathGet2IndexPathScan(CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalIndexPathGet(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // index lookup predicate
		  ))
{
}

CXform::EXformPromise
CXformIndexPathGet2IndexPathScan::Exfp(CExpressionHandle &) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformIndexPathGet2IndexPathScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformIndexPathGet2IndexPathScan::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalIndexPathGet *pop = CLogicalIndexPathGet::PopConvert(pexpr->Pop());
	CMemoryPool *mp = pxfctxt->Pmp();
	CIndexDescriptorArray *pindexdesc = pop->Pindexdesc();
	CTableDescriptorArray *ptabdesc = pop->Ptabdesc();

	// // extract components
	CExpression *pexprIndexCond = (*pexpr)[0];
	// if (pexprIndexCond->DeriveHasSubquery())
	// {
	// 	return;
	// }

	pindexdesc->AddRef();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = pop->PdrgpcrOutput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	COrderSpec *pos = pop->Pos();
	GPOS_ASSERT(NULL != pos);
	pos->AddRef();

	// addref all children
	pexprIndexCond->AddRef();

	CExpression *pexprAlt = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CPhysicalIndexPathScan(
			mp,
			pindexdesc,
			ptabdesc,
			pexpr->Pop()->UlOpId(),
			GPOS_NEW(mp) CName(mp, pop->NameAlias()),
			pdrgpcrOutput,
			pos,
			pop->LowerBound(),
			pop->UpperBound()
			),
		pexprIndexCond);
	pxfres->Add(pexprAlt);
}


// EOF
