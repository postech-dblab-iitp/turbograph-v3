//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software, Inc.
//
//	Template Class for Inner / Left Outer Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementIndexPathApply_H
#define GPOPT_CXformImplementIndexPathApply_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalIndexPathApply.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPhysicalIndexPathJoin.h"
#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformImplementIndexPathApply : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementIndexPathApply(const CXformImplementIndexPathApply &);

public:
	// ctor
	explicit CXformImplementIndexPathApply(CMemoryPool *mp)
		:  // pattern
		  CXformImplementation(GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalIndexPathApply(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // outer child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // inner child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
			  ))
	{
	}

	// dtor
	virtual ~CXformImplementIndexPathApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementIndexPathApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementIndexPathApply";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		if (exprhdl.DeriveHasSubquery(2))
		{
			return ExfpNone;
		}
		return ExfpHigh;
	}

	// actual transform
	virtual void
	Transform(CXformContext *pxfctxt, CXformResult *pxfres,
			  CExpression *pexpr) const
	{
		GPOS_ASSERT(NULL != pxfctxt);
		GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
		GPOS_ASSERT(FCheckPattern(pexpr));

		CMemoryPool *mp = pxfctxt->Pmp();
		CLogicalIndexPathApply *indexApply =
			CLogicalIndexPathApply::PopConvert(pexpr->Pop());

		// extract components
		CExpression *pexprOuter = (*pexpr)[0];
		CExpression *pexprInner = (*pexpr)[1];
		CExpression *pexprScalar = (*pexpr)[2];
		CColRefArray *colref_array = indexApply->PdrgPcrOuterRefs();
		colref_array->AddRef();

		// addref all components
		pexprOuter->AddRef();
		pexprInner->AddRef();
		pexprScalar->AddRef();

		// assemble physical operator
		CPhysicalNLJoin *pop = NULL;

		// Currently index path join only
		pop = GPOS_NEW(mp) CPhysicalIndexPathJoin(
			mp,
			(gpos::INT) indexApply->LowerBound(), (gpos::INT) indexApply->UpperBound(),
			colref_array, indexApply->OrigJoinPred());

		CExpression *pexprResult = GPOS_NEW(mp)
			CExpression(mp, pop, pexprOuter, pexprInner, pexprScalar);

		// add alternative to results
		pxfres->Add(pexprResult);
	}

};	// class CXformImplementIndexPathApply

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementIndexPathApply_H

// EOF
