//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform Inner/Outer Join to Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPathJoin2IndexPathApply_H
#define GPOPT_CXformPathJoin2IndexPathApply_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalApply.h"
#include "gpopt/operators/CLogicalJoin.h"
#include "gpopt/xforms/CXformExploration.h"

#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/CLogicalIndexPathApply.h"
#include "gpopt/operators/CLogicalPathJoin.h"

#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternNode.h"

namespace gpopt
{
using namespace gpos;

class CXformPathJoin2IndexPathApply : public CXformExploration
{
private:
	// private copy ctor
	CXformPathJoin2IndexPathApply(const CXformPathJoin2IndexPathApply &);

protected:
	// is the logical join that is being transformed an outer join?
	BOOL m_fOuterJoin;

public:
	// ctor
	explicit CXformPathJoin2IndexPathApply(CMemoryPool *mp)
		: 
			CXformExploration(GPOS_NEW(mp) CExpression(
				mp,
				GPOS_NEW(mp) CLogicalIndexPathApply(mp),
				GPOS_NEW(mp) CExpression(
					mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // outer child
				GPOS_NEW(mp) CExpression(
					mp, GPOS_NEW(mp) CPatternTree(mp)),  // inner child
				GPOS_NEW(mp) CExpression(
					mp, GPOS_NEW(mp) CPatternTree(mp))	 // predicate tree operator,
				)
			)
	{
			m_fOuterJoin = false; // current not support 
	}

	// dtor
	virtual ~CXformPathJoin2IndexPathApply() {
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	void ComputeColumnSets(CMemoryPool *mp, CExpression *pexprInner,
						   CExpression *pexprScalar,
						   CColRefSet **ppcrsScalarExpr,
						   CColRefSet **ppcrsOuterRefs,
						   CColRefSet **ppcrsReqd) const;

	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfPathJoin2IndexPathApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformPathJoin2IndexPathApply";
	}



};	// class CXformPathJoin2IndexPathApply

}  // namespace gpopt

#endif	// !GPOPT_CXformPathJoin2IndexPathApply_H

// EOF
