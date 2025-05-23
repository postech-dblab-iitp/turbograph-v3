//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2023 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformPushJoinBelowRightUnionAll.h
//
//	@doc:
//		Push join below right union all transform
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushJoinBelowRightUnionAll_H
#define GPOPT_CXformPushJoinBelowRightUnionAll_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPatternNode.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/xforms/CXformPushJoinBelowUnionAll.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushJoinBelowRightUnionAll
//
//	@doc:
//		Push join below union right all transform
//
//---------------------------------------------------------------------------
class CXformPushJoinBelowRightUnionAll : public CXformPushJoinBelowUnionAll
{
private:
	CXformPushJoinBelowRightUnionAll(const CXformPushJoinBelowRightUnionAll &);

public:
	// ctor
	explicit CXformPushJoinBelowRightUnionAll(CMemoryPool *mp)
		: CXformPushJoinBelowUnionAll(

			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternTree(mp)),  // outer child
				  GPOS_NEW(mp)
					  CExpression  // inner child is a union all operation
				  (mp, GPOS_NEW(mp) CLogicalUnionAll(mp),
				   GPOS_NEW(mp)
					   CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))),
				  GPOS_NEW(mp)
					  CExpression(mp,
								  GPOS_NEW(mp) CPatternTree(mp)))  // predicate
		  )
	{
	}

	// dtor
	virtual ~CXformPushJoinBelowRightUnionAll()
	{
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformPushJoinBelowRightUnionAll";
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfPushJoinBelowRightUnionAll;
	}

};	// class CXformPushJoinBelowRightUnionAll

}  // namespace gpopt


#endif	// !GPOPT_CXformPushJoinBelowRightUnionAll_H

// EOF
