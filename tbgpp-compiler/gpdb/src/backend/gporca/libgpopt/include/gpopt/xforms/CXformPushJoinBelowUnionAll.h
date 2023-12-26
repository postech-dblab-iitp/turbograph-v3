//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformPushJoinBelowUnionAll.h
//
//	@doc:
//		Push join below UnionAll operation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushJoinBelowUnionAll_H
#define GPOPT_CXformPushJoinBelowUnionAll_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalUnionAll.h"
// #include "gpopt/xforms/CXformPushGbBelowSetOp.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushJoinBelowUnionAll
//
//	@doc:
//		Push grouping below UnionAll operation
//
//---------------------------------------------------------------------------
class CXformPushJoinBelowUnionAll : public CXformExploration
{
private:
	// private copy ctor
	CXformPushJoinBelowUnionAll(const CXformPushJoinBelowUnionAll &);

public:
	// ctor
	explicit CXformPushJoinBelowUnionAll(CMemoryPool *mp);

	// dtor
	virtual ~CXformPushJoinBelowUnionAll()
	{
	}

	// // Compatibility function
	// virtual BOOL
	// FCompatible(CXform::EXformId exfid)
	// {
	// 	return ExfPushGbBelowUnionAll != exfid;
	// }

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		// return ExfPushJoinBelowUnionAll;
		return ExfPushGbBelowUnionAll; // TODO temporary
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformPushJoinBelowUnionAll";
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformPushJoinBelowUnionAll

}  // namespace gpopt

#endif	// !GPOPT_CXformPushJoinBelowUnionAll_H

// EOF
