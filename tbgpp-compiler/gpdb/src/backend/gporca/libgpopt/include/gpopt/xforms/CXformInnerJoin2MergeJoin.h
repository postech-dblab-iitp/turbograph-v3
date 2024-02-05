//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformInnerJoin2MergeJoin.h
//
//	@doc:
//		Transform inner join to inner Hash Join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2MergeJoin_H
#define GPOPT_CXformInnerJoin2MergeJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2MergeJoin
//
//	@doc:
//		Transform inner join to inner Hash Join
//		Deprecated in favor of CXformImplementInnerJoin.
//
//---------------------------------------------------------------------------
class CXformInnerJoin2MergeJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformInnerJoin2MergeJoin(const CXformInnerJoin2MergeJoin &);


public:
	// ctor
	explicit CXformInnerJoin2MergeJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformInnerJoin2MergeJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2MergeJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2MergeJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformInnerJoin2MergeJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformInnerJoin2MergeJoin_H

// EOF
