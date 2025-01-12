//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDP.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDPCoalescing_H
#define GPOPT_CXformExpandNAryJoinDPCoalescing_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformExpandNAryJoinDPCoalescing
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//
//---------------------------------------------------------------------------
class CXformExpandNAryJoinDPCoalescing : public CXformExploration
{
private:
	// private copy ctor
	CXformExpandNAryJoinDPCoalescing(const CXformExpandNAryJoinDPCoalescing &);

public:
	// ctor
	explicit CXformExpandNAryJoinDPCoalescing(CMemoryPool *mp);

	// dtor
	virtual ~CXformExpandNAryJoinDPCoalescing()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfExpandNAryJoinDPCoalescing;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformExpandNAryJoinDPCoalescing";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// do stats need to be computed before applying xform?
	virtual BOOL
	FNeedsStats() const
	{
		return true;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformExpandNAryJoinDPCoalescing

}  // namespace gpopt


#endif	// !GPOPT_CXformExpandNAryJoinDPCoalescing_H

// EOF
