//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CXformCollapseProjectColumnar.h
//
//	@doc:
//		Transform that collapses two cascaded project nodes
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformCollapseProjectColumnar_H
#define GPOPT_CXformCollapseProjectColumnar_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformSubqueryUnnest.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformCollapseProjectColumnar
//
//	@doc:
//		Transform that collapses two cascaded project nodes
//
//---------------------------------------------------------------------------
class CXformCollapseProjectColumnar : public CXformExploration
{
private:
	// private copy ctor
	CXformCollapseProjectColumnar(const CXformCollapseProjectColumnar &);

public:
	// ctor
	explicit CXformCollapseProjectColumnar(CMemoryPool *mp);

	// dtor
	virtual ~CXformCollapseProjectColumnar()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfCollapseProjectColumnar;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformCollapseProjectColumnar";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformCollapseProjectColumnar

}  // namespace gpopt

#endif	// !GPOPT_CXformCollapseProjectColumnar_H

// EOF
