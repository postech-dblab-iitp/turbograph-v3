//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformImplemenShortestPath.h
//
//	@doc:
//		Transform Logical into Physical ShortestPath
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementShortestPath_H
#define GPOPT_CXformImplementShortestPath_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplemenShortestPath
//
//	@doc:
//		Transform Logical into Physical ShortestPath
//
//---------------------------------------------------------------------------
class CXformImplementShortestPath : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementShortestPath(const CXformImplementShortestPath &);

public:
	// ctor
	explicit CXformImplementShortestPath(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementShortestPath()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementShortestPath;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementShortestPath";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformImplementShortestPath

}  // namespace gpopt

#endif	// !CXformImplementShortestPath

// EOF
