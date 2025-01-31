//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformImplemenAllShortestPath.h
//
//	@doc:
//		Transform Logical into Physical AllShortestPath
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementAllShortestPath_H
#define GPOPT_CXformImplementAllShortestPath_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplemenAllShortestPath
//
//	@doc:
//		Transform Logical into Physical AllShortestPath
//
//---------------------------------------------------------------------------
class CXformImplementAllShortestPath : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementAllShortestPath(const CXformImplementAllShortestPath &);

public:
	// ctor
	explicit CXformImplementAllShortestPath(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementAllShortestPath()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementAllShortestPath;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementAllShortestPath";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformImplementAllShortestPath

}  // namespace gpopt

#endif	// !CXformImplementAllShortestPath

// EOF
