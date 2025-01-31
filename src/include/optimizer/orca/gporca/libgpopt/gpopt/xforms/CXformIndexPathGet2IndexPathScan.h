//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformIndexPathGet2IndexPathScan.h
//
//	@doc:
//		Transform Index Get to Index Scan
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformIndexPathGet2IndexPathScan_H
#define GPOPT_CXformIndexPathGet2IndexPathScan_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformIndexPathGet2IndexPathScan
//
//	@doc:
//		Transform Index Get to Index Scan
//
//---------------------------------------------------------------------------
class CXformIndexPathGet2IndexPathScan : public CXformImplementation
{
private:
	// private copy ctor
	CXformIndexPathGet2IndexPathScan(const CXformIndexPathGet2IndexPathScan &);

public:
	// ctor
	explicit CXformIndexPathGet2IndexPathScan(CMemoryPool *);

	// dtor
	virtual ~CXformIndexPathGet2IndexPathScan()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfIndexPathGet2IndexPathScan;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformIndexPathGet2IndexPathScan";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &	//exprhdl
	) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformIndexPathGet2IndexPathScan

}  // namespace gpopt

#endif	// !GPOPT_CXformIndexPathGet2IndexPathScan_H

// EOF
