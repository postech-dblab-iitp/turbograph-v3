//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformProject2ComputeScalarColumnar.h
//
//	@doc:
//		Transform Project to ComputeScalar
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformProject2ComputeScalarColumnar_H
#define GPOPT_CXformProject2ComputeScalarColumnar_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformProject2ComputeScalarColumnar
//
//	@doc:
//		Transform Project to ComputeScalarColumnar
//
//---------------------------------------------------------------------------
class CXformProject2ComputeScalarColumnar : public CXformImplementation
{
private:
	// private copy ctor
	CXformProject2ComputeScalarColumnar(const CXformProject2ComputeScalarColumnar &);

public:
	// ctor
	explicit CXformProject2ComputeScalarColumnar(CMemoryPool *mp);

	// dtor
	virtual ~CXformProject2ComputeScalarColumnar()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfProject2ComputeScalarColumnar;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformProject2ComputeScalarColumnar";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &exprhdl) const
	{
		if (exprhdl.DeriveHasSubquery(1))
		{
			return CXform::ExfpNone;
		}

		return CXform::ExfpHigh;
	}

	// actual transform
	virtual void Transform(CXformContext *, CXformResult *,
						   CExpression *) const;

};	// class CXformProject2ComputeScalarColumnar

}  // namespace gpopt

#endif	// !GPOPT_CXformProject2ComputeScalarColumnar_H

// EOF
