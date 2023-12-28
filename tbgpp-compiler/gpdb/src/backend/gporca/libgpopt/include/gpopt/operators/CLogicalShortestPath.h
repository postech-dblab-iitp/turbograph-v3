//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalShortestPath.h
//
//	@doc:
//		Shortest Path Operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalShortestPath_H
#define GPOS_CLogicalShortestPath_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalMaxOneRow.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalShortestPath
//
//	@doc:
//		Shortest Path Operator
//
//---------------------------------------------------------------------------
class CLogicalShortestPath : public CLogicalMaxOneRow
{
private:
	// private copy ctor
	CLogicalShortestPath(const CLogicalShortestPath &);

public:
	// ctor
	explicit CLogicalShortestPath(CMemoryPool *mp) : CLogicalMaxOneRow(mp)
	{
	}

	// dtor
	virtual ~CLogicalShortestPath()
	{
	}


	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalShortestPath;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalShortestPath";
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalShortestPath *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalShortestPath == pop->Eopid());

		return reinterpret_cast<CLogicalShortestPath *>(pop);
	}

};	// class CLogicalShortestPath

}  // namespace gpopt


#endif	// !GPOS_CLogicalShortestPath_H

// EOF
