//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalPathJoin.h
//
//	@doc:
//		Inner join operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalPathJoin_H
#define GPOS_CLogicalPathJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalJoin.h"

namespace gpopt
{
// fwd declaration
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalPathJoin
//
//	@doc:
//		Inner join operator
//
//---------------------------------------------------------------------------
class CLogicalPathJoin : public CLogicalJoin
{
private:
	// private copy ctor
	CLogicalPathJoin(const CLogicalPathJoin &);

	INT path_join_lower_bound;
	INT path_join_upper_bound;

public:
	// ctor
	explicit CLogicalPathJoin(
		CMemoryPool *mp,
		INT path_join_lower_bound,
		INT path_join_upper_bound,
		CXform::EXformId origin_xform = CXform::ExfSentinel);

	// dtor
	virtual ~CLogicalPathJoin()
	{
	}


	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalPathJoin;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalPathJoin";
	}

	ULONG
	LowerBound()
	{
		return path_join_lower_bound;
	}

	ULONG
	UpperBound()
	{
		return path_join_upper_bound;
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive not nullable columns
	virtual CColRefSet *
	DeriveNotNullColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PcrsDeriveNotNullCombineLogical(mp, exprhdl);
	}

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintFromPredicates(mp, exprhdl);
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalPathJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalPathJoin == pop->Eopid());

		return dynamic_cast<CLogicalPathJoin *>(pop);
	}

	// determine if an innerJoin group expression has
	// less conjuncts than another
	static BOOL FFewerConj(CMemoryPool *mp, CGroupExpression *pgexprFst,
						   CGroupExpression *pgexprSnd);


};	// class CLogicalPathJoin

}  // namespace gpopt


#endif	// !GPOS_CLogicalPathJoin_H

// EOF
