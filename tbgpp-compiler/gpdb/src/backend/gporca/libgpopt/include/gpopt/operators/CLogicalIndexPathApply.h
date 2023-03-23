//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	Base Index Apply operator for Inner and Outer Join;
//	a variant of inner/outer apply that captures the need to implement a
//	correlated-execution strategy on the physical side, where the inner
//	side is an index scan with parameters from outer side
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIndexPathApply_H
#define GPOPT_CLogicalIndexPathApply_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalApply.h"

namespace gpopt
{
class CLogicalIndexPathApply : public CLogicalApply
{
private:
	// private copy ctor
	CLogicalIndexPathApply(const CLogicalIndexPathApply &);


protected:
	// columns used from Apply's outer child used by index in Apply's inner child
	CColRefArray *m_pdrgpcrOuterRefs;

	// is this an outer join?
	BOOL m_fOuterJoin;

	// a copy of the original join predicate that has been pushed down to the inner side
	CExpression *m_origJoinPred;

	INT path_join_lower_bound;
	INT path_join_upper_bound;

public:
	// ctor
	CLogicalIndexPathApply(CMemoryPool *mp,
						INT path_join_lower_bound,
						INT path_join_upper_bound,
						CColRefArray *pdrgpcrOuterRefs,
					   BOOL fOuterJoin, CExpression *origJoinPred);

	// ctor for patterns
	explicit CLogicalIndexPathApply(CMemoryPool *mp);

	// dtor
	virtual ~CLogicalIndexPathApply();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalIndexPathApply;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalIndexPathApply";
	}

	// outer column references accessor
	CColRefArray *
	PdrgPcrOuterRefs() const
	{
		return m_pdrgpcrOuterRefs;
	}

	// outer column references accessor
	BOOL
	FouterJoin() const
	{
		return m_fOuterJoin;
	}

	CExpression *
	OrigJoinPred()
	{
		return m_origJoinPred;
	}

	INT
	LowerBound()
	{
		return path_join_lower_bound;
	}

	INT
	UpperBound()
	{
		return path_join_upper_bound;
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *
	DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl)
	{
		GPOS_ASSERT(3 == exprhdl.Arity());

		return PcrsDeriveOutputCombineLogical(mp, exprhdl);
	}

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

	// applicable transformations
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// match function
	virtual BOOL Matches(COperator *pop) const;

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &	 // exprhdl
	) const
	{
		return CLogical::EspMedium;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// conversion function
	static CLogicalIndexPathApply *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalIndexPathApply == pop->Eopid());

		return dynamic_cast<CLogicalIndexPathApply *>(pop);
	}

};	// class CLogicalIndexPathApply

}  // namespace gpopt


#endif	// !GPOPT_CLogicalIndexPathApply_H

// EOF
