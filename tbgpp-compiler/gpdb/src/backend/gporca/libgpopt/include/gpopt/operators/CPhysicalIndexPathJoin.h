//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CPhysicalIndexPathJoin.h
//
//	@doc:
//		Inner index nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalIndexPathJoin_H
#define GPOPT_CPhysicalIndexPathJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalNLJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalIndexPathJoin
//
//	@doc:
//		Inner index nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalIndexPathJoin : public CPhysicalNLJoin // S62VAR is it ok??
{
private:
	// columns from outer child used for index lookup in inner child
	CColRefArray *m_pdrgpcrOuterRefs;

	// a copy of the original join predicate that has been pushed down to the inner side
	CExpression *m_origJoinPred;

	INT path_join_lower_bound;
	INT path_join_upper_bound;

	// private copy ctor
	CPhysicalIndexPathJoin(const CPhysicalIndexPathJoin &);

public:
	// ctor
	CPhysicalIndexPathJoin(CMemoryPool *mp,
							INT path_join_lower_bound,
							INT path_join_upper_bound,
							  CColRefArray *colref_array,
							  CExpression *origJoinPred);

	// dtor
	virtual ~CPhysicalIndexPathJoin();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalIndexPathJoin;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalIndexPathJoin";
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// outer column references accessor
	CColRefArray *
	PdrgPcrOuterRefs() const
	{
		return m_pdrgpcrOuterRefs;
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

	// compute required distribution of the n-th child
	virtual CDistributionSpec *PdsRequired(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const;

	virtual CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CReqdPropPlan *prppInput, ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulDistrReq);

	// execution order of children
	virtual EChildExecOrder
	Eceo() const
	{
		// we optimize inner (right) child first to be able to match child hashed distributions
		return EceoRightToLeft;
	}

	// conversion function
	static CPhysicalIndexPathJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalIndexPathJoin == pop->Eopid());

		return dynamic_cast<CPhysicalIndexPathJoin *>(pop);
	}

	CExpression *
	OrigJoinPred()
	{
		return m_origJoinPred;
	}

};	// class CPhysicalIndexPathJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalIndexPathJoin_H

// EOF
