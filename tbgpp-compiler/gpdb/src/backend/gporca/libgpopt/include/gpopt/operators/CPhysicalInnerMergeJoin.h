//	Greenplum Database
//	Copyright (C) 2019 Pivotal Software, Inc.

#ifndef GPOPT_CPhysicalInnerMergeJoin_H
#define GPOPT_CPhysicalInnerMergeJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalJoin.h"

namespace gpopt
{
class CPhysicalInnerMergeJoin : public CPhysicalJoin
{
private:
	// private copy ctor
	CPhysicalInnerMergeJoin(const CPhysicalInnerMergeJoin &);

	CExpressionArray *m_outer_merge_clauses;

	CExpressionArray *m_inner_merge_clauses;

public:
	// ctor
	explicit CPhysicalInnerMergeJoin(
		CMemoryPool *mp, CExpressionArray *outer_merge_clauses,
		CExpressionArray *inner_merge_clauses,
		IMdIdArray *hash_opfamilies = NULL, BOOL is_null_aware = true,
		CXform::EXformId order_origin_xform = CXform::ExfSentinel);

	// dtor
	virtual ~CPhysicalInnerMergeJoin();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalInnerMergeJoin;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalInnerMergeJoin";
	}

	// conversion function
	static CPhysicalInnerMergeJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalInnerMergeJoin == pop->Eopid());

		return dynamic_cast<CPhysicalInnerMergeJoin *>(pop);
	}

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

	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posInput, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	// compute required rewindability of the n-th child
	virtual CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											CRewindabilitySpec *prsRequired,
											ULONG child_index,
											CDrvdPropArray *pdrgpdpCtxt,
											ULONG ulOptReq) const;

	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	virtual CEnfdDistribution::EDistributionMatching Edm(
		CReqdPropPlan *,   // prppInput
		ULONG,			   //child_index,
		CDrvdPropArray *,  // pdrgpdpCtxt,
		ULONG			   // ulOptReq
	);

	virtual CDistributionSpec *PdsDerive(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const;

};	// class CPhysicalInnerMergeJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalInnerMergeJoin_H

// EOF
