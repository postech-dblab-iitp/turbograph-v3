//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Greenplum, Inc.
//
//	@filename:
//		CPhysicalIndexPathJoin.cpp
//
//	@doc:
//		Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalIndexPathJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathJoin::CPhysicalIndexPathJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalIndexPathJoin::CPhysicalIndexPathJoin(CMemoryPool *mp,
													 INT path_join_lower_bound,
													 INT path_join_upper_bound,
													 CColRefArray *colref_array,
													 CExpression *origJoinPred)
	: CPhysicalNLJoin(mp),	
	  m_pdrgpcrOuterRefs(colref_array),
	  m_origJoinPred(origJoinPred),
	  path_join_lower_bound(path_join_lower_bound),
	  path_join_upper_bound(path_join_upper_bound)
{
	GPOS_ASSERT(NULL != colref_array);
	if (NULL != origJoinPred)
	{
		origJoinPred->AddRef();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathJoin::~CPhysicalIndexPathJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalIndexPathJoin::~CPhysicalIndexPathJoin()
{
	m_pdrgpcrOuterRefs->Release();
	CRefCount::SafeRelease(m_origJoinPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathJoin::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalIndexPathJoin::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(
			CPhysicalIndexPathJoin::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalIndexPathJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalIndexPathJoin::PdsRequired(CMemoryPool * /*mp*/,
									   CExpressionHandle & /*exprhdl*/,
									   CDistributionSpec *,	 //pdsRequired,
									   ULONG /*child_index*/,
									   CDrvdPropArray * /*pdrgpdpCtxt*/,
									   ULONG  // ulOptReq
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalIndexPathJoin"));
	return NULL;
}

CEnfdDistribution *
CPhysicalIndexPathJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CReqdPropPlan *prppInput, ULONG child_index,
							   CDrvdPropArray *pdrgpdpCtxt, ULONG ulDistrReq)
{
	GPOS_ASSERT(2 > child_index);

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulDistrReq);

	if (1 == child_index)
	{
		// inner (index-scan side) is requested for Any distribution,
		// we allow outer references on the inner child of the join since it needs
		// to refer to columns in join's outer child
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/),
			dmatch);
	}

	// we need to match distribution of inner
	CDistributionSpec *pdsInner =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		// enforce executing on a single host
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecSingleton(), dmatch);
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		// check if we could create an equivalent hashed distribution request to the inner child
		CDistributionSpecHashed *pdshashed =
			CDistributionSpecHashed::PdsConvert(pdsInner);
		CDistributionSpecHashed *pdshashedEquiv = pdshashed->PdshashedEquiv();

		// If the inner child is a IndexScan on a multi-key distributed index, it
		// may derive an incomplete equiv spec (see CPhysicalScan::PdsDerive()).
		// However, there is no point to using that here since there will be no
		// operator above this that can complete it.
		if (pdshashed->HasCompleteEquivSpec(mp))
		{
			// request hashed distribution from outer
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			CDistributionSpecHashed *pdsHashedRequired = GPOS_NEW(mp)
				CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(),
										pdshashedEquiv->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);

			return GPOS_NEW(mp) CEnfdDistribution(pdsHashedRequired, dmatch);
		}
	}

	// otherwise, require outer child to be replicated
	// The match type for this request has to be "Satisfy" since EdtReplicated
	// is required only property. Since a Broadcast motion will always
	// derive a EdtStrictReplicated distribution spec, it will never "Match"
	// the required distribution spec and hence will not be optimized.
	return GPOS_NEW(mp) CEnfdDistribution(
		GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
		CEnfdDistribution::EdmSatisfy);
}


// EOF
