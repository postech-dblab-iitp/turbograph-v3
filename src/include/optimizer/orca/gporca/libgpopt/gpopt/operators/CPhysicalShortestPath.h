//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalShortestPath.h
//
//	@doc:
//		Physical ShortestPath operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalShortestPath_H
#define GPOPT_CPhysicalShortestPath_H

#include "gpos/base.h"

#include "gpopt/base/COrderSpec.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalShortestPath
//
//	@doc:
//		ShortestPath operator
//
//---------------------------------------------------------------------------
class CPhysicalShortestPath : public CPhysical
{
private:
	// private copy ctor
	CPhysicalShortestPath(const CPhysicalShortestPath &);

public:
	// ctor
	CPhysicalShortestPath(CMemoryPool *mp);

	// ctor
	CPhysicalShortestPath(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptorArray *ptabdescArray,
				CColRef *srccr, 
				CColRef *destcr,
				INT path_lower_bound,
				INT path_upper_bound);

	// dtor
	virtual ~CPhysicalShortestPath();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalShortestPath;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalShortestPath";
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	// alias accessor
	const CName *
	PnameAlias() const
	{
		return m_pnameAlias;
	}

	// table descriptor accessor
	CTableDescriptorArray *
	PtabdescArray() const
	{
		return m_ptabdescArray;
	}

	// source ID column reference accessor
	CColRef*
	PcrSource() const
	{
		return m_srccr;
	}

	// destination ID column reference accessor
	CColRef*
	PcrDestination() const
	{
		return m_destcr;
	}

	// lower bound
	INT
	PathLowerBound() const
	{
		return m_path_lower_bound;
	}

	// upper bound
	INT
	PathUpperBound() const
	{
		return m_path_upper_bound;
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	virtual CColRefSet *PcrsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
		ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	// compute required ctes of the n-th child
	virtual CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CCTEReq *pcter, ULONG child_index,
								  CDrvdPropArray *pdrgpdpCtxt,
								  ULONG ulOptReq) const;

	// compute required sort order of the n-th child
	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posRequired, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	// compute required distribution of the n-th child
	virtual CDistributionSpec *PdsRequired(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const;

	// compute required rewindability of the n-th child
	virtual CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											CRewindabilitySpec *prsRequired,
											ULONG child_index,
											CDrvdPropArray *pdrgpdpCtxt,
											ULONG ulOptReq) const;

	// compute required partition propagation of the n-th child
	virtual CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	// check if required columns are included in output columns
	virtual BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired,
								   ULONG ulOptReq) const;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	virtual COrderSpec *PosDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const;

	// derive distribution
	virtual CDistributionSpec *PdsDerive(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const;

	// derive rewindability
	virtual CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
										  CExpressionHandle &exprhdl) const;

	// derive partition index map
	virtual CPartIndexMap *
	PpimDerive(CMemoryPool *,  // mp
			   CExpressionHandle &exprhdl,
			   CDrvdPropCtxt *	//pdpctxt
	) const
	{
		return PpimPassThruOuter(exprhdl);
	}

	// derive partition filter map
	virtual CPartFilterMap *
	PpfmDerive(CMemoryPool *,  // mp
			   CExpressionHandle &exprhdl) const
	{
		return PpfmPassThruOuter(exprhdl);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	// return rewindability property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &,		// exprhdl
		const CEnfdRewindability *	// per
	) const;

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	virtual BOOL
	FPassThruStats() const
	{
		return false;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// print
	virtual IOstream &OsPrint(IOstream &) const;

	// conversion function
	static CPhysicalShortestPath *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalShortestPath == pop->Eopid());

		return dynamic_cast<CPhysicalShortestPath *>(pop);
	}

private:
	// alias for table
	const CName *m_pnameAlias;

	// table descriptor array
	CTableDescriptorArray *m_ptabdescArray;

	// source ID column reference
	CColRef *m_srccr;

	// destination ID column reference
	CColRef *m_destcr;

	// Bounds
	INT m_path_lower_bound;
	INT m_path_upper_bound;

};	// class CPhysicalShortestPath

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalShortestPath_H

// EOF
