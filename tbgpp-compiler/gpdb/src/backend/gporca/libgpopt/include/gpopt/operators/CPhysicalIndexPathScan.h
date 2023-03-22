//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CPhysicalIndexPathScan.h
//
//	@doc:
//		Base class for physical index scan operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalIndexPathScan_H
#define GPOPT_CPhysicalIndexPathScan_H

#include "gpos/base.h"

#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CPhysicalScan.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CIndexDescriptor;
class CName;
class CDistributionSpecHashed;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalIndexPathScan
//
//	@doc:
//		Base class for physical index scan operators
//
//---------------------------------------------------------------------------
class CPhysicalIndexPathScan : public CPhysical
{
private:
	// alias for table
	const CName *m_pnameAlias;

	// index descriptor
	CIndexDescriptorArray *m_pindexdesc;

	CTableDescriptorArray *m_ptabdesc;

	CColRefArray *m_pdrgpcrOutput;

	CDistributionSpec *m_pds;

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG m_ulOriginOpId;

	// order
	COrderSpec *m_pos;

	INT path_join_lower_bound;
	INT path_join_upper_bound;

	// private copy ctor
	CPhysicalIndexPathScan(const CPhysicalIndexPathScan &);

public:
	// ctors
	CPhysicalIndexPathScan(CMemoryPool *mp,
						CIndexDescriptorArray *pindexdescArray,
					   CTableDescriptorArray *ptabdescArray,
					   ULONG ulOriginOpId,
					   const CName *pnameAlias,
					   CColRefArray *colref_array,
					   COrderSpec *pos,
						INT path_join_lower_bound,
				 	    INT path_join_upper_bound
					   );

	// dtor
	virtual ~CPhysicalIndexPathScan();


	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalIndexPathScan;
	}

	// operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalIndexPathScan";
	}

	// table alias name
	const CName &
	NameAlias() const
	{
		return *m_pnameAlias;
	}

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG
	UlOriginOpId() const
	{
		return m_ulOriginOpId;
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// index descriptor
	// CIndexDescriptor *
	// Pindexdesc() const
	// {
	// 	return m_pindexdesc;
	// }

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	virtual COrderSpec *
	PosDerive(CMemoryPool *,	   //mp
			  CExpressionHandle &  //exprhdl
	) const
	{
		m_pos->AddRef();
		return m_pos;
	}

	// derive partition index map
	virtual CPartIndexMap *
	PpimDerive(CMemoryPool *mp,
			   CExpressionHandle &,	 // exprhdl
			   CDrvdPropCtxt *		 //pdpctxt
	) const
	{
		return GPOS_NEW(mp) CPartIndexMap(mp);
	}

	virtual CRewindabilitySpec *
	PrsDerive(CMemoryPool *mp,
			  CExpressionHandle &  // exprhdl
	) const
	{
		// rewindability of output is always true
		return GPOS_NEW(mp)
			CRewindabilitySpec(CRewindabilitySpec::ErtMarkRestore,
							   CRewindabilitySpec::EmhtNoMotion);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	virtual CColRefSet *
	PcrsRequired(CMemoryPool *,		   // mp
				 CExpressionHandle &,  // exprhdl
				 CColRefSet *,		   // pcrsRequired
				 ULONG,				   // child_index
				 CDrvdPropArray *,	   // pdrgpdpCtxt
				 ULONG				   // ulOptReq
	)
	{
		GPOS_ASSERT(!"CPhysicalScan has no children");
		return NULL;
	}

	// compute required ctes of the n-th child
	virtual CCTEReq *
	PcteRequired(CMemoryPool *,		   //mp,
				 CExpressionHandle &,  //exprhdl,
				 CCTEReq *,			   //pcter,
				 ULONG,				   //child_index,
				 CDrvdPropArray *,	   //pdrgpdpCtxt,
				 ULONG				   //ulOptReq
	) const
	{
		GPOS_ASSERT(!"CPhysicalScan has no children");
		return NULL;
	}

	// compute required sort columns of the n-th child
	virtual COrderSpec *
	PosRequired(CMemoryPool *,		  // mp
				CExpressionHandle &,  // exprhdl
				COrderSpec *,		  // posRequired
				ULONG,				  // child_index
				CDrvdPropArray *,	  // pdrgpdpCtxt
				ULONG				  // ulOptReq
	) const
	{
		GPOS_ASSERT(!"CPhysicalScan has no children");
		return NULL;
	}

	// compute required distribution of the n-th child
	virtual CDistributionSpec *
	PdsRequired(CMemoryPool *,		  // mp
				CExpressionHandle &,  // exprhdl
				CDistributionSpec *,  // pdsRequired
				ULONG,				  // child_index
				CDrvdPropArray *,	  // pdrgpdpCtxt
				ULONG				  // ulOptReq
	) const
	{
		GPOS_ASSERT(!"CPhysicalScan has no children");
		return NULL;
	}

	// compute required rewindability of the n-th child
	virtual CRewindabilitySpec *
	PrsRequired(CMemoryPool *,		   //mp
				CExpressionHandle &,   //exprhdl
				CRewindabilitySpec *,  //prsRequired
				ULONG,				   // child_index
				CDrvdPropArray *,	   // pdrgpdpCtxt
				ULONG				   // ulOptReq
	) const
	{
		GPOS_ASSERT(!"CPhysicalScan has no children");
		return NULL;
	}


	// compute required partition propagation of the n-th child
	virtual CPartitionPropagationSpec *
	PppsRequired(CMemoryPool *,				   //mp,
				 CExpressionHandle &,		   //exprhdl,
				 CPartitionPropagationSpec *,  //pppsRequired,
				 ULONG,						   //child_index,
				 CDrvdPropArray *,			   //pdrgpdpCtxt,
				 ULONG						   // ulOptReq
	)
	{
		GPOS_ASSERT(!"CPhysicalScan has no children");
		return NULL;
	}


	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &, const CEnfdOrder *peo) const;

	virtual CDistributionSpec *PdsDerive(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const;


	virtual BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired,
								   ULONG ulOptReq) const;



	// derive partition filter map
	virtual CPartFilterMap *
	PpfmDerive(CMemoryPool *mp,
			   CExpressionHandle &	// exprhdl
	) const
	{
		// return empty part filter map
		return GPOS_NEW(mp) CPartFilterMap(mp);
	}


	// return rewindability property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType
	EpetRewindability(CExpressionHandle &,		  // exprhdl
					  const CEnfdRewindability *  // per
	) const
	{
		// no need for enforcing rewindability on output
		return CEnfdProp::EpetUnnecessary;
	}

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	virtual BOOL
	FPassThruStats() const
	{
		return false;
	}



	// conversion function
	static CPhysicalIndexPathScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalIndexPathScan == pop->Eopid());

		return dynamic_cast<CPhysicalIndexPathScan *>(pop);
	}

	// statistics derivation during costing
	virtual IStatistics *
	PstatsDerive(CMemoryPool *,		   // mp
				 CExpressionHandle &,  // exprhdl
				 CReqdPropPlan *,	   // prpplan
				 IStatisticsArray *	   //stats_ctxt
	) const
	{
		GPOS_ASSERT(
			!"stats derivation during costing for index scan is invalid");

		return NULL;
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CPhysicalIndexPathScan

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalIndexPathScan_H

// EOF
