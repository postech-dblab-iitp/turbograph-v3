//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIndexPathGet.h
//
//	@doc:
//		Basic index accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIndexPathGet_H
#define GPOPT_CLogicalIndexPathGet_H

#include "gpos/base.h"

#include "gpopt/base/COrderSpec.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CLogical.h"

#include <vector>

namespace gpopt
{
// fwd declarations
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalIndexPathGet
//
//	@doc:
//		Basic index accessor
//
//---------------------------------------------------------------------------
class CLogicalIndexPathGet : public CLogical
{
private:
	// index descriptor
	CIndexDescriptorArray *m_pindexdesc;

	// table descriptor
	CTableDescriptorArray *m_ptabdesc;

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG m_ulOriginOpId;

	// alias for table
	const CName *m_pnameAlias;

	// output columns
	CColRefArray *m_pdrgpcrOutput;

	// set representation of output columns
	CColRefSet *m_pcrsOutput;

	// order spec
	COrderSpec *m_pos;

	// distribution columns (empty for master only tables)
	CColRefSet *m_pcrsDist;

	INT path_join_lower_bound;
	INT path_join_upper_bound;

	// private copy ctor
	CLogicalIndexPathGet(const CLogicalIndexPathGet &);

public:
	// ctors
	explicit CLogicalIndexPathGet(CMemoryPool *mp);

	CLogicalIndexPathGet(CMemoryPool *mp,
					const std::vector<const IMDIndex *> pmdindexArray,
					 CTableDescriptorArray *ptabdescArray,
					 ULONG ulOriginOpId,
					 const CName *pnameAlias,
					 CColRefArray *pdrgpcrOutput,
					 INT path_join_lower_bound,
				 	 INT path_join_upper_bound
					 );

	// dtor
	virtual ~CLogicalIndexPathGet();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalIndexPathGet;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalIndexPathGet";
	}

	// distribution columns
	virtual const CColRefSet *
	PcrsDist() const
	{
		return m_pcrsDist;
	}

	// array of output columns
	CColRefArray *
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG
	UlOriginOpId() const
	{
		return m_ulOriginOpId;
	}

	// index name
	// const CName &
	// Name() const
	// {
	// 	return m_pindexdesc->Name();
	// }

	// table alias name
	const CName &
	NameAlias() const
	{
		return *m_pnameAlias;
	}

	// index descriptor
	CIndexDescriptorArray *
	Pindexdesc() const
	{
		return m_pindexdesc;
	}

	// table descriptor
	CTableDescriptorArray *
	Ptabdesc() const
	{
		return m_ptabdesc;
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

	// order spec
	COrderSpec *
	Pos() const
	{
		return m_pos;
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// derive outer references
	virtual CColRefSet *DeriveOuterReferences(CMemoryPool *mp,
											  CExpressionHandle &exprhdl);

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *mp,
						CExpressionHandle &	 //exprhdl
	) const
	{
		return GPOS_NEW(mp) CPartInfo(mp);
	}

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp,
							 CExpressionHandle &  // exprhdl
	) const
	{
		// no property
		return GPOS_NEW(mp) CPropConstraint(
			mp, GPOS_NEW(mp) CColRefSetArray(mp), NULL /*pcnstr*/);
	}

	// derive key collections
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive join depth
	virtual ULONG
	DeriveJoinDepth(CMemoryPool *,		 // mp
					CExpressionHandle &	 // exprhdl
	) const
	{
		return path_join_upper_bound - path_join_lower_bound + 1;
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *mp,
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   //pcrsInput
			 ULONG				   // child_index
	) const
	{
		// TODO:  March 26 2012; statistics derivation for indexes
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspLow;
	}

	//-------------------------------------------------------------------------------------
	// conversion function
	//-------------------------------------------------------------------------------------

	static CLogicalIndexPathGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalIndexPathGet == pop->Eopid());

		return dynamic_cast<CLogicalIndexPathGet *>(pop);
	}


	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalIndexPathGet

}  // namespace gpopt

#endif	// !GPOPT_CLogicalIndexPathGet_H

// EOF
