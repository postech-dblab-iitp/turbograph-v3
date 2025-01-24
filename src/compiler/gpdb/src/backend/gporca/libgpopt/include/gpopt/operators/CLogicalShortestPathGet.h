//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalShortestPathGet.h
//
//	@doc:
//		Basic table accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalShortestPathGet_H
#define GPOPT_CLogicalShortestPathGet_H

#include "gpos/base.h"

#include "gpopt/operators/CLogical.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalShortestPathGet
//
//	@doc:
//		Basic table accessor
//
//---------------------------------------------------------------------------
class CLogicalShortestPathGet : public CLogical
{
private:
	// alias for table
	const CName *m_pnameAlias;

	// table descriptor array
	CTableDescriptorArray *m_ptabdescArray;

	// output columns - is not ColRefTable. is a separate column
	CColRefArray *m_pdrgpcrOutput;

	// partition keys
	CColRef2dArray *m_pdrgpdrgpcrPart;

	// distribution columns (empty for master only tables)
	CColRefSet *m_pcrsDist;

	INT path_join_lower_bound;
	INT path_join_upper_bound;

	void CreatePartCols(CMemoryPool *mp, const ULongPtrArray *pdrgpulPart);

	// private copy ctor
	CLogicalShortestPathGet(const CLogicalShortestPathGet &);

public:
	// ctors
	explicit CLogicalShortestPathGet(CMemoryPool *mp);

	CLogicalShortestPathGet(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptorArray *ptabdescArray,
				CColRefArray *pdrgpcrOutput,
				INT path_join_lower_bound,
				INT path_join_upper_bound
				);

	// dtor
	virtual ~CLogicalShortestPathGet();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalShortestPathGet;
	}

	// distribution columns
	virtual const CColRefSet *
	PcrsDist() const
	{
		return m_pcrsDist;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalShortestPathGet";
	}

	// accessors
	CColRefArray *
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// return table's name
	const CName &
	Name() const
	{
		return *m_pnameAlias;
	}

	// return table's descriptor
	CTableDescriptorArray *
	PtabdescArray() const
	{
		return m_ptabdescArray;
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


	// partition columns
	CColRef2dArray *
	PdrgpdrgpcrPartColumns() const
	{
		return m_pdrgpdrgpcrPart;
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

	// derive not nullable output columns
	virtual CColRefSet *DeriveNotNullColumns(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const;

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *mp,
						CExpressionHandle &	 // exprhdl
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
		//return PpcDeriveConstraintFromTable(mp, m_ptabdesc, m_pdrgpcrOutput);

		// no property
		return GPOS_NEW(mp) CPropConstraint(
			mp, GPOS_NEW(mp) CColRefSetArray(mp), NULL /*pcnstr*/);
	}

	// derive join depth
	virtual ULONG
	DeriveJoinDepth(CMemoryPool *,		 // mp
					CExpressionHandle &	 // exprhdl
	) const
	{
		return 1;
	}

	// derive table descriptor
	// virtual CTableDescriptor *
	// DeriveTableDescriptor(CMemoryPool *,	   // mp
	// 					  CExpressionHandle &  // exprhdl
	// ) const
	// {
	// 	return m_ptabdesc;
	// }

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp,
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   // pcrsInput
			 ULONG				   // child_index
	) const
	{
		GPOS_ASSERT(!"CLogicalShortestPathGet has no children");
		return NULL;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// derive key collections
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalShortestPathGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalShortestPathGet == pop->Eopid() );

		return dynamic_cast<CLogicalShortestPathGet *>(pop);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalShortestPathGet

}  // namespace gpopt


#endif	// !GPOPT_CLogicalShortestPathGet_H

// EOF
