//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalAllShortestPath.h
//
//	@doc:
//		Shortest Path Operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalAllShortestPath_H
#define GPOS_CLogicalAllShortestPath_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalAllShortestPath
//
//	@doc:
//		Shortest Path Operator
//
//---------------------------------------------------------------------------
class CLogicalAllShortestPath : public CLogicalUnary
{
private:
	// private copy ctor
	CLogicalAllShortestPath(const CLogicalAllShortestPath &);

public:
	// ctor
	explicit CLogicalAllShortestPath(CMemoryPool *mp);

	// ctor
	CLogicalAllShortestPath(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptorArray *ptabdescArray,
				CColRef *srccr, 
				CColRef *destcr,
				INT path_join_lower_bound,
				INT path_join_upper_bound);

	// dtor
	virtual ~CLogicalAllShortestPath();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalAllShortestPath;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalAllShortestPath";
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

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
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *, CExpressionHandle &);

	// derive outer references
	virtual CColRefSet *DeriveOuterReferences(CMemoryPool *mp,
											  CExpressionHandle &exprhdl);

	// derive not null columns
	virtual CColRefSet *DeriveNotNullColumns(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const;


	// derive constraint property
	virtual CPropConstraint *DerivePropertyConstraint(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CColRefSet *pcrsInput,
								 ULONG child_index) const;

	//-------------------------------------------------------------------------------------
	// Derived Stats
	//-------------------------------------------------------------------------------------

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalAllShortestPath *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalAllShortestPath == pop->Eopid());

		return reinterpret_cast<CLogicalAllShortestPath *>(pop);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &os) const;

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

};	// class CLogicalAllShortestPath

}  // namespace gpopt


#endif	// !GPOS_CLogicalAllShortestPath_H

// EOF
