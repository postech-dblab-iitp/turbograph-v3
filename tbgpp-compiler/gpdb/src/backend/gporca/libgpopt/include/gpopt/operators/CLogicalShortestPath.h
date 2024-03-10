//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalShortestPath.h
//
//	@doc:
//		Shortest Path Operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalShortestPath_H
#define GPOS_CLogicalShortestPath_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalShortestPath
//
//	@doc:
//		Shortest Path Operator
//
//---------------------------------------------------------------------------
class CLogicalShortestPath : public CLogicalUnary
{
private:
	// private copy ctor
	CLogicalShortestPath(const CLogicalShortestPath &);

public:
	// ctor
	explicit CLogicalShortestPath(CMemoryPool *mp);

	// ctor
	CLogicalShortestPath(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptorArray *ptabdescArray,
				CColRef *srccr, 
				CColRef *destcr);

	// dtor
	virtual ~CLogicalShortestPath();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalShortestPath;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalShortestPath";
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
	static CLogicalShortestPath *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalShortestPath == pop->Eopid());

		return reinterpret_cast<CLogicalShortestPath *>(pop);
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

};	// class CLogicalShortestPath

}  // namespace gpopt


#endif	// !GPOS_CLogicalShortestPath_H

// EOF
