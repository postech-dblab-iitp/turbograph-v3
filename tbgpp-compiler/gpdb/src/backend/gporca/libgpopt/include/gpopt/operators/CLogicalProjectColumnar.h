//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalProjectColumnar.h
//
//	@doc:
//		Project operator - S62
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalProjectColumnar_H
#define GPOS_CLogicalProjectColumnar_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
// fwd declaration
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalProjectColumnar
//
//	@doc:
//		Project operator - S62 columnar
//		Unlike CLogicalProject, 
// 		This projection only fetches columns listed in the projection list.
//		
//
//---------------------------------------------------------------------------
class CLogicalProjectColumnar : public CLogicalUnary
{
private:
	// private copy ctor
	CLogicalProjectColumnar(const CLogicalProjectColumnar &);

	// return equivalence class from scalar ident project element
	static CColRefSetArray *PdrgpcrsEquivClassFromScIdent(
		CMemoryPool *mp, CExpression *pexprPrEl, CColRefSet *not_null_columns);

	// extract constraint from scalar constant project element
	static void ExtractConstraintFromScConst(CMemoryPool *mp,
											 CExpression *pexprPrEl,
											 CConstraintArray *pdrgpcnstr,
											 CColRefSetArray *pdrgpcrs);

public:
	// ctor
	explicit CLogicalProjectColumnar(CMemoryPool *mp);

	// dtor
	virtual ~CLogicalProjectColumnar()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalProjectColumnar;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalProject_";
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// dervive keys
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *DerivePropertyConstraint(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalProjectColumnar *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalProjectColumnar == pop->Eopid());

		return dynamic_cast<CLogicalProjectColumnar *>(pop);
	}

};	// class CLogicalProjectColumnar

}  // namespace gpopt

#endif	// !GPOS_CLogicalProjectColumnar_H

// EOF
