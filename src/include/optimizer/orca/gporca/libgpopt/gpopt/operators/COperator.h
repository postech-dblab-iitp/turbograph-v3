//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2011 Greenplum, Inc.
//
//	@filename:
//		COperator.h
//
//	@doc:
//		Base class for all operators: logical, physical, scalar, patterns
//---------------------------------------------------------------------------
#ifndef GPOPT_COperator_H
#define GPOPT_COperator_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CFunctionProp.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/base/CReqdPropRelational.h"

namespace gpopt
{
using namespace gpos;

// forward declarations
class CExpressionHandle;
class CReqdPropPlan;
class CReqdPropRelational;

// dynamic array for operators
typedef CDynamicPtrArray<COperator, CleanupRelease> COperatorArray;

// hash map mapping CColRef -> CColRef
typedef CHashMap<CColRef, CColRef, CColRef::HashValue, CColRef::Equals,
				 CleanupNULL<CColRef>, CleanupNULL<CColRef> >
	ColRefToColRefMap;

//---------------------------------------------------------------------------
//	@class:
//		COperator
//
//	@doc:
//		base class for all operators
//
//---------------------------------------------------------------------------
class COperator : public CRefCount, public DbgPrintMixin<COperator>
{
private:
	// private copy ctor
	COperator(COperator &);

protected:
	// operator id that is unique over all instances of all operator types
	// for the current query
	ULONG m_ulOpId;

	// memory pool for internal allocations
	CMemoryPool *m_mp;

	// is pattern of xform
	BOOL m_fPattern;

	// return an addref'ed copy of the operator
	virtual COperator *PopCopyDefault();

	// derive stability function property from children
	static IMDFunction::EFuncStbl EfsDeriveFromChildren(
		CExpressionHandle &exprhdl, IMDFunction::EFuncStbl efsDefault);

	// derive function properties from children
	static CFunctionProp *PfpDeriveFromChildren(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		IMDFunction::EFuncStbl efsDefault, BOOL fHasVolatileFunctionScan,
		BOOL fScan);

	// generate unique operator ids
	static ULONG m_aulOpIdCounter;

public:
	// identification
	enum EOperatorId
	{
		EopLogicalGet,
		EopLogicalExternalGet,
		EopLogicalPathGet,					// S62VAR varlen added
		EopLogicalShortestPathGet,			// S62VAR varlen added
		EopLogicalAllShortestPathGet,			// S62VAR varlen added
		EopLogicalIndexGet,
		EopLogicalIndexPathGet,					// S62VAR  varlen added
		EopLogicalBitmapTableGet,
		EopLogicalSelect,
		EopLogicalUnion,
		EopLogicalUnionAll,
		EopLogicalIntersect,
		EopLogicalIntersectAll,
		EopLogicalDifference,
		EopLogicalDifferenceAll,
		EopLogicalInnerJoin,
		EopLogicalPathJoin,				// S62VAR added
		EopLogicalNAryJoin,
		EopLogicalLeftOuterJoin,
		EopLogicalLeftSemiJoin,
		EopLogicalLeftAntiSemiJoin,
		EopLogicalLeftAntiSemiJoinNotIn,
		EopLogicalFullOuterJoin,
		EopLogicalGbAgg,
		EopLogicalGbAggDeduplicate,
		EopLogicalLimit,
		EopLogicalProject,						// PSQL operator - row major
		EopLogicalProjectColumnar,				// S62 Added
		EopLogicalRename,
		EopLogicalInnerApply,
		EopLogicalPathApply,				// S62VAR added
		EopLogicalShortestPath,				// S62VAR added
		EopLogicalAllShortestPath,				// S62VAR added
		EopLogicalInnerCorrelatedApply,
		EopLogicalIndexApply,
		EopLogicalIndexPathApply,			// S62VAR added
		EopLogicalLeftOuterApply,
		EopLogicalLeftOuterCorrelatedApply,
		EopLogicalLeftSemiApply,
		EopLogicalLeftSemiCorrelatedApply,
		EopLogicalLeftSemiApplyIn,
		EopLogicalLeftSemiCorrelatedApplyIn,
		EopLogicalLeftAntiSemiApply,
		EopLogicalLeftAntiSemiCorrelatedApply,
		EopLogicalLeftAntiSemiApplyNotIn,
		EopLogicalLeftAntiSemiCorrelatedApplyNotIn,
		EopLogicalRightOuterJoin,
		EopLogicalConstTableGet,
		EopLogicalDynamicGet,
		EopLogicalDynamicIndexGet,
		EopLogicalSequence,
		EopLogicalTVF,
		EopLogicalCTEAnchor,
		EopLogicalCTEProducer,
		EopLogicalCTEConsumer,
		EopLogicalSequenceProject,
		EopLogicalInsert,
		EopLogicalDelete,
		EopLogicalUpdate,
		EopLogicalDML,
		EopLogicalSplit,
		EopLogicalRowTrigger,
		EopLogicalPartitionSelector,
		EopLogicalAssert,
		EopLogicalMaxOneRow,
		EopLogicalMultiExternalGet,

		EopScalarCmp,
		EopScalarIsDistinctFrom,
		EopScalarIdent,
		EopScalarProjectElement,
		EopScalarProjectList,
		EopScalarNAryJoinPredList,
		EopScalarConst,
		EopScalarBoolOp,
		EopScalarFunc,
		EopScalarMinMax,
		EopScalarAggFunc,
		EopScalarWindowFunc,
		EopScalarOp,
		EopScalarNullIf,
		EopScalarNullTest,
		EopScalarBooleanTest,
		EopScalarIf,
		EopScalarSwitch,
		EopScalarSwitchCase,
		EopScalarCaseTest,
		EopScalarCast,
		EopScalarCoerceToDomain,
		EopScalarCoerceViaIO,
		EopScalarArrayCoerceExpr,
		EopScalarCoalesce,
		EopScalarArray,
		EopScalarArrayCmp,
		EopScalarArrayRef,
		EopScalarArrayRefIndexList,
		EopScalarValuesList,

		EopScalarAssertConstraintList,
		EopScalarAssertConstraint,

		EopScalarSortGroupClause,
		EopScalarSubquery,
		EopScalarSubqueryAny,
		EopScalarSubqueryAll,
		EopScalarSubqueryExists,
		EopScalarSubqueryNotExists,

		EopScalarDMLAction,

		EopScalarBitmapIndexProbe,
		EopScalarBitmapBoolOp,

		EopPhysicalTableScan,
		EopPhysicalExternalScan,
		EopPhysicalIndexScan,
		EopPhysicalIndexPathScan,						// S62VAR added for path - maybe its index only?
		EopPhysicalIndexOnlyScan,
		EopPhysicalMultiTableIndexOnlyScan,				// S62 added for AdjIdxScan
		EopPhysicalBitmapTableScan,
		EopPhysicalFilter,
		EopPhysicalInnerNLJoin,
		EopPhysicalInnerIndexNLJoin,
		EopPhysicalIndexPathJoin,					// S62VAR added for path
		EopPhysicalCorrelatedInnerNLJoin,
		EopPhysicalLeftOuterNLJoin,
		EopPhysicalLeftOuterIndexNLJoin,
		EopPhysicalCorrelatedLeftOuterNLJoin,
		EopPhysicalLeftSemiNLJoin,
		EopPhysicalCorrelatedLeftSemiNLJoin,
		EopPhysicalCorrelatedInLeftSemiNLJoin,
		EopPhysicalLeftAntiSemiNLJoin,
		EopPhysicalCorrelatedLeftAntiSemiNLJoin,
		EopPhysicalLeftAntiSemiNLJoinNotIn,
		EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin,
		EopPhysicalInnerMergeJoin,					// S62 added for inner merge join
		EopPhysicalFullMergeJoin,
		EopPhysicalDynamicTableScan,
		EopPhysicalSequence,
		EopPhysicalTVF,
		EopPhysicalCTEProducer,
		EopPhysicalCTEConsumer,
		EopPhysicalSequenceProject,
		EopPhysicalDynamicIndexScan,

		EopPhysicalInnerHashJoin,
		EopPhysicalLeftOuterHashJoin,
		EopPhysicalLeftSemiHashJoin,
		EopPhysicalLeftAntiSemiHashJoin,
		EopPhysicalLeftAntiSemiHashJoinNotIn,
		EopPhysicalRightOuterHashJoin,

		EopPhysicalMotionGather,
		EopPhysicalMotionBroadcast,
		EopPhysicalMotionHashDistribute,
		EopPhysicalMotionRoutedDistribute,
		EopPhysicalMotionRandom,

		EopPhysicalHashAgg,
		EopPhysicalHashAggDeduplicate,
		EopPhysicalStreamAgg,
		EopPhysicalStreamAggDeduplicate,
		EopPhysicalScalarAgg,

		EopPhysicalSerialUnionAll,
		EopPhysicalParallelUnionAll,

		EopPhysicalSort,
		EopPhysicalLimit,
		EopPhysicalComputeScalar,					// PSQL operator - Row major
		EopPhysicalComputeScalarColumnar,			// S62 Added
		EopPhysicalShortestPath,				// S62 Added
		EopPhysicalAllShortestPath,				// S62 Added
		EopPhysicalSpool,
		EopPhysicalPartitionSelector,
		EopPhysicalPartitionSelectorDML,

		EopPhysicalConstTableGet,

		EopPhysicalDML,
		EopPhysicalSplit,
		EopPhysicalRowTrigger,

		EopPhysicalAssert,

		EopPatternTree,
		EopPatternLeaf,
		EopPatternMultiLeaf,
		EopPatternMultiTree,
		EopPatternNode,

		EopLogicalDynamicBitmapTableGet,
		EopPhysicalDynamicBitmapTableScan,
		EopPhysicalMultiExternalScan,

		EopSentinel
	};

	// aggregate type
	enum EGbAggType
	{
		EgbaggtypeGlobal,		 // global group by aggregate
		EgbaggtypeLocal,		 // local group by aggregate
		EgbaggtypeIntermediate,	 // intermediate group by aggregate

		EgbaggtypeSentinel
	};

	// coercion form
	enum ECoercionForm
	{
		EcfExplicitCall,  // display as a function call
		EcfExplicitCast,  // display as an explicit cast
		EcfImplicitCast,  // implicit cast, so hide it
		EcfDontCare		  // don't care about display
	};

	// ctor
	explicit COperator(CMemoryPool *mp);

	// dtor
	virtual ~COperator()
	{
	}

	// the id of the operator
	ULONG
	UlOpId() const
	{
		return m_ulOpId;
	}

	// ident accessors
	virtual EOperatorId Eopid() const = 0;

	// return a string for operator name
	virtual const CHAR *SzId() const = 0;

	// the following functions check operator's type

	// is operator logical?
	virtual BOOL
	FLogical() const
	{
		return false;
	}

	// is operator physical?
	virtual BOOL
	FPhysical() const
	{
		return false;
	}

	// is operator scalar?
	virtual BOOL
	FScalar() const
	{
		return false;
	}

	// is operator pattern?
	virtual BOOL
	FPattern() const
	{
		return false;
	}

	// hash function
	virtual ULONG HashValue() const;

	// sensitivity to order of inputs
	virtual BOOL FInputOrderSensitive() const = 0;

	// match function;
	// abstract to enforce an implementation for each new operator
	virtual BOOL Matches(COperator *pop) const = 0;

	// create container for derived properties
	virtual CDrvdProp *PdpCreate(CMemoryPool *mp) const = 0;

	// create container for required properties
	virtual CReqdProp *PrpCreate(CMemoryPool *mp) const = 0;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist) = 0;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class COperator

}  // namespace gpopt


#endif	// !GPOPT_COperator_H

// EOF
