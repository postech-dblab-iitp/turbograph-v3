#pragma once

#include "main/client_context.hpp"

#include <iostream>

#include "gpos/_api.h"
#include "naucrates/init.h"
#include "gpopt/init.h"

#include "unittest/gpopt/engine/CEngineTest.h"
#include "gpos/test/CUnittest.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "gpos/memory/CMemoryPool.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "gpopt/operators/CLogicalGet.h"

#include "gpos/_api.h"
#include "gpos/common/CMainArgs.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CFSimulatorTestExt.h"
#include "gpos/test/CUnittest.h"
#include "gpos/types.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/init.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/base/CDistributionSpecStrictSingleton.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CScalarIdent.h"

#include "planner/planner.hpp"

#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "kuzu/binder/bound_statement.h"
#include "kuzu/binder/query/reading_clause/bound_reading_clause.h"
#include "kuzu/binder/query/reading_clause/bound_match_clause.h"
#include "kuzu/binder/expression/expression.h"

#include "naucrates/traceflags/traceflags.h"



#include "BTNode.h"
#include "planner/logical_plan.hpp"



namespace s62 {

using namespace kuzu::binder;

class ClientContext;
class Planner {

public:
	Planner(duckdb::ClientContext* context = nullptr);	// TODO change client signature to reference
	~Planner();

	void execute(BoundStatement* bound_statement);

private:

	/* Orca Related */
	void orcaInit();
	static void * _orcaExec(void* planner_ptr);
	void _orcaSetTraceFlags();
	CQueryContext* _orcaGenQueryCtxt(CMemoryPool* mp, CExpression* logical_plan);
	CMDProviderMemory* _orcaGetProviderMemory();
	void _orcaInitXForm();
	gpdbcost::CCostModelGPDB* _orcaGetCostModel(CMemoryPool* mp);
	void _orcaSetOptCtxt(CMemoryPool* mp, CMDAccessor* mda, gpdbcost::CCostModelGPDB* pcm);

	/* Generating orca logical plan */
	CExpression* lGetLogicalPlan();
	CExpression* lPlanSingleQuery(const NormalizedSingleQuery& singleQuery);
	LogicalPlan* lPlanQueryPart(
        const NormalizedQueryPart& queryPart, LogicalPlan* prev_plan);
	LogicalPlan* lPlanReadingClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan* lPlanMatchClause(
		BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan* lPlanUnwindClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan* lPlanRegularMatch(const QueryGraphCollection& queryGraphCollection,
        expression_vector& predicates, LogicalPlan* prev_plan);
	
	/* Helper for generating orca logical plans */
	LogicalPlan* lExprLogicalGetNode(string name);
	LogicalPlan* lExprLogicalGetEdge(string name);

	CExpression * lExprLogicalGet(uint64_t obj_id, string rel_name, uint64_t rel_width, string alias = "");

	CExpression* lExprScalarProjectionOnColIds(CExpression* relation, vector<uint64_t> col_ids);
	CExpression* lExprLogicalJoinOnId(CExpression* lhs, CExpression* rhs, uint64_t lhs_pos, uint64_t rhs_pos);
	
	CTableDescriptor * lCreateTableDesc(CMemoryPool *mp, ULONG num_cols, IMDId *mdid,
						   const CName &nameTable, gpos::BOOL fPartitioned = false);
	CTableDescriptor * lTabdescPlainWithColNameFormat(
		CMemoryPool *mp, ULONG num_cols, IMDId *mdid, const WCHAR *wszColNameFormat,
		const CName &nameTable,
		gpos::BOOL is_nullable  // define nullable columns
	);

	gpopt::CColRef* lGetIthColRef(CColRefSet* refset, uint64_t idx);

private:
	duckdb::ClientContext* context;	// TODO this should be reference - refer to plansuite
	CMemoryPool* memory_pool;

	BoundStatement* bound_statement;

	// TODO maybe, add logical query context.

	// TODO kuzu had following
	// expression_vector propertiesToScan;
    // JoinOrderEnumerator joinOrderEnumerator;
    // ProjectionPlanner projectionPlanner;

};

}