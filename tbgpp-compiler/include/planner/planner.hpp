#pragma once

#include "main/client_context.hpp"
#include "common/enums/index_type.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "main/database.hpp"


#include <iostream>
#include <type_traits>

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
#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/base/CDistributionSpecStrictSingleton.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefTable.h"

#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalProjectColumnar.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalPathJoin.h"
#include "gpopt/operators/CLogicalPathGet.h"

#include "gpopt/operators/CPhysicalTableScan.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalIndexPathScan.h"
#include "gpopt/operators/CPhysicalSerialUnionAll.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPhysicalFilter.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"
#include "gpopt/operators/CPhysicalIndexPathJoin.h"
#include "gpopt/operators/CPhysicalInnerNLJoin.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalSort.h"

#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarConst.h"

#include "naucrates/init.h"
#include "naucrates/traceflags/traceflags.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeGeneric.h"
#include "naucrates/base/IDatumGeneric.h"
#include "naucrates/base/CDatumGenericGPDB.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "kuzu/binder/bound_statement.h"
#include "kuzu/binder/query/reading_clause/bound_reading_clause.h"
#include "kuzu/binder/query/reading_clause/bound_match_clause.h"
#include "kuzu/binder/expression/expression.h"
#include "kuzu/binder/expression/function_expression.h"
#include "kuzu/binder/expression/literal_expression.h"
#include "kuzu/binder/expression/property_expression.h"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/order_type.hpp"

#include "BTNode.h"
#include "planner/logical_plan.hpp"
#include "planner/value_ser_des.hpp"

#include "mdprovider/MDProviderTBGPP.h"


using namespace kuzu::binder;
using namespace gpopt;

namespace s62 {

enum class MDProviderType {
	MEMORY,
	TBGPP
};

class ClientContext;

class PlannerConfig {

public:
enum JoinOrderType {
	JOIN_ORDER_IN_QUERY = 1,
	JOIN_ORDER_GREEDY_SEARCH = 2,
	JOIN_ORDER_EXHAUSTIVE_SEARCH = 3,
	JOIN_ORDER_EXHAUSTIVE2_SEARCH = 4
};

public:
	bool DEBUG_PRINT;
	bool ORCA_DEBUG_PRINT;
	bool RUN_PLAN_WO_COMPILE;

	bool INDEX_JOIN_ONLY;
	// TODO s62 configure optimizer_join_order_threshold!	 // this matters when exhastive2 and exhasutive
	PlannerConfig::JoinOrderType JOIN_ORDER_TYPE;
	uint8_t JOIN_ORDER_DP_THRESHOLD_CONFIG;

	PlannerConfig() :
		DEBUG_PRINT(false),
		ORCA_DEBUG_PRINT(false),
		INDEX_JOIN_ONLY(false),
		RUN_PLAN_WO_COMPILE(false),
		JOIN_ORDER_TYPE(JoinOrderType::JOIN_ORDER_EXHAUSTIVE2_SEARCH),
		JOIN_ORDER_DP_THRESHOLD_CONFIG(10)
	{ }
};

class PlannerUtils {

public:
};

class Planner {

public:
	Planner(PlannerConfig config, MDProviderType mdp_type, duckdb::ClientContext* context, string memory_mdp_path = "");	// TODO change client signature to reference
	~Planner();

	void execute(BoundStatement* bound_statement);
	vector<duckdb::CypherPipelineExecutor*> genPipelineExecutors();
	vector<string> getQueryOutputColNames();

private:
	// planner.cpp
	/* Orca Related */
	void reset();
	void orcaInit();
	static void * _orcaExec(void* planner_ptr);
	void _orcaSetTraceFlags();
	CQueryContext* _orcaGenQueryCtxt(CMemoryPool* mp, CExpression* logical_plan);
	CMDProviderMemory* _orcaGetProviderMemory();
	MDProviderTBGPP* _orcaGetProviderTBGPP();
	void _orcaInitXForm();
	gpdbcost::CCostModelGPDB* _orcaGetCostModel(CMemoryPool* mp);
	void _orcaSetOptCtxt(CMemoryPool* mp, CMDAccessor* mda, gpdbcost::CCostModelGPDB* pcm);

private:
	// planner_logical.cpp
	/* Generating orca logical plan */
	CExpression *lGetLogicalPlan();
	CExpression *lPlanSingleQuery(const NormalizedSingleQuery& singleQuery);
	LogicalPlan *lPlanQueryPart(
        const NormalizedQueryPart& queryPart, LogicalPlan* prev_plan);
	LogicalPlan *lPlanProjectionBody(LogicalPlan* plan, BoundProjectionBody* proj_body);
	LogicalPlan *lPlanReadingClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan *lPlanMatchClause(
		BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan *lPlanUnwindClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan *lPlanRegularMatch(const QueryGraphCollection& queryGraphCollection, LogicalPlan* prev_plan);
	LogicalPlan *lPlanNodeOrRelExpr(NodeOrRelExpression* node_expr, bool is_node);
	LogicalPlan *lPlanPathGet(RelExpression* edge_expr);
	LogicalPlan *lPlanProjectionOnColRefs(LogicalPlan* plan, CColRefArray* colrefs);
	LogicalPlan *lPlanSelection(const expression_vector& predicates, LogicalPlan* prev_plan);
	LogicalPlan *lPlanOrderBy(const expression_vector &orderby_exprs, const vector<bool> sort_orders, LogicalPlan *prev_plan);
	
	/* Helper functions for generating orca logical plans */
	CExpression *lExprScalarExpression(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarComparisonExpr(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarPropertyExpr(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarLiteralExpr(Expression* expression, LogicalPlan* prev_plan);


	std::pair<CExpression*, CColRefArray*> lExprLogicalGetNodeOrEdge(
		string name, vector<uint64_t> oids,
		map<uint64_t, map<uint64_t, uint64_t>> * schema_proj_mapping, bool insert_projection
	);

	CExpression * lExprLogicalGet(uint64_t obj_id, string rel_name, string alias = "");
	CExpression * lExprLogicalUnionAllWithMapping(CExpression* lhs, CColRefArray* lhs_mapping, CExpression* rhs, CColRefArray* rhs_mapping);

	std::pair<CExpression*, CColRefArray*> lExprScalarAddSchemaConformProject(
		CExpression* relation, vector<uint64_t> col_ids_to_project,
		vector<pair<IMDId*, gpos::INT>>* target_schema_types
	);
	CExpression* lExprLogicalJoin(CExpression* lhs, CExpression* rhs,
		CColRef* lhs_colref, CColRef* rhs_colref, bool project_out_lhs_key=false, bool project_out_rhs_key=false);
	CExpression* lExprLogicalPathJoin(CExpression* lhs, CExpression* rhs,
		CColRef* lhs_colref, CColRef* rhs_colref, int32_t lower_bound, int32_t upper_bound);
	CExpression* lExprLogicalCartProd(CExpression* lhs, CExpression* rhs);
	
	CTableDescriptor * lCreateTableDescForRel(CMDIdGPDB* rel_mdid, std::string print_name="");
	CTableDescriptor * lCreateTableDesc(CMemoryPool *mp, IMDId *mdid,
						   const CName &nameTable, gpos::BOOL fPartitioned = false);
	CTableDescriptor * lTabdescPlainWithColNameFormat(
		CMemoryPool *mp, IMDId *mdid, const WCHAR *wszColNameFormat,
		const CName &nameTable,
		gpos::BOOL is_nullable  // define nullable columns
	);

	inline CMDAccessor* lGetMDAccessor() { return COptCtxt::PoctxtFromTLS()->Pmda(); };
	inline CMDIdGPDB* lGenRelMdid(uint64_t obj_id) { return GPOS_NEW(this->memory_pool) CMDIdGPDB(IMDId::EmdidRel, obj_id, 0, 0); }
	inline const IMDRelation* lGetRelMd(uint64_t obj_id) {
		return lGetMDAccessor()->RetrieveRel(lGenRelMdid(obj_id));
	}

private:
	// planner_physical.cpp
	/* Generating orca physical plan */
	void pGenPhysicalPlan(CExpression* orca_plan_root);
	bool pValidatePipelines();
	vector<duckdb::CypherPhysicalOperator*>* pTraverseTransformPhysicalPlan(CExpression* plan_expr);
	
	// scan
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopTableScan(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopFilterWithScanPushdown(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopUnionAllForNodeOrEdgeScan(CExpression* plan_expr);

	// pipelined ops
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopProjectionColumnar(CExpression* plan_expr);

	// joins
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(CExpression* plan_expr);

	// limit, sort
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopLimit(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopSort(CExpression* plan_expr);

	bool pIsIndexJoinOnPhysicalID(CExpression* plan_expr);

	bool pMatchExprPattern(CExpression* root, vector<COperator::EOperatorId>& pattern, uint64_t pattern_root_idx=0, bool physical_op_only=false);
	bool pIsUnionAllOpAccessExpression(CExpression* expr);
	
	uint64_t pGetColIdxOfColref(CColRefSet* refset, const CColRef* target_col);
	uint64_t pGetColIdxFromTable(OID table_oid, const CColRef* target_col);

	bool pIsColumnarProjectionSimpleProject(CExpression* proj_expr);;
	CColRefArray* pGetUnderlyingColRefsOfColumnarProjection(CColRefArray* output_colrefs, CExpression* proj_expr);

// TODO move to PlannerUtils
	inline duckdb::LogicalType pConvertTypeOidToLogicalType(OID oid) {
		return duckdb::LogicalType( pConvertTypeOidToLogicalTypeId(oid) );
	}
	inline duckdb::LogicalTypeId pConvertTypeOidToLogicalTypeId(OID oid) {
		auto type_id = static_cast<std::underlying_type_t<duckdb::LogicalTypeId>>(oid);
		return (duckdb::LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID);
	}

	duckdb::OrderByNullType translateNullType(COrderSpec::ENullTreatment ent);

private:
	// config
	const PlannerConfig config;
	const MDProviderType mdp_type;
	const std::string memory_mdp_filepath;

	// core
	duckdb::ClientContext* context;	// TODO this should be reference - refer to plansuite
	CMemoryPool* memory_pool;

	// used and initialized in each execution
	BoundStatement* bound_statement;			// input parse statemnt
	std::map<OID, std::vector<CColRef*>> table_col_mapping;
	vector<duckdb::CypherPipeline*> pipelines;	// output plan pipelines
	vector<std::string> output_col_names;
};

}