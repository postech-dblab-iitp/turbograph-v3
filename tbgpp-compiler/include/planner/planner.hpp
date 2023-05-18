#pragma once

#include "main/client_context.hpp"
#include "common/enums/index_type.hpp"
#include "common/constants.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "main/database.hpp"
#include "catalog_wrapper.hpp"
#include "function/function.hpp"
#include "function/aggregate_function.hpp"
#include "function/aggregate/distributive_functions.hpp"

#include <iostream>
#include <type_traits>
#include <string>

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
#include "naucrates/md/CMDTypeBoolGPDB.h"

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

// orca logical ops
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
#include "gpopt/operators/CLogicalRightOuterJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalPathJoin.h"
#include "gpopt/operators/CLogicalPathGet.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalGbAggDeduplicate.h"
#include "gpopt/operators/CScalarSubqueryExists.h"

// orca physical ops
#include "gpopt/operators/CPhysicalTableScan.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalIndexPathScan.h"
#include "gpopt/operators/CPhysicalSerialUnionAll.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPhysicalFilter.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"
#include "gpopt/operators/CPhysicalLeftOuterIndexNLJoin.h"
#include "gpopt/operators/CPhysicalIndexPathJoin.h"
#include "gpopt/operators/CPhysicalInnerNLJoin.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalLimit.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalHashAggDeduplicate.h"
#include "gpopt/operators/CPhysicalStreamAgg.h"
#include "gpopt/operators/CPhysicalStreamAggDeduplicate.h"

#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarValuesList.h"

#include "gpopt/operators/CScalarSwitch.h"
#include "gpopt/operators/CScalarSwitchCase.h"

#include "naucrates/init.h"
#include "naucrates/traceflags/traceflags.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeGeneric.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/base/IDatumGeneric.h"
#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"


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
#include "kuzu/binder/expression/node_rel_expression.h"
#include "kuzu/binder/expression/case_expression.h"
#include "kuzu/binder/expression/existential_subquery_expression.h"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/order_type.hpp"
#include "common/enums/join_type.hpp"


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

	bool INDEX_JOIN_ONLY;
	PlannerConfig::JoinOrderType JOIN_ORDER_TYPE;
	uint8_t JOIN_ORDER_DP_THRESHOLD_CONFIG;

	int num_iterations = 1;

	PlannerConfig() :
		DEBUG_PRINT(false),
		ORCA_DEBUG_PRINT(false),
		INDEX_JOIN_ONLY(false),
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
	LogicalPlan *lGetLogicalPlan();
	LogicalPlan *lPlanSingleQuery(const NormalizedSingleQuery &singleQuery);
	LogicalPlan *lPlanQueryPart(
        const NormalizedQueryPart &queryPart, LogicalPlan *prev_plan);
	LogicalPlan *lPlanProjectionBody(LogicalPlan *plan, BoundProjectionBody *proj_body);
	LogicalPlan *lPlanReadingClause(
        BoundReadingClause *boundReadingClause, LogicalPlan *prev_plan);
	LogicalPlan *lPlanMatchClause(
		BoundReadingClause *boundReadingClause, LogicalPlan *prev_plan);
	LogicalPlan *lPlanUnwindClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan *lPlanRegularMatch(const QueryGraphCollection& queryGraphCollection, LogicalPlan* prev_plan, bool is_optional_match);
	LogicalPlan *lPlanRegularMatchFromSubquery(const QueryGraphCollection& queryGraphCollection, LogicalPlan* outer_plan);
	LogicalPlan *lPlanNodeOrRelExpr(NodeOrRelExpression* node_expr, bool is_node);
	LogicalPlan *lPlanPathGet(RelExpression* edge_expr);
	LogicalPlan *lPlanSelection(const expression_vector& predicates, LogicalPlan* prev_plan);
	LogicalPlan *lPlanProjection(const expression_vector& expressions, LogicalPlan* prev_plan);
	LogicalPlan *lPlanGroupBy(const expression_vector &expressions, LogicalPlan* prev_plan);
	LogicalPlan *lPlanOrderBy(const expression_vector &orderby_exprs, const vector<bool> sort_orders, LogicalPlan *prev_plan);
	LogicalPlan *lPlanDistinct(CColRefArray *colrefs, LogicalPlan *prev_plan);
	LogicalPlan *lPlanSkipOrLimit(BoundProjectionBody *proj_body, LogicalPlan *prev_plan);
	
	
	// scalar expression
	CExpression *lExprScalarExpression(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarBoolOp(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarComparisonExpr(Expression* expression, LogicalPlan* prev_plan);
	CExpression* lExprScalarCmpEq(CExpression* left_expr, CExpression* right_expr);	// note that two inputs are gpos::CExpression*
	CExpression *lTryGenerateScalarIdent(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarPropertyExpr(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarPropertyExpr(string k1, string k2, LogicalPlan* prev_plan);
	CExpression *lExprScalarLiteralExpr(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarAggFuncExpr(Expression* expression, LogicalPlan* prev_plan);
	CExpression *lExprScalarCaseElseExpr(Expression *expression, LogicalPlan *prev_plan);
	CExpression *lExprScalarExistentialSubqueryExpr(Expression *expression, LogicalPlan *prev_plan);

	/* Helper functions for generating orca logical plans */
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
		CColRef* lhs_colref, CColRef* rhs_colref, gpopt::COperator::EOperatorId join_op);
	CExpression* lExprLogicalPathJoin(CExpression* lhs, CExpression* rhs,
		CColRef* lhs_colref, CColRef* rhs_colref, int32_t lower_bound, int32_t upper_bound,
		 gpopt::COperator::EOperatorId join_op);
	CExpression* lExprLogicalCartProd(CExpression* lhs, CExpression* rhs);
	
	CTableDescriptor * lCreateTableDescForRel(CMDIdGPDB* rel_mdid, std::string rel_name="");
	CTableDescriptor * lCreateTableDesc(CMemoryPool *mp, IMDId *mdid,
						   const CName &nameTable, string rel_name, gpos::BOOL fPartitioned = false);
	CTableDescriptor * lTabdescPlainWithColNameFormat(
		CMemoryPool *mp, IMDId *mdid, const WCHAR *wszColNameFormat,
		const CName &nameTable, string rel_name,
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
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopUnionAllForNodeOrEdgeScan(CExpression* plan_expr);

	// pipelined ops
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopProjectionColumnar(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalFilter(CExpression* plan_expr);

	// joins
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(CExpression* plan_expr, bool is_left_outer);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerNLJoinToCartesianProduct(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalNLJoinToBlockwiseNLJoin(CExpression* plan_expr);

	// limit, sort
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopLimit(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopSort(CExpression* plan_expr);

	// aggregations
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopAgg(CExpression* plan_expr);

	// scalar expression
	unique_ptr<duckdb::Expression> pTransformScalarExpr(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarIdent(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarConst(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarCmp(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarBoolOp(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarAggFunc(CExpression * scalar_expr, CColRefArray* child_cols, CColRefArray* rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarSwitch(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray* rhs_child_cols = nullptr);

	// investigate plan properties
	bool pMatchExprPattern(CExpression* root, vector<COperator::EOperatorId>& pattern, uint64_t pattern_root_idx=0, bool physical_op_only=false);
	bool pIsIndexJoinOnPhysicalID(CExpression* plan_expr);
	bool pIsUnionAllOpAccessExpression(CExpression* expr);
	bool pIsColumnarProjectionSimpleProject(CExpression* proj_expr);
	bool pIsFilterPushdownAbleIntoScan(CExpression* selection_expr);
	bool pIsCartesianProduct(CExpression* expr);
	
	// helper functions
	void pGenerateScanMappingAndFromTableID(OID table_oid, CColRefArray* columns, vector<uint64_t>& out_mapping);
	void pGenerateTypes(CColRefArray* columns, vector<duckdb::LogicalType>& out_types);
	void pGenerateColumnNames(CColRefArray* columns, vector<string>& out_col_names);
	uint64_t pGetColIdxFromTable(OID table_oid, const CColRef* target_col);
	void pGenerateFilterExprs(CColRefArray* outer_cols, duckdb::ExpressionType &exp_type, CExpression *filter_pred_expr, vector<unique_ptr<duckdb::Expression>> &filter_exprs);

	inline string pGetColNameFromColRef(const CColRef* column) {
		std::wstring name_ws(column->Name().Pstr()->GetBuffer());
		string name(name_ws.begin(), name_ws.end());
		return name;
	}
	inline duckdb::LogicalType pConvertTypeOidToLogicalType(OID oid) {
		return duckdb::LogicalType( pConvertTypeOidToLogicalTypeId(oid) );
	}
	inline duckdb::LogicalTypeId pConvertTypeOidToLogicalTypeId(OID oid) {
		auto type_id = static_cast<std::underlying_type_t<duckdb::LogicalTypeId>>(oid);
		return (duckdb::LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID);
	}

	// scalar helper functions 
	static duckdb::OrderByNullType pTranslateNullType(COrderSpec::ENullTreatment ent);
	static duckdb::ExpressionType pTranslateCmpType(IMDType::ECmpType cmp_type);
	static duckdb::ExpressionType pTranslateBoolOpType(CScalarBoolOp::EBoolOperator op_type);
	static duckdb::JoinType pTranslateJoinType(COperator* op);
	static CColRef* pGetColRefFromScalarIdent(CExpression* ident_expr);

private:
	// config
	const PlannerConfig config;
	const MDProviderType mdp_type;
	const std::string memory_mdp_filepath;

	// core
	duckdb::ClientContext* context;	// TODO this should be reference - refer to plansuite
	CMemoryPool* memory_pool;

	// used and initialized in each execution
	BoundStatement* bound_statement;					// input parse statemnt
	vector<duckdb::CypherPipeline*> pipelines;			// output plan pipelines
	vector<std::string> logical_plan_output_col_names;			// output col names
	std::vector<CColRef*> logical_plan_output_colrefs;	// final output colrefs of the logical plan (user's view)
	std::vector<CColRef*> physical_plan_output_colrefs;	// final output colrefs of the physical plan
	

};

}