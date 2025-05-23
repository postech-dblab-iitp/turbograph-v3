#pragma once

#include "main/client_context.hpp"
#include "common/enums/index_type.hpp"
#include "common/constants.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "main/database.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "function/function.hpp"
#include "function/aggregate_function.hpp"
#include "function/aggregate/distributive_functions.hpp"

#include <iostream>
#include <type_traits>
#include <string>

#include "gpos/_api.h"
#include "gpopt/init.h"

#include "gpos/test/CUnittest.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/base.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpopt/base/CQueryContext.h"

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
#include "gpopt/operators/CScalarCast.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalRightOuterJoin.h"
#include "gpopt/operators/CLogicalLeftOuterApply.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalPathJoin.h"
#include "gpopt/operators/CLogicalPathGet.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalGbAggDeduplicate.h"
#include "gpopt/operators/CLogicalShortestPath.h"
#include "gpopt/operators/CLogicalAllShortestPath.h"
#include "gpopt/operators/CScalarSubqueryExists.h"

// orca physical ops
#include "gpopt/operators/CPhysicalTableScan.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalIndexOnlyScan.h"
#include "gpopt/operators/CPhysicalIndexPathScan.h"
#include "gpopt/operators/CPhysicalSerialUnionAll.h"
#include "gpopt/operators/CPhysicalFilter.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"
#include "gpopt/operators/CPhysicalLeftOuterIndexNLJoin.h"
#include "gpopt/operators/CPhysicalIndexPathJoin.h"
#include "gpopt/operators/CPhysicalInnerNLJoin.h"
#include "gpopt/operators/CPhysicalInnerHashJoin.h"
#include "gpopt/operators/CPhysicalInnerMergeJoin.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalLimit.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalHashAggDeduplicate.h"
#include "gpopt/operators/CPhysicalStreamAgg.h"
#include "gpopt/operators/CPhysicalStreamAggDeduplicate.h"
#include "gpopt/operators/CPhysicalShortestPath.h"
#include "gpopt/operators/CPhysicalAllShortestPath.h"

#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarValuesList.h"
#include "gpopt/operators/CScalarSwitch.h"
#include "gpopt/operators/CScalarSwitchCase.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CIndexDescriptor.h"

#include "naucrates/init.h"
#include "naucrates/traceflags/traceflags.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDCast.h"
#include "naucrates/md/IMDTypeGeneric.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/base/IDatumGeneric.h"
#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "gpdbcost/CCostModelGPDB.h"

#include "CypherLexer.h"
#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
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
#include "kuzu/binder/expression/parameter_expression.h"
#include "kuzu/binder/expression/list_comprehension_expression.h"
#include "kuzu/binder/expression/pattern_comprehension_expression.h"
#include "kuzu/binder/expression/filter_expression.h"
#include "kuzu/binder/expression/idincoll_expression.h"
#include "kuzu/binder/expression/path_expression.h"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "execution/cypher_physical_operator_group.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/order_type.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

#include "planner/logical_plan.hpp"
#include "planner/value_ser_des.hpp"
#include "optimizer/mdprovider/MDProviderTBGPP.h"

using namespace kuzu::binder;
using namespace gpopt;

#define USE_INVERTED_INDEX

namespace s62 {

typedef vector<duckdb::OperatorType> PipelineOperatorTypes;
typedef vector<vector<uint64_t>> PipelineNumSchemas;
typedef vector<vector<duckdb::Schema>> PipelineSchemas;
typedef vector<duckdb::Schema> PipelineUnionSchema;

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
	bool HASH_JOIN_ONLY;
	bool MERGE_JOIN_ONLY;
	bool DISABLE_INDEX_JOIN;
	bool DISABLE_HASH_JOIN;
	bool DISABLE_MERGE_JOIN;
	bool ORCA_COMPILE_ONLY;
	PlannerConfig::JoinOrderType JOIN_ORDER_TYPE;
	uint8_t JOIN_ORDER_DP_THRESHOLD_CONFIG;
	
	PlannerConfig() :
		DEBUG_PRINT(false),
		ORCA_DEBUG_PRINT(false),
		INDEX_JOIN_ONLY(false),
		HASH_JOIN_ONLY(false),
		MERGE_JOIN_ONLY(false),
		DISABLE_INDEX_JOIN(false),
		DISABLE_HASH_JOIN(false),
		DISABLE_MERGE_JOIN(false),
		ORCA_COMPILE_ONLY(false),
		JOIN_ORDER_TYPE(JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH),
		JOIN_ORDER_DP_THRESHOLD_CONFIG(10)
	{ }
};

class PlannerUtils {

public:
};

class Planner {

public:
	Planner(PlannerConfig config, MDProviderType mdp_type, duckdb::ClientContext *context, string memory_mdp_path = "");	// TODO change client signature to reference
	~Planner();

	void execute(BoundStatement *bound_statement);
	vector<duckdb::CypherPipelineExecutor *> genPipelineExecutors();
	vector<string> getQueryOutputColNames();
	vector<OID> getQueryOutputOIDs();

private:
	// planner.cpp
	/* Orca Related */
	void reset();
	void orcaInit();
	static void *_orcaExec(void *planner_ptr);
	void _orcaSetTraceFlags();
	CQueryContext *_orcaGenQueryCtxt(CMemoryPool *mp, CExpression *logical_plan);
	CMDProviderMemory *_orcaGetProviderMemory();
	MDProviderTBGPP *_orcaGetProviderTBGPP();
	void _orcaInitXForm();
	gpdbcost::CCostModelGPDB *_orcaGetCostModel(CMemoryPool *mp);
	void _orcaSetOptCtxt(CMemoryPool *mp, CMDAccessor *mda, gpdbcost::CCostModelGPDB *pcm);

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
        BoundReadingClause *boundReadingClause, LogicalPlan *prev_plan);
	LogicalPlan *lPlanRegularMatch(const QueryGraphCollection& queryGraphCollection, LogicalPlan *prev_plan);
	LogicalPlan *lPlanRegularOptionalMatch(const QueryGraphCollection& queryGraphCollection, LogicalPlan *prev_plan);
	LogicalPlan *lPlanRegularMatchFromSubquery(const QueryGraphCollection& queryGraphCollection, LogicalPlan *outer_plan);
	LogicalPlan *lPlanNodeOrRelExpr(NodeOrRelExpression *node_rel_expr);
	LogicalPlan *lPlanPathGet(RelExpression *edge_expr);
	LogicalPlan *lPlanSelection(const expression_vector& predicates, LogicalPlan *prev_plan);
	LogicalPlan *lPlanProjection(const expression_vector& expressions, LogicalPlan *prev_plan);
	LogicalPlan *lPlanGroupBy(const expression_vector &expressions, LogicalPlan *prev_plan);
	LogicalPlan *lPlanOrderBy(const expression_vector &orderby_exprs, const vector<bool> sort_orders, LogicalPlan *prev_plan);
	LogicalPlan *lPlanDistinct(const expression_vector &expressions, CColRefArray *colrefs, LogicalPlan *prev_plan);
	LogicalPlan *lPlanSkipOrLimit(BoundProjectionBody *proj_body, LogicalPlan *prev_plan);
	LogicalPlan *lPlanShortestPath(QueryGraph* qg, NodeExpression *lhs, RelExpression* edge, NodeExpression *rhs, LogicalPlan *prev_plan);
	LogicalPlan *lPlanAllShortestPath(QueryGraph* qg, NodeExpression *lhs, RelExpression* edge, NodeExpression *rhs, LogicalPlan *prev_plan);

	// scalar expression
	CExpression *lExprScalarExpression(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type = DataTypeID::INVALID);
	CExpression *lExprScalarNullOp(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarBoolOp(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarComparisonExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarCmpEq(CExpression *left_expr, CExpression *right_expr);	// note that two inputs are gpos::CExpression*
	CExpression *lTryGenerateScalarIdent(kuzu::binder::Expression *expression, LogicalPlan *prev_plan);
	CExpression *lExprScalarPropertyExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarPropertyExpr(string k1, uint64_t k2, LogicalPlan *prev_plan);
	CExpression *lExprScalarLiteralExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarAggFuncExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarFuncExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarCaseElseExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarExistentialSubqueryExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarCastExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan);
	CExpression *lExprScalarParamExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarListComprehensionExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarPatternComprehensionExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarFilterExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarIdInCollExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	CExpression *lExprScalarShortestPathExpr(kuzu::binder::Expression *expression, LogicalPlan *prev_plan, DataTypeID required_type);
	INT lGetTypeModFromType(duckdb::LogicalType type);

	// scalar expression duckdb
	unique_ptr<duckdb::Expression> lExprScalarExpressionDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarBoolOpDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarComparisonExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarPropertyExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarLiteralExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarAggFuncExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarFuncExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarCaseElseExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarExistentialSubqueryExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarCastExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarParamExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarShortestPathExprDuckDB(kuzu::binder::Expression *expression);
	unique_ptr<duckdb::Expression> lExprScalarAllShortestPathExprDuckDB(kuzu::binder::Expression *expression);

	/* Helper functions for generating orca logical plans */
	CExpression *lGetCoalescedGet(NodeOrRelExpression *node_rel_expr, 
		const expression_vector &prop_exprs, std::vector<uint64_t> &table_oids, uint64_t &repr_oid);
    std::pair<CExpression *, CColRefArray *> lExprLogicalGetNodeOrEdge(
        string name, vector<uint64_t> &oids,
        std::vector<std::vector<uint64_t>> *table_oids_in_groups,
        vector<int> &used_col_idx,
        map<uint64_t, map<uint64_t, uint64_t>> *schema_proj_mapping,
        bool insert_projection, bool whole_node_required);
    void lBuildSchemaProjectionMapping(
        std::vector<uint64_t> &table_oids, NodeOrRelExpression *node_expr,
        const expression_vector &prop_exprs,
        map<uint64_t, map<uint64_t, uint64_t>> &schema_proj_mapping,
        vector<int> &used_col_idx, bool is_dsi = false);
    void lGenerateNodeOrEdgeSchema(NodeOrRelExpression *node_expr,
                                   const expression_vector &prop_exprs,
                                   bool is_node, vector<int> &used_col_idx,
                                   CColRefArray *prop_colrefs,
                                   LogicalSchema &schema);
	void lGetUsedPropertyIDs(
		NodeOrRelExpression *node_rel_expr, const expression_vector &prop_exprs,
		 std::vector<uint64_t>& prop_key_ids, bool exclude_pid = true
	);

	void lUpdatePropExprsForNewTable(
		NodeOrRelExpression *node_rel_expr, const expression_vector &prop_exprs,
		uint64_t oid, std::vector<uint64_t>& property_location
	);

    CExpression *lExprLogicalGet(
        uint64_t obj_id, string rel_name, bool is_instance = false,
        std::vector<uint64_t> *table_oids_in_group = nullptr,
        bool whole_node_required = false, string alias = "");
    CExpression *lExprLogicalUnionAllWithMapping(CExpression *lhs,
                                                 CColRefArray *lhs_mapping,
                                                 CExpression *rhs,
                                                 CColRefArray *rhs_mapping);

    std::pair<CExpression *, CColRefArray *> lExprScalarAddSchemaConformProject(
        CExpression *relation, vector<uint64_t> &col_ids_to_project,
        vector<pair<IMDId *, gpos::INT>> *target_schema_types,
        vector<CColRef *> &union_schema_colrefs);
    CExpression *lExprLogicalJoin(CExpression *lhs, CExpression *rhs,
                                  CColRef *lhs_colref, CColRef *rhs_colref,
                                  gpopt::COperator::EOperatorId join_op,
                                  CExpression *additional_join_pred);
    CExpression *lExprLogicalPathJoin(CExpression *lhs, CExpression *rhs,
                                      CColRef *lhs_colref, CColRef *rhs_colref,
                                      int32_t lower_bound, int32_t upper_bound,
                                      gpopt::COperator::EOperatorId join_op);

    CExpression *lExprLogicalShortestPathJoin(
        CExpression *lhs, CExpression *rhs, CColRef *lhs_colref,
        CColRef *rhs_colref, gpopt::COperator::EOperatorId join_op);
    CExpression *lExprLogicalCartProd(CExpression *lhs, CExpression *rhs);

    CTableDescriptor *lCreateTableDescForRel(CMDIdGPDB *rel_mdid,
                                             std::string rel_name = "");
    CTableDescriptor *lCreateTableDesc(CMemoryPool *mp, IMDId *mdid,
                                       const CName &nameTable, string rel_name,
                                       gpos::BOOL fPartitioned = false);
    CTableDescriptor *lTabdescPlainWithColNameFormat(
        CMemoryPool *mp, IMDId *mdid, const WCHAR *wszColNameFormat,
        const CName &nameTable, string rel_name,
        gpos::BOOL is_nullable  // define nullable columns
    );

    inline CMDAccessor *lGetMDAccessor()
    {
        return COptCtxt::PoctxtFromTLS()->Pmda();
    }
    inline CMDIdGPDB *lGenRelMdid(uint64_t obj_id)
    {
        return GPOS_NEW(this->memory_pool)
            CMDIdGPDB(IMDId::EmdidRel, obj_id, 0, 0);
    }
    inline const IMDRelation *lGetRelMd(uint64_t obj_id)
    {
        return lGetMDAccessor()->RetrieveRel(lGenRelMdid(obj_id));
    }

    // helper functions
    bool lIsCastingFunction(std::string &func_name);
    CColRef *lCreateColRefFromName(std::string &name, const IMDType *mdid_type);
    CTableDescriptorArray *lGetTableDescriptorArrayFromOids(
        string &unique_name, vector<uint64_t> &oids);
    void lPruneUnnecessaryGraphlets(std::vector<uint64_t> &table_oids,
                                    NodeOrRelExpression *node_expr,
                                    const expression_vector &prop_exprs,
                                    std::vector<uint64_t> &pruned_table_oids);
	void lPruneUnnecessaryColumns(NodeOrRelExpression *node_expr,
                                    const expression_vector &prop_exprs,
                                    std::vector<uint64_t> &pruned_table_oids);

   private:
	// planner_physical.cpp
	/* Generating orca physical plan */
	void pGenPhysicalPlan(CExpression *orca_plan_root);
	bool pValidatePipelines();
	duckdb::CypherPhysicalOperatorGroups *pTraverseTransformPhysicalPlan(CExpression *plan_expr);
	
	// scan
	duckdb::CypherPhysicalOperatorGroups *pTransformEopTableScan(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopUnionAllForNodeOrEdgeScan(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopNormalTableScan(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopDSITableScan(CExpression *plan_expr);

	// union all
	duckdb::CypherPhysicalOperatorGroups *pTransformEopUnionAll(CExpression *plan_expr);

	// pipelined ops
	duckdb::CypherPhysicalOperatorGroups *pTransformEopProjectionColumnar(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalFilter(CExpression *plan_expr);

	// joins
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(CExpression *plan_expr, bool is_left_outer);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerIndexNLJoinToIdSeekNormal(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerIndexNLJoinToIdSeekDSI(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInner(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerIndexNLJoinToVarlenAdjIdxJoin(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalInnerNLJoinToCartesianProduct(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalNLJoinToBlockwiseNLJoin(CExpression *plan_expr, bool is_correlated = false);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalHashJoinToHashJoin(CExpression* plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopPhysicalMergeJoinToMergeJoin(CExpression* plan_expr);
	void pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithSortOrder(CExpression *plan_expr, duckdb::CypherPhysicalOperatorGroups *result);
	void pTransformEopPhysicalInnerIndexNLJoinToIdSeekForUnionAllInnerWithoutSortOrder(CExpression *plan_expr, duckdb::CypherPhysicalOperatorGroups *result);
	void pTransformEopPhysicalInnerIndexNLJoinToProjectionForUnionAllInner(CExpression *plan_expr, duckdb::CypherPhysicalOperatorGroups *result);

	// limit, sort
	duckdb::CypherPhysicalOperatorGroups *pTransformEopLimit(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopSort(CExpression *plan_expr);
	duckdb::CypherPhysicalOperatorGroups *pTransformEopTopNSort(CExpression *plan_expr);

	// shortestPath
	duckdb::CypherPhysicalOperatorGroups* pTransformEopShortestPath(CExpression* plan_expr);
	duckdb::CypherPhysicalOperatorGroups* pTransformEopAllShortestPath(CExpression* plan_expr);

	// aggregations
	duckdb::CypherPhysicalOperatorGroups *pTransformEopAgg(CExpression *plan_expr);

	// scalar expression
	unique_ptr<duckdb::Expression> pTransformScalarExpr(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarIdent(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarIdent(CExpression *scalar_expr, CColRefArray *child_cols, ULONG child_index);
	unique_ptr<duckdb::Expression> pTransformScalarConst(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarCmp(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarBoolOp(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarAggFunc(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarAggFunc(CExpression *scalar_expr, CColRefArray *child_cols, duckdb::LogicalType child_ref_type, int child_ref_idx, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarFunc(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarFunc(CExpression * scalar_expr, vector<unique_ptr<duckdb::Expression>>& child);
	unique_ptr<duckdb::Expression> pTransformScalarSwitch(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pTransformScalarNullTest(CExpression *scalar_expr, CColRefArray *child_cols, CColRefArray *rhs_child_cols = nullptr);
	unique_ptr<duckdb::Expression> pGenScalarCast(unique_ptr<duckdb::Expression> orig_expr, duckdb::LogicalType target_type);
	void pGetAllScalarIdents(CExpression * scalar_expr, vector<uint32_t> &sccmp_colids);
	void pConvertLocalFilterExprToUnionAllFilterExpr(unique_ptr<duckdb::Expression> &expr, CColRefArray* cols, vector<ULONG> unionall_output_original_col_ids);
	void pShiftFilterPredInnerColumnIndices(unique_ptr<duckdb::Expression> &expr, size_t outer_size);
	void pGetFilterOnlyInnerColsIdx(CExpression *filter_expr, CColRefArray *inner_cols, CColRefArray *output_cols, vector<ULONG> &inner_cols_idx);
	void pGetFilterOnlyInnerColsIdx(CExpression *filter_expr, CColRefSet *inner_cols, CColRefSet *output_cols, vector<const CColRef *> &filter_only_inner_cols);

	// investigate plan properties
	bool pMatchExprPattern(CExpression *root, vector<COperator::EOperatorId>& pattern, uint64_t pattern_root_idx=0, bool physical_op_only=false);
	bool pIsIndexJoinOnPhysicalID(CExpression *plan_expr);
	bool pIsUnionAllOpAccessExpression(CExpression *expr);
	bool pIsColumnarProjectionSimpleProject(CExpression *proj_expr);
	bool pIsFilterPushdownAbleIntoScan(CExpression *selection_expr);
	bool pIsCartesianProduct(CExpression *expr);
	
	// helper functions
	void pGenerateScanMapping(OID table_oid, CColRefArray *columns, vector<uint64_t>& out_mapping);
	void pGenerateTypes(CColRefArray *columns, vector<duckdb::LogicalType>& out_types);
	void pGenerateColumnNames(CColRefArray *columns, vector<string>& out_col_names);
	uint64_t pGetColIdxFromTable(OID table_oid, const CColRef *target_col);
	void pGenerateSchemaFlowGraph(duckdb::CypherPhysicalOperatorGroups &final_pipeline_ops);
	void pClearSchemaFlowGraph();
	void pInitializeSchemaFlowGraph();
	void pGenerateMappingInfo(vector<duckdb::idx_t> &scan_cols_id, duckdb::PropertyKeyID_vector *key_ids, vector<duckdb::LogicalType> &global_types,
		vector<duckdb::LogicalType> &local_types, vector<uint64_t> &projection_mapping, vector<uint64_t> &scan_projection_mapping);
	void pGenerateMappingInfo(vector<duckdb::idx_t> &scan_cols_id, duckdb::PropertyKeyID_vector *key_ids, vector<duckdb::LogicalType> &global_types,
		vector<duckdb::LogicalType> &local_types, vector<uint32_t>& union_inner_col_map, vector<uint32_t>& inner_col_map,
		vector<uint64_t> &projection_mapping, vector<uint64_t> &scan_projection_mapping, bool load_physical_id_col);
	void pBuildSchemaFlowGraphForSingleSchemaScan(duckdb::Schema &output_schema);
	void pBuildSchemaFlowGraphForMultiSchemaScan(duckdb::Schema &global_schema, vector<duckdb::Schema>& local_schemas);
	void pBuildSchemaFlowGraphForUnaryOperator(duckdb::Schema &output_schema);
	void pBuildSchemaFlowGraphForBinaryOperator(duckdb::Schema &output_schema, size_t num_rhs_schemas);
	OID pGetTableOidFromScanExpr(CExpression *scan_expr);

	void pConstructNodeScanParams(CExpression *projection_expr, vector<uint64_t>& oids, 
								vector<vector<uint64_t>>& projection_mapping, vector<vector<uint64_t>>& scan_projection_mapping,
								vector<duckdb::LogicalType>& global_types, vector<duckdb::Schema>& local_schemas);
	bool pConstructColumnInfosRegardingFilter(
		CExpression *projection_expr, vector<ULONG>& output_original_colref_ids,
		vector<duckdb::idx_t>& non_filter_only_column_idxs
	);
	void pConstructFilterColPosVals(
		CExpression *projection_expr, vector<int64_t> &pred_attr_poss, vector<duckdb::Value>& literal_vals
	);

	inline bool pIsPhysicalIdCol(CColRefTable *col) {
		return col->AttrNum() == INT(-1);
	}

	inline bool pIsComplexCondition(CExpression *expr) {
		return expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarBoolOp;
	}

	bool pFindOperandsColIdxs(CExpression *expr, CColRefArray *cols, duckdb::idx_t &out_idx);

	inline string pGetColNameFromColRef(const CColRef *column) {
		std::wstring name_ws(column->Name().Pstr()->GetBuffer());
		string name(name_ws.begin(), name_ws.end());
		return name;
	}
	inline duckdb::LogicalType pConvertTypeOidToLogicalType(OID oid, INT type_mod) {
		auto type_id = pConvertTypeOidToLogicalTypeId(oid);
		if (type_id == duckdb::LogicalTypeId::DECIMAL) {
			if (type_mod == 0 || type_mod == -1) {
				return duckdb::LogicalType::DECIMAL(12, 2);
			} 
			else {
				uint8_t width = (uint8_t)(type_mod >> 8);
				uint8_t scale = (uint8_t)(type_mod & 0xFF);
				return duckdb::LogicalType::DECIMAL(width, scale);
			}
		}
		else if (type_id == duckdb::LogicalTypeId::LIST) {
			if (type_mod == -1) {
				return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
			}
			INT child_type_oid = (type_mod & 0xFF) + LOGICAL_TYPE_BASE_ID;
			INT child_type_mod = (type_mod >> 8);
			return duckdb::LogicalType::LIST(pConvertTypeOidToLogicalType((OID)child_type_oid, child_type_mod));
		}
		else if (type_id == duckdb::LogicalTypeId::PATH) {
			return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
		}
		return duckdb::LogicalType(type_id);
	}
	inline duckdb::LogicalTypeId pConvertTypeOidToLogicalTypeId(OID oid) {
		return (duckdb::LogicalTypeId) static_cast<std::underlying_type_t<duckdb::LogicalTypeId>>((oid - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);
	}
	inline duckdb::LogicalType pConvertKuzuTypeToLogicalType(DataType type) {
		switch (type.typeID) {
		case DataTypeID::PATH:
			return duckdb::LogicalType::PATH(duckdb::LogicalType::UBIGINT);
		case DataTypeID::LIST:
			D_ASSERT(type.childType != nullptr);
			return duckdb::LogicalType::LIST(pConvertKuzuTypeToLogicalType(*type.childType));
		default:
			return duckdb::LogicalType((duckdb::LogicalTypeId)type.typeID);
		}
	}
	duckdb::CypherPhysicalOperatorGroups *pBuildSchemaflowGraphForBinaryJoin(CExpression *plan_expr, duckdb::CypherPhysicalOperator *op, duckdb::Schema& output_schema);
	duckdb::LogicalType pGetColumnsDuckDBType(const CColRef *column);
	void pGetColumnsDuckDBType(CColRefArray *columns, vector<duckdb::LogicalType>& out_types);
	void pGetColumnsDuckDBType(CColRefArray *columns, vector<duckdb::LogicalType>& out_types, vector<duckdb::idx_t>& col_prop_ids);
	void pGetProjectionExprs(vector<duckdb::LogicalType> output_types, vector<duckdb::idx_t>& ref_idxs, vector<unique_ptr<duckdb::Expression>>& out_exprs);
	void pRemoveColumnsFromSchemas(vector<duckdb::Schema>& in_schemas, vector<duckdb::idx_t>& ref_idxs, vector<duckdb::Schema>& out_schemas);
	bool pIsColEdgeProperty(const CColRef* colref);
	void pGenerateCartesianProductSchema(vector<duckdb::Schema>& lhs_schemas, vector<duckdb::Schema>& rhs_schemas, vector<duckdb::Schema>& out_schemas);
	bool pIsJoinRhsOutputPhysicalIdOnly(CExpression *expr);
	bool pCmpColName(const CColRef *colref, const WCHAR *col_name);

	// scalar helper functions
	void pTranslatePredicateToJoinCondition(CExpression* pred, vector<duckdb::JoinCondition>& out_conds, CColRefArray* lhs_cols, CColRefArray* rhs_cols);
	static duckdb::OrderByNullType pTranslateNullType(COrderSpec::ENullTreatment ent);
	static duckdb::ExpressionType pTranslateCmpType(IMDType::ECmpType cmp_type, bool contains_null = false);
	static duckdb::ExpressionType pTranslateBoolOpType(CScalarBoolOp::EBoolOperator op_type);
	static duckdb::JoinType pTranslateJoinType(COperator *op);
	static CColRef *pGetColRefFromScalarIdent(CExpression *ident_expr);
	static bool pIsCmpTypeRange(IMDType::ECmpType cmp_type);
	static bool pIsRangeCmpTypeOpposite(IMDType::ECmpType cmp_type, IMDType::ECmpType second_cmp_type);
	static bool pIsRangeCmpTypeLower(IMDType::ECmpType cmp_type);
	static bool pIsRangeCmpTypeUpper(IMDType::ECmpType cmp_type);
	static bool pIsRangeCmpTypeInclusive(IMDType::ECmpType cmp_type);

	// ID Get
	static OID pGetTypeIdFromScalar(CExpression *expr);
	static OID pGetTypeIdFromScalarIdent(CExpression *ident_expr);
	static OID pGetTypeIdFromScalarConst(CExpression *const_expr);
	static OID pGetTypeIdFromScalarFunc(CExpression *func_expr);
	static OID pGetTypeIdFromScalarAggFunc(CExpression *agg_expr);
	static OID pGetTypeIdFromScalarSwitch(CExpression *switch_expr);

	// Mod Get
	static INT pGetTypeModFromScalar(CExpression *expr);
	static INT pGetTypeModFromScalarIdent(CExpression *ident_expr);
	static INT pGetTypeModFromScalarConst(CExpression *const_expr);
	static INT pGetTypeModFromScalarFunc(CExpression *func_expr);
	static INT pGetTypeModFromScalarAggFunc(CExpression *agg_expr);
	static INT pGetTypeModFromScalarSwitch(CExpression *switch_expr);

	// Attr Value Get
	void pGetFilterAttrPosAndValue(CExpression *filter_pred_expr, gpos::ULONG &attr_pos, duckdb::Value &attr_value);
	void pGetFilterAttrPosAndValue(CExpression *filter_pred_expr, IMDId *mdid, gpos::ULONG &attr_pos, duckdb::Value &attr_value);

	// AdjIdxJoin Helpers
	bool pIsComplexPred(CExpression *pred_expr);
	CExpression *pFindFilterExpr(CExpression *plan_expr);
	CExpression *pFindIndexScanExpr(CExpression *plan_expr);
	bool pIsFilterExist(CExpression* plan_expr);
	bool pIsEdgePropertyInFilter(CExpression* plan_expr);
	bool pIsPropertyInCols(CColRefArray *cols);
	bool pIsIDColInCols(CColRefArray *cols);
	duckdb::idx_t pGetColIndexInPred(CExpression *pred, CColRefArray *colrefs);
	void pGetDuckDBTypesFromColRefs(CColRefArray *colrefs, vector<duckdb::LogicalType> &out_types);
	void pGetObjetIdsForColRefs(CColRefArray *cols, vector<uint64_t> &out_oids);
	void pPushCartesianProductSchema(duckdb::Schema& out_schema, vector<duckdb::LogicalType> &rhs_types);
	void pConstructColMapping(CColRefArray *in_cols, CColRefArray *out_cols, vector<uint32_t> &out_mapping);
	void pAppendFilterOnlyCols(CExpression *filter_expr, CColRefArray *input_cols, CColRefArray *output_cols, CColRefArray* result_cols);
	void pSeperatePropertyNonPropertyCols(CColRefSet *input_cols, CColRefSet *property_cols, CColRefSet* non_property_cols);
	void pGetFilterDuckDBExprs(CExpression *filter_expr, CColRefArray *outer_cols, CColRefArray *inner_cols, size_t index_shifting_size, vector<unique_ptr<duckdb::Expression>> &out_exprs);
	void pGetProjectionExprsWithJoinCond(CExpression *scalar_cmp_expr, CColRefArray *input_cols, CColRefArray *output_cols, vector<duckdb::LogicalType> output_types, vector<unique_ptr<duckdb::Expression>> &out_exprs);
	void pGetProjectionExprs(CColRefArray *input_cols, CColRefArray *output_cols, vector<duckdb::LogicalType> output_types, vector<unique_ptr<duckdb::Expression>> &out_exprs);
	CColRef* pGetIDColInCols(CColRefArray *cols);
	size_t pGetNumOuterSchemas();
	bool pIsAdjIdxJoinInto(CExpression *scalar_expr, CColRefSet *outer_cols, CColRefSet *inner_cols, CExpression *&adjidxjoin_into_expr);
	CExpression *reBuildFilterExpr(CExpression *filter_expr, CExpression *adjidxjoin_into_expr);
	CExpression *recursiveBuildFilterExpr(CExpression *scalar_expr, CExpression *adjidxjoin_into_expr);
	void pGetIdentIndices(unique_ptr<duckdb::Expression> &expr, vector<duckdb::idx_t> &out_idxs);
	duckdb::AdjIdxIdIdxs pGetAdjIdxIdIdxs(CColRefArray *inner_cols, IMDIndex::EmdindexType index_type);

	// Hash Aggregate Helpers
	void pUpdateProjAggExprs(CExpression* pexprScalarExpr, 
								vector<unique_ptr<duckdb::Expression>> &agg_exprs, 
								vector<unique_ptr<duckdb::Expression>> &agg_groups, 
								vector<unique_ptr<duckdb::Expression>> &proj_exprs,
								vector<duckdb::LogicalType>& agg_types,
								vector<duckdb::LogicalType>& proj_types,
								CColRefArray* child_cols, 
								bool& adjust_agg_groups_performed, 
								bool &has_pre_projection);
	void pAdustAggGroups( vector<unique_ptr<duckdb::Expression>>& agg_groups,  vector<unique_ptr<duckdb::Expression>> &agg_exprs);

	// Filter DNF Transformation
	CExpression* pPredToDNF(CExpression *pred);
	CExpression* pDistributeANDOverOR(CExpression *a, CExpression *b);

private:
	// config
	const PlannerConfig config;
	const MDProviderType mdp_type;
	const std::string memory_mdp_filepath;

	// core
	duckdb::ClientContext *context;	// TODO this should be reference - refer to plansuite
	CMemoryPool *memory_pool;

	// logical plnaning
	vector<duckdb::PropertyKeyID> pruned_key_ids;

	// used and initialized in each execution
	BoundStatement *bound_statement;											// input parse statemnt
	vector<duckdb::CypherPipeline *> pipelines;									// output plan pipelines
	std::map<CColRef *, std::string> property_col_to_output_col_names_mapping; 	// actual output col names for property columns
	vector<std::string> logical_plan_output_col_names;							// output col names
	vector<OID> logical_plan_output_col_oids;									// output col oids			
	std::vector<CColRef*> logical_plan_output_colrefs;							// final output colrefs of the logical plan (user's view)
	std::vector<CColRef*> physical_plan_output_colrefs;							// final output colrefs of the physical plan
	
	// logical soptimization context
	bool l_is_outer_plan_registered;		// whether subquery opt context can access outer plan
	LogicalPlan* l_registered_outer_plan;	// registered plan

	// schema flow graph
	PipelineOperatorTypes pipeline_operator_types;
	PipelineNumSchemas num_schemas_of_childs;
	PipelineSchemas pipeline_schemas;
	PipelineSchemas other_source_schemas; // @jhha: temporal impl. please remove
	PipelineUnionSchema pipeline_union_schema;
	vector<duckdb::SchemaFlowGraph> sfgs;
	bool generate_sfg = false;
	bool restrict_generate_sfg_for_unionall = false;

	// dynamic schema instantiation
	vector<CExpression *> output_expressions_to_be_refined;
	vector<NodeOrRelExpression *> node_or_rel_expressions_to_be_refined;
	// vector<CColRef *> colrefs_for_dsi; // should include columns used for grouping key / join column
	CColRefSet *colrefs_for_dsi = nullptr;
	bool analyze_ongoing = false;

	// md provider
	gpmd::MDProviderTBGPP *provider = nullptr;

	// const variables for system columns
	string ID_COLNAME = "_id";
	string SID_COLNAME = "_sid";
	string TID_COLNAME = "_tid";
	const WCHAR *w_id_col_name = L"_id";
	const WCHAR *w_sid_col_name = L"_sid";
	const WCHAR *w_tid_col_name = L"_tid";
	CName *id_col_cname;
	CName *sid_col_cname;
	CName *tid_col_cname;
	uint64_t ID_COLNAME_ID = 0;
	uint64_t SID_COLNAME_ID;
	uint64_t TID_COLNAME_ID;
};

}