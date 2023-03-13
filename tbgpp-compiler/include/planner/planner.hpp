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
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/operators/CPhysicalTableScan.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalSerialUnionAll.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"
#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"
#include "gpopt/operators/CPhysicalInnerNLJoin.h"
#include "gpopt/operators/CPhysicalComputeScalarColumnar.h"


#include "naucrates/init.h"
#include "naucrates/traceflags/traceflags.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "kuzu/binder/bound_statement.h"
#include "kuzu/binder/query/reading_clause/bound_reading_clause.h"
#include "kuzu/binder/query/reading_clause/bound_match_clause.h"
#include "kuzu/binder/expression/expression.h"

#include "execution/cypher_pipeline.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "BTNode.h"
#include "planner/logical_plan.hpp"
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
	bool DEBUG_PRINT;
	bool ORCA_DEBUG_PRINT;
	bool DISABLE_HASH_JOIN;

	PlannerConfig() :
		DEBUG_PRINT(false),
		ORCA_DEBUG_PRINT(false),
		DISABLE_HASH_JOIN(false)
		{ }
};

class Planner {

public:
	Planner(PlannerConfig config, MDProviderType mdp_type, duckdb::ClientContext* context, string memory_mdp_path = "");	// TODO change client signature to reference
	~Planner();

	void execute(BoundStatement* bound_statement);
	inline vector<duckdb::CypherPipeline*> getConstructedPipelines() { return pipelines; }

private:
	// planner.cpp
	/* Orca Related */
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
	CExpression* lGetLogicalPlan();
	CExpression* lPlanSingleQuery(const NormalizedSingleQuery& singleQuery);
	LogicalPlan* lPlanQueryPart(
        const NormalizedQueryPart& queryPart, LogicalPlan* prev_plan);
	LogicalPlan* lPlanProjectionBody(LogicalPlan* plan, BoundProjectionBody* proj_body);
	LogicalPlan* lPlanReadingClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan* lPlanMatchClause(
		BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan* lPlanUnwindClause(
        BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan);
	LogicalPlan* lPlanRegularMatch(const QueryGraphCollection& queryGraphCollection, LogicalPlan* prev_plan);
	LogicalPlan* lPlanNodeOrRelExpr(NodeOrRelExpression* node_expr, bool is_node);
	LogicalPlan* lPlanProjectionOnColIds(LogicalPlan* plan, vector<uint64_t>& col_ids);
	
	/* Helper functions for generating orca logical plans */
	CExpression* lExprLogicalGetNodeOrEdge(
		string name, vector<uint64_t> oids,
		map<uint64_t, map<uint64_t, uint64_t>> * schema_proj_mapping, bool insert_projection
	);

	CExpression * lExprLogicalGet(uint64_t obj_id, string rel_name, string alias = "");
	CExpression * lExprLogicalUnionAllWithMapping(CExpression* lhs, CColRefArray* lhs_mapping, CExpression* rhs, CColRefArray* rhs_mapping);
	CExpression * lExprLogicalUnionAll(CExpression* lhs, CExpression* rhs);

	std::pair<CExpression*, CColRefArray*> lExprScalarAddSchemaConformProject(
		CExpression* relation, vector<uint64_t> col_ids_to_project,
		vector<pair<IMDId*, gpos::INT>>* target_schema_types
	);
	CExpression* lExprLogicalJoinOnId(CExpression* lhs, CExpression* rhs,
		uint64_t lhs_pos, uint64_t rhs_pos, bool project_out_lhs_key=false, bool project_out_rhs_key=false);
	CExpression* lExprLogicalCartProd(CExpression* lhs, CExpression* rhs);
	
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
	
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopTableScan(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopProjectionColumnar(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopUnionAllForNodeOrEdgeScan(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(CExpression* plan_expr);
	vector<duckdb::CypherPhysicalOperator*>* pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression* plan_expr);

	bool pIsIndexJoinOnPhysicalID(CExpression* plan_expr);

	bool pMatchExprPattern(CExpression* root, vector<COperator::EOperatorId>& pattern, uint64_t pattern_root_idx=0, bool physical_op_only=false);
	bool pIsUnionAllOpAccessExpression(CExpression* expr);
	
	uint64_t pGetColIdxOfColref(CColRefSet* refset, const CColRef* target_col);
	uint64_t pGetColIdxFromTable(OID table_oid, const CColRef* target_col);

	inline duckdb::LogicalType pConvertTypeOidToLogicalType(OID oid) {
		return duckdb::LogicalType( pConvertTypeOidToLogicalTypeId(oid) );
	}
	inline duckdb::LogicalTypeId pConvertTypeOidToLogicalTypeId(OID oid) {
		auto type_id = static_cast<std::underlying_type_t<duckdb::LogicalTypeId>>(oid);
		return (duckdb::LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID);
	}

private:
	// config
	const PlannerConfig config;
	const MDProviderType mdp_type;
	const std::string memory_mdp_filepath;

	// core
	duckdb::ClientContext* context;	// TODO this should be reference - refer to plansuite
	CMemoryPool* memory_pool;
	std::map<OID, std::vector<CColRef*>> table_col_mapping;

	BoundStatement* bound_statement;			// input parse statemnt
	vector<duckdb::CypherPipeline*> pipelines;	// output plan pipelines
};

}