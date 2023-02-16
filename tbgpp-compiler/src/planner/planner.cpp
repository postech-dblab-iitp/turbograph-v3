#include "gpopt/operators/CLogicalUnionAll.h"

#include "planner.hpp"
#include "mdprovider/MDProviderTBGPP.h"

#include <string>
#include <limits>


namespace s62 {

Planner::Planner(MDProviderType mdp_type, duckdb::ClientContext* context, std::string memory_mdp_path)
	: mdp_type(mdp_type), context(context), memory_mdp_filepath(memory_mdp_path) {

	if(mdp_type == MDProviderType::MEMORY) {
		assert(memory_mdp_filepath != "");
		//  "filepath should be provided in memory provider mode"
	}
	this->orcaInit();	
}

Planner::~Planner() {

	// Destroy memory pool for orca
	CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(this->memory_pool);
}

void Planner::orcaInit() {
	struct gpos_init_params gpos_params = {NULL};
	gpos_init(&gpos_params);
	gpdxl_init();
	gpopt_init();

	// Allocate memory pool for orca
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();
	amp.Detach();
	this->memory_pool = mp;

}

CQueryContext* Planner::_orcaGenQueryCtxt(CMemoryPool* mp, CExpression* logical_plan) {
	CQueryContext *result = nullptr;
	
	D_ASSERT(logical_plan != nullptr);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(logical_plan->DeriveOutputColumns());
	// keep a subset of columns
	CColRefSet *pcrsOutput = GPOS_NEW(mp) CColRefSet(mp);
	CColRefSetIter crsi(*pcrs);
	while (crsi.Advance()) {
		CColRef *colref = crsi.Pcr();
		if (1 != colref->Id() % GPOPT_TEST_REL_WIDTH) {
			pcrsOutput->Include(colref);
		}
	}
	pcrs->Release();

	// construct an ordered array of the output columns
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	CColRefSetIter crsiOutput(*pcrsOutput);
	while (crsiOutput.Advance()) {
		CColRef *colref = crsiOutput.Pcr();
		colref_array->Append(colref);
	}
	// generate a sort order
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	// no sort constraint
	// pos->Append(GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidGeneral, GPDB_INT4_LT_OP),
	// 			pcrsOutput->PcrAny(), COrderSpec::EntFirst);
	// TODO error do we need to 
	// CDistributionSpec *pds = GPOS_NEW(mp)
	// 	CDistributionSpecStrictSingleton(CDistributionSpecSingleton::EstMaster);
	CDistributionSpec *pds = GPOS_NEW(mp)
		CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	CRewindabilitySpec *prs = GPOS_NEW(mp) CRewindabilitySpec(
		CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	// we require exact matching on distribution since final query results must be sent to master
	CEnfdDistribution *ped =
		GPOS_NEW(mp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per =
		GPOS_NEW(mp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PcterProducers(mp);
	CReqdPropPlan *prpp =
		GPOS_NEW(mp) CReqdPropPlan(pcrsOutput, peo, ped, per, pcter);
	CMDNameArray *pdrgpmdname = GPOS_NEW(mp) CMDNameArray(mp);
	const ULONG length = colref_array->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*colref_array)[ul];
		CMDName *mdname = GPOS_NEW(mp) CMDName(mp, colref->Name().Pstr());
		pdrgpmdname->Append(mdname);
	}

	result = GPOS_NEW(mp) CQueryContext(mp, logical_plan, prpp, colref_array,
									pdrgpmdname, true /*fDeriveStats*/);

	return result;
}

void Planner::execute(kuzu::binder::BoundStatement* bound_statement) {
	
	D_ASSERT(bound_statement != nullptr);
	this->bound_statement = bound_statement;
	gpos_exec_params params;
	{
		params.func = Planner::_orcaExec;
		params.arg = this;
		params.stack_start = &params;
		params.error_buffer = NULL;
		params.error_buffer_size = -1;
		params.abort_requested = NULL;
	}
	
	auto gpos_output_code = gpos_exec(&params);
	return;
}

CMDProviderMemory* Planner::_orcaGetProviderMemory() {
	CMDProviderMemory * provider = nullptr;
	CMemoryPool *mp = nullptr; 
	{
		CAutoMemoryPool amp;
		mp = amp.Pmp();
		std::string md_path = this->memory_mdp_filepath;
		provider = new (mp, __FILE__, __LINE__) CMDProviderMemory(mp, md_path.c_str());
		// detach safety
		(void) amp.Detach();
	}
	D_ASSERT( provider != nullptr );
	provider->AddRef();

	return provider;
}

gpmd::MDProviderTBGPP* Planner::_orcaGetProviderTBGPP() {
	gpmd::MDProviderTBGPP * provider = nullptr;
	CMemoryPool *mp = nullptr; 
	{
		CAutoMemoryPool amp;
		mp = amp.Pmp();
		provider = new (mp, __FILE__, __LINE__) gpmd::MDProviderTBGPP(mp);
		// detach safety
		(void) amp.Detach();
	}
	D_ASSERT( provider != nullptr );
	provider->AddRef();

	return provider;
}


void Planner::_orcaInitXForm() {
	// TODO i want to remove this.
	CXformFactory::Pxff()->Shutdown();	// for allowing scan
	GPOS_RESULT eres = CXformFactory::Init();
}

void Planner::_orcaSetTraceFlags() {
	/* Set AutoTraceFlags */
	// refer to CConfigParamMapping.cpp
	//CAutoTraceFlag atf(gpos::EOptTraceFlag::EopttraceDisableMotions, true /*fSet*/);
	//CAutoTraceFlag atf(gpos::EOptTraceFlag::EopttraceDisableMotionBroadcast, true /*fSet*/);
	//CAutoTraceFlag atf2(gpos::EOptTraceFlag::EopttraceDisableMotionGather, true /*fSet*/);
	// CAutoTraceFlag atf3(gpos::EOptTraceFlag::EopttracePrintXform, true /*fSet*/);
	// CAutoTraceFlag atf4(gpos::EOptTraceFlag::EopttracePrintPlan, true /*fSet*/);
	// CAutoTraceFlag atf5(gpos::EOptTraceFlag::EopttracePrintMemoAfterExploration, true /*fSet*/);
	// CAutoTraceFlag atf6(gpos::EOptTraceFlag::EopttracePrintMemoAfterImplementation, true /*fSet*/);
	// CAutoTraceFlag atf7(gpos::EOptTraceFlag::EopttracePrintMemoAfterOptimization, true /*fSet*/);
	// CAutoTraceFlag atf8(gpos::EOptTraceFlag::EopttracePrintMemoEnforcement, true /*fSet*/);
	CAutoTraceFlag atf9(gpos::EOptTraceFlag::EopttraceEnumeratePlans, true /*fSet*/);
	CAutoTraceFlag atf10(gpos::EOptTraceFlag::EopttracePrintOptimizationContext, true /*fSet*/);
}

gpdbcost::CCostModelGPDB* Planner::_orcaGetCostModel(CMemoryPool* mp) {
	auto m_cost_model_params = GPOS_NEW(mp) CCostModelParamsGPDB(mp);
	m_cost_model_params->SetParam(39, 1.0, 1.0, 1.0);	// single machine cost - may need to make this alive
	m_cost_model_params->SetParam(13, 0.0, 0.0, 1000000.0);	// gather cost
	m_cost_model_params->SetParam(14, 0.0, 0.0, 1000000.0);	// gather cost
	m_cost_model_params->SetParam(15, 10000000.0, 10000000.0, 10000000.0);	// redistribute cost
	m_cost_model_params->SetParam(16, 10000000.0, 10000000.0, 10000000.0);	// redistribute cs
	m_cost_model_params->SetParam(17, 10000000.0, 10000000.0, 10000000.0);	// broadcast cost
	m_cost_model_params->SetParam(18, 10000000.0, 10000000.0, 10000000.0);	// broadcast cost

	gpdbcost::CCostModelGPDB* pcm = GPOS_NEW(mp) CCostModelGPDB(mp, 1, m_cost_model_params);	// one segment
	D_ASSERT(pcm != nullptr);

	return pcm;
}

void Planner::_orcaSetOptCtxt(CMemoryPool* mp, CMDAccessor* mda, gpdbcost::CCostModelGPDB* pcm) {
	COptimizerConfig *optimizer_config =
		COptimizerConfig::PoconfDefault(mp, pcm);
	// use the default constant expression evaluator which cannot evaluate any expression
	IConstExprEvaluator * pceeval = GPOS_NEW(mp) CConstExprEvaluatorDefault();
	COptCtxt *poctxt =
		COptCtxt::PoctxtCreate(mp, mda, pceeval, optimizer_config);
	poctxt->SetHasMasterOnlyTables();
	ITask::Self()->GetTls().Store(poctxt);
}

void * Planner::_orcaExec(void* planner_ptr) {

	Planner* planner = (Planner*) planner_ptr;
	CMemoryPool* mp = planner->memory_pool;
	
	/* Initialize */
	InitDXL();
	planner->_orcaInitXForm();
	CMDCache::Init();
	IMDProvider* provider;
	if( planner->mdp_type == MDProviderType::MEMORY ) {
		provider = (IMDProvider*) planner->_orcaGetProviderMemory();
	} else {
		provider = (IMDProvider*) planner->_orcaGetProviderTBGPP();
	}
	/* Core area */ 
	{	// this area should be enforced
		/* Optimizer components */
		planner->_orcaSetTraceFlags();
		CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, provider);
		gpdbcost::CCostModelGPDB* pcm = planner->_orcaGetCostModel(mp);
		planner->_orcaSetOptCtxt(mp, &mda, pcm);
		
		/* Optimize */
		CExpression *orca_logical_plan = planner->lGetLogicalPlan();
		{
			std::cout << "[TEST] logical plan string" << std::endl;
			CWStringDynamic str(mp);
			COstreamString oss(&str);
			orca_logical_plan->OsPrint(oss);
			GPOS_TRACE(str.GetBuffer());
		}
		CEngine eng(mp);
		CQueryContext* pqc = planner->_orcaGenQueryCtxt(mp, orca_logical_plan);
		eng.Init(pqc, NULL /*search_stage_array*/);
		eng.Optimize();
		CExpression *orca_physical_plan = eng.PexprExtractPlan();	// best plan
		{
			std::cout << "[TEST] best physical plan string" << std::endl;
			CWStringDynamic str(mp);
			COstreamString oss(&str);
			orca_physical_plan->OsPrint(oss);
			GPOS_TRACE(str.GetBuffer());
		}
		// TODO convert orca plan into ours
		orca_logical_plan->Release();
		orca_physical_plan->Release();
		GPOS_DELETE(pqc);

		/* Terminate */
		CTaskLocalStorageObject *ptlsobj =
			ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt);
		ITask::Self()->GetTls().Remove(ptlsobj);
		GPOS_DELETE(ptlsobj);
	}

	/* Shutdown */
	CRefCount::SafeRelease(provider);
	CMDCache::Shutdown();
	
	return nullptr;
}

CExpression* Planner::lGetLogicalPlan() {

	D_ASSERT( this->bound_statement != nullptr );
	auto& regularQuery = *((BoundRegularQuery*)(this->bound_statement));

	// TODO need union between single queries
	D_ASSERT( regularQuery.getNumSingleQueries() == 1);
	vector<CExpression*> childLogicalPlans(regularQuery.getNumSingleQueries());
	for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
		childLogicalPlans[i] = lPlanSingleQuery(*regularQuery.getSingleQuery(i));
	}
	return childLogicalPlans[0];
}

CExpression* Planner::lPlanSingleQuery(const NormalizedSingleQuery& singleQuery) {

	// TODO refer kuzu properties to scan
		// populate properties to scan
    	// propertiesToScan.clear();

	CExpression* plan;
	D_ASSERT(singleQuery.getNumQueryParts() == 1);
	for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
		// TODO plan object may need to be pushed up once again. thus the function signature need to be changed as well.
        auto plan_obj = lPlanQueryPart(*singleQuery.getQueryPart(i), nullptr);
		plan = plan_obj->getPlanExpr();
    }
	return plan;
}

LogicalPlan* Planner::lPlanQueryPart(
	const NormalizedQueryPart& queryPart, LogicalPlan* prev_plan) {

	LogicalPlan* cur_plan = prev_plan;
	for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
        cur_plan = lPlanReadingClause(queryPart.getReadingClause(i), cur_plan);
    }
	D_ASSERT( queryPart.getNumUpdatingClause() == 0);
	// plan projectionBody after reading clauses
	if (queryPart.hasProjectionBody()) {
		// Plan normalized filter
		if (queryPart.hasProjectionBodyPredicate()) {
			// TODO S62 fixme. wtf is this?
			// Need to check semantics on whether filter or project should be planned first on behalf of other.
				// maybe filter first?
			D_ASSERT(false); // filter not yet implemented.
			// appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
        }
		// Plan normalized projection
		cur_plan = lPlanProjectionBody(cur_plan, queryPart.getProjectionBody());
    }
	return cur_plan;
}

LogicalPlan* Planner::lPlanProjectionBody(LogicalPlan* plan, BoundProjectionBody* proj_body) {

	/* Aggregate - generate LogicalGbAgg series */
	if(proj_body->hasAggregationExpressions()) {
		D_ASSERT(false);
		// TODO plan is manipulated
		// maybe need to split function without agg and with agg.
	}

	/* Scalar projection - using CLogicalProject */
		// find all projection expressions that requires new columns
		// generate logicalproiection and record the mappings

	// maintain new mappings

	/* Simple projection - switch orders between columns; use lPlanProjectionOnColIds */
	vector<uint64_t> simple_proj_colids;
	const auto& proj_exprs = proj_body->getProjectionExpressions();
	for( auto& proj_expr: proj_exprs) {
		auto& proj_expr_type = proj_expr.get()->expressionType;
		if(proj_expr_type == kuzu::common::ExpressionType::PROPERTY) {
			// first depth projection = simple projection
			PropertyExpression* prop_expr = (PropertyExpression*)(proj_expr.get());
			string k1 = prop_expr->getVariableName();
			string k2 = prop_expr->getPropertyName();
			uint64_t idx = plan->getSchema()->getIdxOfKey(k1, k2);
			simple_proj_colids.push_back(idx);
		} else {
			// currently do not allow other cases
			D_ASSERT(false);
		}
	}
	plan = lPlanProjectionOnColIds(plan, simple_proj_colids);
	
	/* OrderBy */
	if(proj_body->hasOrderByExpressions()) {
		// orderByExpressions
		// isAscOrders
		D_ASSERT(false);
	}
	
	/* Skip limit */
	if( proj_body->hasSkipOrLimit() ) {
		// CLogicalLimit
		D_ASSERT(false);
	}

	/* Distinct */
	if(proj_body->getIsDistinct()) {
		D_ASSERT(false);
	}

	D_ASSERT(plan != nullptr);
	return plan;
}

LogicalPlan* Planner::lPlanReadingClause(
	BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan) {

	LogicalPlan* plan;

	auto readingClauseType = boundReadingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        plan = lPlanMatchClause(boundReadingClause, prev_plan);
    } break;
    case ClauseType::UNWIND: {
        plan = lPlanUnwindClause(boundReadingClause, prev_plan);
    } break;
    default:
        D_ASSERT(false);
    }

	return plan;
}

LogicalPlan* Planner::lPlanMatchClause(
	BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan) {

	auto boundMatchClause = (BoundMatchClause*)boundReadingClause;
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    expression_vector predicates = boundMatchClause->hasWhereExpression() ?
                          boundMatchClause->getWhereExpression()->splitOnAND() :
                          expression_vector{};

	LogicalPlan* plan;
    if (boundMatchClause->getIsOptional()) {
        D_ASSERT(false);
		// TODO optionalmatch
    } else {
		plan = lPlanRegularMatch(*queryGraphCollection, predicates, prev_plan);
    }

	// TODO append edge isomorphism
		// TODO need to know about the label info...
		// for each qg in qgc, 
			// list all edges
				// nested for loops

	return plan;
}

LogicalPlan* Planner::lPlanUnwindClause(
	BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan) {
	D_ASSERT(false);
	return nullptr;
}

LogicalPlan* Planner::lPlanRegularMatch(const QueryGraphCollection& qgc,
	expression_vector& predicates, LogicalPlan* prev_plan) {

	LogicalPlan* plan = nullptr;

	string ID_COLNAME = "_id";
	string SID_COLNAME = "_sid";
	string TID_COLNAME = "_tid";


	LogicalPlan* qg_plan = prev_plan;

	D_ASSERT( qgc.getNumQueryGraphs() > 0 );
	for(int idx=0; idx < qgc.getNumQueryGraphs(); idx++){
		QueryGraph* qg = qgc.getQueryGraph(idx);

		for(int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
			RelExpression* qedge = qg->getQueryRel(edge_idx).get();
			NodeExpression* lhs = qedge->getSrcNode().get();
			NodeExpression* rhs = qedge->getDstNode().get();
			string edge_name = qedge->getUniqueName();
			string lhs_name = qedge->getSrcNode()->getUniqueName();
			string rhs_name = qedge->getDstNode()->getUniqueName();

			bool is_lhs_bound = false;
			bool is_rhs_bound = false;
			if(qg_plan != nullptr) {
				is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name) ? true : false;
				is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name) ? true : false;
			}

			LogicalPlan* hop_plan;

			LogicalPlan* lhs_plan;
			// A join R
			if( !is_lhs_bound ) {
				lhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)lhs, true);
			} else {
				// lhs bound
				lhs_plan = qg_plan;
			}
			D_ASSERT(lhs_plan != nullptr);
			// Scan R
			LogicalPlan* edge_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)qedge, false);
			
			// TODO need to make this as a function
			auto join_expr = lExprLogicalJoinOnId(lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
				lhs_plan->getSchema()->getIdxOfKey(lhs_name, ID_COLNAME),
				edge_plan->getSchema()->getIdxOfKey(edge_name, SID_COLNAME)
			);
			lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
			lhs_plan->addBinaryParentOp(join_expr, edge_plan);
			
			// R join B
			if( is_lhs_bound && is_rhs_bound ) {
				// no join necessary - add filter predicate
				D_ASSERT(false);
				hop_plan = qg_plan; // TODO fixme
			} else {
				LogicalPlan* rhs_plan;
				// join necessary
				if(!is_rhs_bound) {
					rhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)rhs, true);
				} else {
					// lhs unbound and rhs bound
					rhs_plan = qg_plan;
				}
				// (AR) join B
				auto join_expr = lExprLogicalJoinOnId(lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
					lhs_plan->getSchema()->getIdxOfKey(edge_name, TID_COLNAME),
					rhs_plan->getSchema()->getIdxOfKey(rhs_name, ID_COLNAME)
				);
				lhs_plan->getSchema()->appendSchema(rhs_plan->getSchema());
				lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
				hop_plan = lhs_plan;
			}
			D_ASSERT(hop_plan != nullptr);
			// When lhs, rhs is unbound, qg_plan is not merged with the hop_plan. Thus cartprod.
			if ( (qg_plan != nullptr) && (!is_lhs_bound) && (!is_rhs_bound)) {
				auto cart_expr = lExprLogicalCartProd(qg_plan->getPlanExpr(), hop_plan->getPlanExpr());
				qg_plan->getSchema()->appendSchema(hop_plan->getSchema());
				qg_plan->addBinaryParentOp(cart_expr, hop_plan);
			} else {
				qg_plan = hop_plan;
			}
			D_ASSERT(qg_plan != nullptr);
		}
		// if no edge, this is single node scan case
		if(qg->getQueryNodes().size() == 1) {
			LogicalPlan* nodescan_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)qg->getQueryNodes()[0].get(), true);
			if(qg_plan == nullptr) {
				qg_plan = nodescan_plan;
			} else {
				// cartprod
				auto cart_expr = lExprLogicalCartProd(qg_plan->getPlanExpr(), nodescan_plan->getPlanExpr());
				qg_plan->getSchema()->appendSchema(nodescan_plan->getSchema());
				qg_plan->addBinaryParentOp(cart_expr, nodescan_plan);
			}
		}
		D_ASSERT(qg_plan != nullptr);
	}

	return qg_plan;

}

LogicalPlan* Planner::lPlanProjectionOnColIds(LogicalPlan* plan, vector<uint64_t>& col_ids) {

	CMemoryPool* mp = this->memory_pool;

	// refer to CXformExpandFullOuterJoin.cpp
	CColRefArray *pdrgpcrOutput = GPOS_NEW(mp) CColRefArray(mp);
	CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);
	
	// output columns of the union
	CColRefArray* plan_cols = plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp);
	for(auto& col_id: col_ids){
		D_ASSERT( col_id < plan_cols->Size() );
		pdrgpcrOutput->Append(plan_cols->operator[](col_id));
	}
	pdrgpcrOutput->AddRef();
	pdrgdrgpcrInput->Append(pdrgpcrOutput);	// UnionAll with only one input relation
	
	// This is a trick to generate simple projection expression (columnar) in the row-major Orca.
	CExpression* project_expr = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgdrgpcrInput),
		plan->getPlanExpr());	// Unary unionall!

	plan->addUnaryParentOp(project_expr);
	return plan;
}

LogicalPlan* Planner::lPlanNodeOrRelExpr(NodeOrRelExpression* node_expr, bool is_node) {

	auto table_oids = node_expr->getTableIDs();
	D_ASSERT(table_oids.size() >= 1);

	map<uint64_t, map<uint64_t, uint64_t>> schema_proj_mapping;	// maps from new_col_id->old_col_id
	for( auto& t_oid: table_oids) {
		schema_proj_mapping.insert({t_oid, map<uint64_t, uint64_t>()});
	}
	D_ASSERT(schema_proj_mapping.size() == table_oids.size());

	// these properties include system columns (e.g. _id)
	auto& prop_exprs = node_expr->getPropertyExpressions();
	for( int colidx=0; colidx < prop_exprs.size(); colidx++) {
		auto& _prop_expr = prop_exprs[colidx];
		PropertyExpression* expr = static_cast<PropertyExpression*>(_prop_expr.get());
		for( auto& t_oid: table_oids) {
			if( expr->hasPropertyID(t_oid) ) {
				// table has property
				schema_proj_mapping.find(t_oid)->
					second.insert({(uint64_t)colidx, (uint64_t)(expr->getPropertyID(t_oid))});
			} else {
				// need to be projected as null column
				schema_proj_mapping.find(t_oid)->
					second.insert({(uint64_t)colidx, std::numeric_limits<uint64_t>::max()});
			}
		}
	}
	
	// get plan
	CExpression* plan_expr;
	auto node_name = node_expr->getUniqueName();
	auto node_name_print = node_expr->getRawName();
	if( table_oids.size() == 1) {
		// no schema projection necessary
		plan_expr = lExprLogicalGetNodeOrEdge(node_name_print, table_oids);
	} else {
		// generate scan with projection mapping
		plan_expr = lExprLogicalGetNodeOrEdge(node_name_print, table_oids, &schema_proj_mapping);	// id, vp1, vp2
	}

	// insert node schema
	LogicalSchema schema;
	// _id and properties
	for(int col_idx = 0; col_idx < prop_exprs.size(); col_idx++ ) {
		auto& _prop_expr = prop_exprs[col_idx];
		PropertyExpression* expr = static_cast<PropertyExpression*>(_prop_expr.get());
		string expr_name = expr->getRawName();
		if( is_node ) {
			schema.appendNodeProperty(node_name, expr_name);
		}  else {
			schema.appendEdgeProperty(node_name, expr_name);
		}
	}
	D_ASSERT( schema.getNumPropertiesOfKey(node_name) == prop_exprs.size() );

	LogicalPlan* plan = new LogicalPlan(plan_expr, schema);
	D_ASSERT( !plan->getSchema()->isEmpty() );
	return plan;
}

CExpression* Planner::lExprLogicalGetNodeOrEdge(string name, vector<uint64_t> relation_oids,
	map<uint64_t, map<uint64_t, uint64_t>>* schema_proj_mapping) {
	
	CMemoryPool* mp = this->memory_pool;

	CExpression* union_plan = nullptr;
	const bool do_schema_mapping = (schema_proj_mapping != nullptr);
	D_ASSERT(relation_oids.size() > 0);

	// generate type hints to the projected schema
	uint64_t num_proj_cols;								// size of the union schema
	vector<pair<gpmd::IMDId*, gpos::INT>> union_schema_types;	// mdid type and type modifier for both types
	if(do_schema_mapping) {
		num_proj_cols =
			(*schema_proj_mapping)[relation_oids[0]].size() > 0
				? (*schema_proj_mapping)[relation_oids[0]].rbegin()->first + 1
				: 0;
		// iterate schema_projection mapping
		for( int col_idx = 0; col_idx < num_proj_cols; col_idx++) {
			// foreach mappings
			uint64_t valid_oid;
			uint64_t valid_cid = std::numeric_limits<uint64_t>::max();

			for( auto& oid: relation_oids) {
				uint64_t idx_to_try = (*schema_proj_mapping)[oid].find(col_idx)->second;
				if (idx_to_try != std::numeric_limits<uint64_t>::max() ) {
					valid_oid = oid;
					valid_cid = idx_to_try;
				}
			}			
			D_ASSERT(valid_cid != std::numeric_limits<uint64_t>::max());
			// extract info and maintain vector of column type infos
			gpmd::IMDId* col_type_imdid = lGetRelMd(valid_oid)->GetMdCol(valid_cid)->MdidType();
			gpos::INT col_type_modifier = lGetRelMd(valid_oid)->GetMdCol(valid_cid)->TypeModifier();
			union_schema_types.push_back( make_pair(col_type_imdid, col_type_modifier) );
		}
	}

	CColRefArray* idx0_output_array;
	for( int idx = 0; idx < relation_oids.size(); idx++) {
		auto& oid = relation_oids[idx];

		CExpression * expr;
		const gpos::ULONG num_cols = lGetRelMd(oid)->ColumnCount();
		icecream::ic.enable(); IC(); IC(oid, num_cols); icecream::ic.disable();

		D_ASSERT(num_cols != 0);
		expr = lExprLogicalGet(oid, name, num_cols);

		// conform schema if necessary
		CColRefArray* output_array;
		if(do_schema_mapping) {
			auto& mapping = (*schema_proj_mapping)[oid];
			assert(num_proj_cols == mapping.size());
			vector<uint64_t> project_cols;
			for(int proj_col_idx = 0; proj_col_idx < num_proj_cols; proj_col_idx++) {
				project_cols.push_back(mapping.find(proj_col_idx)->second);
			}
			D_ASSERT(project_cols.size() > 0);
			//expr = lExprScalarAddSchemaConformProject(expr, project_cols, &union_schema_types);
			auto proj_result = lExprScalarAddSchemaConformProject(expr, project_cols, &union_schema_types);
			expr = proj_result.first;
			output_array = proj_result.second;
		} else {
			output_array = expr->DeriveOutputColumns()->Pdrgpcr(mp);
		}

		// add union
		if(idx == 0) {
			// REL
			union_plan = expr;
			idx0_output_array = output_array;
		} else if (idx == 1) {
			// REL + REL
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, expr, idx0_output_array, output_array);
		} else {
			// UNION + REL
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, expr, union_plan->DeriveOutputColumns()->Pdrgpcr(mp), output_array);
		}
	}

	return union_plan;
}

CExpression * Planner::lExprLogicalGet(uint64_t obj_id, string rel_name, uint64_t rel_width, string alias) {
	CMemoryPool* mp = this->memory_pool;

	if(alias == "") { alias = rel_name; }
	D_ASSERT(rel_width > 0);

	CWStringConst strName(std::wstring(rel_name.begin(), rel_name.end()).c_str());
	CTableDescriptor *ptabdesc =
		lCreateTableDesc(mp, rel_width,
					   lGenRelMdid(obj_id),	// 6.objid.0.0
					   CName(&strName));

	CWStringConst strAlias(std::wstring(alias.begin(), alias.end()).c_str());

	CLogicalGet *pop = GPOS_NEW(mp) CLogicalGet(
		mp, GPOS_NEW(mp) CName(mp, CName(&strAlias)), ptabdesc);

	CExpression *scan_expr = GPOS_NEW(mp) CExpression(mp, pop);
	CColRefArray *arr = pop->PdrgpcrOutput();
	for (ULONG ul = 0; ul < arr->Size(); ul++) {
		CColRef *ref = (*arr)[ul];
		ref->MarkAsUnknown();
	}
	return scan_expr;
}

CExpression * Planner::lExprLogicalUnionAllWithMapping(CExpression* lhs, CExpression* rhs, CColRefArray* lhs_mapping, CColRefArray* rhs_mapping) {

	CMemoryPool* mp = this->memory_pool;

	// refer to CXformExpandFullOuterJoin.cpp
	CColRefArray *pdrgpcrOutput = GPOS_NEW(mp) CColRefArray(mp);
	CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);
	
	// output columns of the union
	pdrgpcrOutput->AppendArray(lhs_mapping);
	pdrgpcrOutput->AddRef();
	pdrgdrgpcrInput->Append(lhs_mapping);
	pdrgdrgpcrInput->Append(rhs_mapping);

	CExpression *pexprUnionAll = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgdrgpcrInput),
		lhs, rhs);

	return pexprUnionAll;

}

CExpression * Planner::lExprLogicalUnionAll(CExpression* lhs, CExpression* rhs) {

	CMemoryPool* mp = this->memory_pool;
	return lExprLogicalUnionAllWithMapping(
				lhs, rhs, lhs->DeriveOutputColumns()->Pdrgpcr(mp),
				rhs->DeriveOutputColumns()->Pdrgpcr(mp));

}

/*
 * CExpression* returns result of CLogicalProject, which has schema of (relation + projected schema)
 * CColRefAray* returns colrefs only for (projected schema). Thus this colref should be passed
 * to CLoigcalUnionAll to actually perform projection
*/
std::pair<CExpression*, CColRefArray*> Planner::lExprScalarAddSchemaConformProject(CExpression* relation,
	vector<uint64_t> col_ids_to_project, vector<pair<gpmd::IMDId*, gpos::INT>>* target_schema_types
) {
	// col_ids_to_project may include std::numeric_limits<uint64_t>::max(),
	// which indicates null projection
	CMemoryPool* mp = this->memory_pool;

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray* output_col_array = GPOS_NEW(mp) CColRefArray(mp);

	uint64_t target_col_id = 0;
	for(auto col_id: col_ids_to_project) {
		CExpression* scalar_proj_elem;
		if( col_id == std::numeric_limits<uint64_t>::max()) {
			D_ASSERT(target_schema_types != nullptr);
			// project null column
			auto& type_info = (*target_schema_types)[target_col_id];
			CExpression* null_expr =
				CUtils::PexprScalarConstNull(mp, lGetMDAccessor()->RetrieveType(type_info.first) , type_info.second);
			const CWStringConst col_name_str(GPOS_WSZ_LIT("const_null"));
			CName col_name(&col_name_str);
			CColRef *new_colref = col_factory->PcrCreate( lGetMDAccessor()->RetrieveType(type_info.first), type_info.second, col_name);
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), null_expr);
			
			proj_array->Append(scalar_proj_elem);
			output_col_array->Append(new_colref);
			
		} else {
			// project non-null column
			CColRef *colref = lGetIthColRef(relation->DeriveOutputColumns(), col_id);
			CColRef *new_colref = col_factory->PcrCreate(colref);	// generate new reference
			output_col_array->Append(new_colref);
			CExpression* ident_expr = GPOS_NEW(mp)
					CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), ident_expr);

			proj_array->Append(scalar_proj_elem);	
		}
		target_col_id++;
	}

	// project nothing 
	CExpression *pexprPrjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
	CExpression *proj_expr = CUtils::PexprLogicalProject(mp, relation, pexprPrjList, false);

	return make_pair(proj_expr, output_col_array);

}

CExpression * Planner::lExprLogicalJoinOnId(CExpression* lhs, CExpression* rhs,
		uint64_t lhs_pos, uint64_t rhs_pos, bool project_out_lhs_key, bool project_out_rhs_key) {
		
	/*
		if V join R, set (project_out_lhs, project_out_rhs) as (0,1)
		if R join V, set (project_out_lhs, project_out_rhs) as (1,0)
	*/

	CMemoryPool* mp = this->memory_pool;

	CColRefSet* lcols = lhs->DeriveOutputColumns();
	auto lhs_size = lcols->Size();
	CColRefSet* rcols = rhs->DeriveOutputColumns();
	auto rhs_size = rcols->Size();
	CColRef *pcrLeft = lGetIthColRef(lcols, lhs_pos);
	CColRef *pcrRight = lGetIthColRef(rcols, rhs_pos);

	lhs->AddRef();
	rhs->AddRef();

	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
	auto join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprEquality);

	// TODO need function for join projection
	// if( project_out_lhs_key || project_out_rhs_key ) {
	// 	uint64_t idx_to_project_out = (project_out_lhs_key == true) ? (lhs_pos) : (lhs_size + rhs_pos);
	// 	join_result = lExprScalarProjectionExceptColIds(join_result, vector<uint64_t>({idx_to_project_out}));
	// }

	return join_result;
}


CExpression * Planner::lExprLogicalCartProd(CExpression* lhs, CExpression* rhs) {
	/* Perform cartesian product = inner join on predicate true	*/

	CMemoryPool* mp = this->memory_pool;

	CExpression *pexprTrue = CUtils::PexprScalarConstBool(mp, true, false);
	auto prod_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprTrue);
	D_ASSERT( CUtils::FCrossJoin(prod_result) );
	return prod_result;
}

CTableDescriptor * Planner::lCreateTableDesc(CMemoryPool *mp, ULONG num_cols, IMDId *mdid,
						   const CName &nameTable, gpos::BOOL fPartitioned) {

	CTableDescriptor *ptabdesc = lTabdescPlainWithColNameFormat(mp, num_cols, mdid,
										  GPOS_WSZ_LIT("column_%04d"),
										  nameTable, true /* is_nullable */);
	// if (fPartitioned) {
	// 	D_ASSERT(false);
	// 	ptabdesc->AddPartitionColumn(0);
	// }
	// create a keyset containing the first column
	// CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp, num_cols);
	// pbs->ExchangeSet(0);
	// ptabdesc->FAddKeySet(pbs);

	return ptabdesc;
}

CTableDescriptor * Planner::lTabdescPlainWithColNameFormat(
		CMemoryPool *mp, ULONG num_cols, IMDId *mdid, const WCHAR *wszColNameFormat,
		const CName &nameTable, gpos::BOOL is_nullable  // define nullable columns
	) {

	GPOS_ASSERT(0 < num_cols);
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDTypeInt4 *pmdtypeint4 =
		md_accessor->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);
	CWStringDynamic *str_name = GPOS_NEW(mp) CWStringDynamic(mp);
	CTableDescriptor *ptabdesc = GPOS_NEW(mp) CTableDescriptor(
		mp, mdid, nameTable,
		false,	// convert_hash_to_random
		IMDRelation::EreldistrMasterOnly, IMDRelation::ErelstorageAppendOnlyCols,
		0  // ulExecuteAsUser
	);
	for (ULONG i = 0; i < num_cols; i++) {
		str_name->Reset();
		str_name->AppendFormat(wszColNameFormat, i);

// TODO access cmdacessor to get valid type mdid

		// create a shallow constant string to embed in a name
		CWStringConst strName(str_name->GetBuffer());
		CName nameColumnInt(&strName);
		CColumnDescriptor *pcoldescInt = GPOS_NEW(mp)
			CColumnDescriptor(mp, pmdtypeint4, default_type_modifier,
							  nameColumnInt, i + 1, is_nullable);
		ptabdesc->AddColumn(pcoldescInt);
	}

	GPOS_DELETE(str_name);
	return ptabdesc;

}

CColRef* Planner::lGetIthColRef(CColRefSet* refset, uint64_t target_idx) {
	
	CMemoryPool* mp = this->memory_pool;

	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
	refset->ExtractColIds(mp, colids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	const ULONG size = colids->Size();
	for (ULONG idx = 0; idx < size; idx++) {
		ULONG colid = *((*colids)[idx]);
		if(idx == target_idx) {
			return col_factory->LookupColRef(colid);
		}
	}
	D_ASSERT(false);
	return nullptr; // to prevent compiler warning
	
}


} // s62