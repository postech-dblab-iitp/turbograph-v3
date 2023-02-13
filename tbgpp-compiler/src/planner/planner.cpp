#include "gpopt/operators/CLogicalUnionAll.h"

#include "planner/planner.hpp"
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

	LogicalPlan* plan;
	for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
        plan = lPlanReadingClause(queryPart.getReadingClause(i), prev_plan);
    }
	D_ASSERT( queryPart.getNumUpdatingClause() == 0);
	// if (queryPart.hasProjectionBody()) {
    //     projectionPlanner.planProjectionBody(*queryPart.getProjectionBody(), plans);
    //     if (queryPart.hasProjectionBodyPredicate()) {
    //         for (auto& plan : plans) {
    //             appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
    //         }
    //     }
    // }

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

// // TODO 0202 testcase - return scan on "n"
	auto* qg = qgc.getQueryGraph(0);
	vector<shared_ptr<NodeExpression>> query_nodes = qg->getQueryNodes();
	NodeExpression* node1 = query_nodes[0].get();

	auto table_oids = node1->getTableIDs();
	D_ASSERT(table_oids.size() >= 1);

	map<uint64_t, map<uint64_t, uint64_t>> schema_proj_mapping;	// maps from new_col_id->old_col_id
	for( auto& t_oid: table_oids) {
		schema_proj_mapping.insert({t_oid, map<uint64_t, uint64_t>()});
	}
	D_ASSERT(schema_proj_mapping.size() == table_oids.size());

	auto& prop_exprs = node1->getPropertyExpressions();
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
	
	// currently generate scan on only one table
	plan = lExprLogicalGetNode("V", table_oids, &schema_proj_mapping);	// id, vp1, vp2
	return plan;
// 0202 testcase - return scan on "n"

	// auto* qg = qgc.getQueryGraph(0);
	// vector<shared_ptr<NodeExpression>> query_nodes = qg->getQueryNodes();
	// auto node1 = query_nodes[0].get();
	// auto edge = query_edges[0].get();
	// auto node2 = query_nodes[1].get();
	// auto lnode1 = lExprLogicalGetNode("V", 10000);	// id, vp1, vp2
	// auto ledge = lExprLogicalGetEdge("R", 20000);		// id, sid, tid
	// auto lnode2 = lExprLogicalGetNode("V", 10000);	// id, vp1, vp2
	// auto firstjoin = lExprLogicalJoinOnId(lnode1->getPlanExpr, ledge, 0, 1);	// id = sid
	// auto secondjoin = lExprLogicalJoinOnId(firstjoin, lnode2, 5, 0);	// tid = id
	// auto leaf_plan_obj = new LogicalPlan(secondjoin);

// TODO bidirectional matching not considered yet.
	
	// auto num_qgs = qgc.getNumQueryGraphs();
	// vector<vector<string>> bound_nodes(num_qgs, vector<string>());	// for each qg
	// vector<vector<string>> bound_edges(num_qgs, vector<string>());
	// vector<LogicalPlan*> gq_plans(num_qgs, nullptr);

// TODO logic
	// for(int idx=0; idx < qgc.getNumQueryGraphs(); idx++){
	// 	QueryGraph* qg = qgc.getQueryGraph(idx);

	// 	LogicalPlan* qg_plan = nullptr;

	// 	for(int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
	// 		auto shd_edge = qg->getQueryRel(edge_idx);
	// 		RelExpression* qedge = shd_edge.get();
	// 		string edge_name = qedge->getUniqueName();
	// 		string lhs_name = qedge->getSrcNodeName();
	// 		string rhs_name = qedge->getDstNodeName();

	// 		bool isLHSBound = false;
	// 		bool isRHSBound = false;
	// 		if(!qg_plan) {
	// 			isLHSBound = qg_plan->isNodeBound(lhs_name) ? true : false;
	// 			isRHSBound = qg_plan->isNodeBound(rhs_name) ? true : false;
	// 		}
	// 		if( (!isLHSBound) && (!isRHSBound) ) {			// unb, unb

	// 		} else if((isLHSBound) && (!isRHSBound) ) {		// bound, unb

	// 		} else if((!isLHSBound) && (isRHSBound) ) {		// unb, bnd

	// 		} else {										// bound, bound

	// 		}
	// 		// TODO continue logic;
	// 	}
			
		// append qg plan;
	//}
// fin logic

	// now join 
	return nullptr;
}

LogicalPlan* Planner::lExprLogicalGetNode(string name, vector<uint64_t> relation_oids,
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

	CColRefArray* idx1_output_array;
	for( int idx = 0; idx < relation_oids.size(); idx++) {
		auto& oid = relation_oids[idx];

		CExpression * expr;
		const gpos::ULONG num_cols = lGetRelMd(oid)->ColumnCount();

		D_ASSERT(num_cols != 0);
		expr = lExprLogicalGet(oid, name, num_cols);

		// add projection if exists
		if(do_schema_mapping) {
			auto& mapping = (*schema_proj_mapping)[oid];
			assert(num_proj_cols == mapping.size());
			vector<uint64_t> project_cols;
			for(int proj_col_idx = 0; proj_col_idx < num_proj_cols; proj_col_idx++) {
				project_cols.push_back(mapping.find(proj_col_idx)->second);
			}
			D_ASSERT(project_cols.size() > 0);
			expr = lExprScalarProjectionOnColIds(expr, project_cols, &union_schema_types);
		}
		
		// add union
		CColRefArray* expr_array = expr->DeriveOutputColumns()->Pdrgpcr(mp);
		CColRefArray* output_array = GPOS_NEW(mp) CColRefArray(mp);
		for(int i = 0; i<expr_array->Size(); i++) {
			if(do_schema_mapping && (i < num_cols)) { continue; }
			output_array->Append(expr_array->operator[](i));
		}
		if(idx == 0) {
			// REL
			union_plan = expr;
			idx1_output_array = output_array;
		} else if (idx == 1) {
			// REL + REL
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, expr, idx1_output_array, output_array);
		} else {
			// UNION + REL
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, expr, union_plan->DeriveOutputColumns()->Pdrgpcr(mp), output_array);
		}
	}

	auto plan = new LogicalPlan(union_plan);
	// TODO add schema
	plan->bindNode(name);
	return plan;
}

LogicalPlan* Planner::lExprLogicalGetEdge(string name, vector<uint64_t> oids) {
	auto scan_expr = lExprLogicalGet(oids[0], name, 5);	// TODO fixme
	//return scan_expr;
	// projection
	// lExprScalarProjectionOnColIds(scan_expr, vector<uint64_t>({0,1,2})); 	// use only id, src, tgt
	auto plan = new LogicalPlan(scan_expr);
	// TODO update schema
	plan->bindEdge(name);
	return plan;
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


// Output Schema: RELATION + COL_IDS_TO_PROJECT
CExpression* Planner::lExprScalarProjectionOnColIds(CExpression* relation,
	vector<uint64_t> col_ids_to_project, vector<pair<gpmd::IMDId*, gpos::INT>>* target_schema_types
) {
	// col_ids_to_project may include std::numeric_limits<uint64_t>::max(),
	// which indicates null projection
	CMemoryPool* mp = this->memory_pool;

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray* old_colrefs = GPOS_NEW(mp) CColRefArray(mp);
	CColRefArray* colref_array = GPOS_NEW(mp) CColRefArray(mp);

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

			colref_array->Append(new_colref);
			proj_array->Append(scalar_proj_elem);
			
		} else {
			// project non-null column
			CColRef *colref = lGetIthColRef(relation->DeriveOutputColumns(), col_id);
			CColRef *new_colref = col_factory->PcrCreate(colref);	// generate new reference
			CExpression* ident_expr = GPOS_NEW(mp)
					CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), ident_expr);

			old_colrefs->Append(colref);
			colref_array->Append(new_colref);
			proj_array->Append(scalar_proj_elem);	
		}
		target_col_id++;
	}

	// project nothing 
	CExpression *pexprPrjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
	CExpression *proj_expr = CUtils::PexprLogicalProject(mp, relation, pexprPrjList, false);

	return proj_expr;
	// UlongToColRefMap *colref_mapping = GPOS_NEW(mp) UlongToColRefMap(mp);
	// for(uint64_t colid = 0 ; colid < col_ids_to_project.size(); colid++) {
	// 	colref_mapping->Insert(GPOS_NEW(mp) ULONG(colid), colref_array->operator[](colid));
	// }

	// auto final_expr = proj_expr->PexprCopyWithRemappedColumns(mp, colref_mapping, true);
	// return final_expr;
}

CExpression* Planner::lExprScalarProjectionExceptColIds(CExpression* relation, vector<uint64_t> col_ids_to_project_out) {

	CMemoryPool* mp = this->memory_pool;

	assert(false); // TODO need to change projection mechnaism like lExprScalarProjectionOnColIds
	return nullptr;
	// CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	// CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	
	// for(int col_id = 0; col_id < relation->DeriveOutputColumns()->Size(); col_id++ ) {
	// 	if( col_id == std::numeric_limits<uint64_t>::max()) {
	// 		// schemaless case not implemented yet.
	// 		assert(false);
	// 	}
	// 	// bypass ids to project
	// 	for(auto& neg_id: col_ids_to_project_out){
	// 		if( col_id == neg_id ) continue;
	// 	}
	// 	CExpression* scalar_proj_elem;
	// 	CColRef *colref = lGetIthColRef(relation->DeriveOutputColumns(), col_id);
	// 	CColRef *new_colref = col_factory->PcrCreate(colref);	// generate new reference
	// 	scalar_proj_elem = GPOS_NEW(mp) CExpression(
	// 			mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref),
	// 			GPOS_NEW(mp)
	// 				CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref)));
	// 	proj_array->Append(scalar_proj_elem);
	// }
	// CExpression *pexprPrjList =
	// 	GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
	// CExpression* proj_expr =  GPOS_NEW(mp) CExpression(
	// 		mp, GPOS_NEW(mp) CLogicalProject(mp), relation, pexprPrjList);
	// return proj_expr;

}

CExpression * Planner::lExprLogicalJoinOnId(CExpression* lhs, CExpression* rhs,
		uint64_t lhs_pos, uint64_t rhs_pos, bool project_out_lhs_key, bool project_out_rhs_key) {
		
	// if V join R, set (project_out_lhs, project_out_rhs) as (0,1)
	// if R join V, set (project_out_lhs, project_out_rhs) as (1,0)

	CMemoryPool* mp = this->memory_pool;

	CColRefSet* lcols = lhs->DeriveOutputColumns();
	auto lhs_size = lcols->Size();
	CColRefSet* rcols = rhs->DeriveOutputColumns();
	auto rhs_size = rcols->Size();
	CColRef *pcrLeft = lGetIthColRef(lcols, lhs_pos);
	CColRef *pcrRight = lGetIthColRef(rcols, rhs_pos);

	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
	auto join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprEquality);

	if( project_out_lhs_key || project_out_rhs_key ) {
		uint64_t idx_to_project_out = (project_out_lhs_key == true) ? (lhs_pos) : (lhs_size + rhs_pos);
		join_result = lExprScalarProjectionExceptColIds(join_result, vector<uint64_t>({idx_to_project_out}));
	}

	return join_result;
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
		IMDRelation::EreldistrMasterOnly, IMDRelation::ErelstorageHeap,
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

CColRef* Planner::lGetIthColRef(CColRefSet* refset, uint64_t idx) {

	CColRefSetIter crsi(*refset);
	for(int i = 0; i <= idx; i++) {	// advance idx+1 times
		crsi.Advance();
	}
	return crsi.Pcr();
}


} // s62