#include "planner.hpp"
#include "mdprovider/MDProviderTBGPP.h"


#include <string>
#include <limits>


namespace s62 {

Planner::Planner(PlannerConfig config, MDProviderType mdp_type, duckdb::ClientContext* context, std::string memory_mdp_path)
	: config(config), mdp_type(mdp_type), context(context), memory_mdp_filepath(memory_mdp_path) {

	if(mdp_type == MDProviderType::MEMORY) {
		assert(memory_mdp_filepath != "");
		//  "filepath should be provided in memory provider mode"
	}
	this->orcaInit();
}

Planner::~Planner() {

	CMDCache::Shutdown();
	
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

	CMDCache::Init();
}

void Planner::reset() {
	
	// reset planner context 
	// note that we reuse orca memory pool
	table_col_mapping.clear();
	bound_statement = nullptr;
	pipelines.clear();
	output_col_names.clear();

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
		pcrsOutput->Include(colref);
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
	
	// reset previous context
	this->reset();

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

	CMemoryPool* mp = this->memory_pool;

	CBitSet *enabled_trace_flags = NULL;
	CBitSet *disabled_trace_flags = NULL;
	CBitSet *traceflag_bitset = GPOS_NEW(mp) CBitSet(mp, EopttraceSentinel);
	CBitSet *join_heuristic_bitset = NULL;

	// https://gpdb.docs.pivotal.io/6-14/ref_guide/config_params/guc-list.html

        /*
                When GPORCA is enabled (the default), this parameter sets the
           join enumeration algorithm:

		query - Uses the join order specified in the query.
		greedy - Evaluates the join order specified in the query
           and alternatives based on minimum cardinalities of the relations in
           the joins.
		exhaustive - Applies transformation rules to find and
           evaluate up to a configurable threshold number
           (optimizer_join_order_threshold, default 10) of n-way inner joins,
           and then changes to and uses the greedy method beyond that. While
           planning time drops significantly at that point, plan quality and
           execution time may get worse.
		exhaustive2 - Operates with an emphasis
           on generating join orders that are suitable for dynamic partition
           elimination. This algorithm applies transformation rules to find and
           evaluate n-way inner and outer joins. When evaluating very large
           joins with more than optimizer_join_order_threshold (default 10)
           tables, this algorithm employs a gradual transition to the greedy
           method; planning time goes up smoothly as the query gets more
           complicated, and plan quality and execution time only gradually
           degrade.

        */

	// the outputs of each modes are disabled(flag on)
	// e.g. query/greedy/exhaustive all disables DPv2, and exhastive disables DPv1
	switch( config.JOIN_ORDER_TYPE ) {
		case PlannerConfig::JoinOrderType::JOIN_ORDER_IN_QUERY:
			join_heuristic_bitset = CXform::PbsJoinOrderInQueryXforms(mp);
			break;
		case PlannerConfig::JoinOrderType::JOIN_ORDER_GREEDY_SEARCH:
			join_heuristic_bitset = CXform::PbsJoinOrderOnGreedyXforms(mp);
			break;
		case PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH:
			join_heuristic_bitset = CXform::PbsJoinOrderOnExhaustiveXforms(mp);
			break;
		case PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE2_SEARCH:
			join_heuristic_bitset = CXform::PbsJoinOrderOnExhaustive2Xforms(mp);
			break;
		default:
			D_ASSERT(false);	// must be one of one options
			break;
	}
	D_ASSERT(join_heuristic_bitset != NULL);
	
	if(config.INDEX_JOIN_ONLY) {
		// disable hash join
		CBitSet *hash_join_bitste = CXform::PbsHashJoinXforms(mp);
		traceflag_bitset->Union(hash_join_bitste);
		hash_join_bitste->Release();	
		// disable nl join
		GPOPT_DISABLE_XFORM(CXform::ExfInnerJoin2NLJoin);
	}

	traceflag_bitset->Union(join_heuristic_bitset);
	join_heuristic_bitset->Release();

	SetTraceflags(mp, traceflag_bitset, &enabled_trace_flags, &disabled_trace_flags);

	if(config.ORCA_DEBUG_PRINT) {
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintPlan);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintXformResults);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintXform);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintMemoAfterExploration);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintMemoAfterImplementation);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintMemoAfterOptimization);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintOptimizationContext);
		GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttracePrintRequiredColumns);
	}


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
			if(planner->config.DEBUG_PRINT) {
				std::cout << "[TEST] logical plan string" << std::endl;
				CWStringDynamic str(mp);
				COstreamString oss(&str);
				orca_logical_plan->OsPrint(oss);
				GPOS_TRACE(str.GetBuffer());
			}
		}
		CEngine eng(mp);
		CQueryContext* pqc = planner->_orcaGenQueryCtxt(mp, orca_logical_plan);
	
		/* Register output column names */
		CMDNameArray* result_col_names = pqc->Pdrgpmdname();
		for(gpos::ULONG idx = 0; idx < result_col_names->Size(); idx++) {
			std::wstring table_name_ws(result_col_names->operator[](idx)->GetMDName()->GetBuffer());
			planner->output_col_names.push_back(string(table_name_ws.begin(), table_name_ws.end()));
		}
	
		/* LogicalRules */
		CExpression *orca_logical_plan_after_logical_opt = pqc->Pexpr();
		{
			if(planner->config.DEBUG_PRINT) {
				std::cout << "[TEST] PREPROCESSED logical plan" << std::endl;
				CWStringDynamic str(mp);
				COstreamString oss(&str);
				orca_logical_plan_after_logical_opt->OsPrint(oss);
				GPOS_TRACE(str.GetBuffer());
			}
		}

		eng.Init(pqc, NULL /*search_stage_array*/);
		eng.Optimize();
		CExpression *orca_physical_plan = eng.PexprExtractPlan();	// best plan
		{
			if(planner->config.DEBUG_PRINT) {
				std::cout << "[TEST] best physical plan string" << std::endl;
				CWStringDynamic str(mp);
				COstreamString oss(&str);
				orca_physical_plan->OsPrint(oss);
				GPOS_TRACE(str.GetBuffer());
			}
		}
		planner->pGenPhysicalPlan(orca_physical_plan);	// convert to our plan
		
		

		orca_logical_plan->Release();
		orca_physical_plan->Release();
		GPOS_DELETE(pqc);

		/* Terminate */
		CTaskLocalStorageObject *ptlsobj =
			ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt);
		ITask::Self()->GetTls().Remove(ptlsobj);
		GPOS_DELETE(ptlsobj);
	}

	CRefCount::SafeRelease(provider);
	
	return nullptr;
}

vector<duckdb::CypherPipelineExecutor*> Planner::genPipelineExecutors() {

	D_ASSERT(pipelines.size() > 0);

	std::vector<duckdb::CypherPipelineExecutor*> executors;

	for( auto& pipe: pipelines) {
		// find children - the child ordering matters. 
		// must run in ascending order of the vector
		auto* new_ctxt = new duckdb::ExecutionContext(context);
		vector<duckdb::CypherPipelineExecutor*> child_executors;

		for( auto& ce: executors ) {
			// if child pipeline exists, its executor must be previously be constructed
			if ( pipe->GetSource() == ce->pipeline->GetSink() ) {
				// pipelines are connected if prev.sink == now.source
				// TODO s62  not generally true. consider hash join need to extend
				// TODO this is compare pointer, any better?
				child_executors.push_back(ce);
			} 
		}

		duckdb::CypherPipelineExecutor *pipe_exec;
		if (child_executors.size() > 0 ){
			pipe_exec = new duckdb::CypherPipelineExecutor(new_ctxt, pipe, child_executors);
		} else {
			pipe_exec = new duckdb::CypherPipelineExecutor(new_ctxt, pipe);
		}
		executors.push_back(pipe_exec);
	}

	return executors;
}

vector<string> Planner::getQueryOutputColNames(){
	
	// TODO no asserts?
	return output_col_names;
}


} // s62