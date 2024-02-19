#include "planner.hpp"
#include "mdprovider/MDProviderTBGPP.h"


#include <string>
#include <limits>
#include <map>


namespace s62 {

Planner::Planner(PlannerConfig config, MDProviderType mdp_type, duckdb::ClientContext* context, std::string memory_mdp_path)
	: config(config), mdp_type(mdp_type), context(context), memory_mdp_filepath(memory_mdp_path) {

	if(mdp_type == MDProviderType::MEMORY) {
		assert(memory_mdp_filepath != "");
		//  "filepath should be provided in memory provider mode"
	}

	// opt context
	l_is_outer_plan_registered = false;
	l_registered_outer_plan = nullptr;

	this->orcaInit();

	// initialize system column names
	id_col_cname = GPOS_NEW(memory_pool)
		CName(GPOS_NEW(memory_pool) CWStringConst(w_id_col_name), true /*fOwnsMemory*/);
	sid_col_cname = GPOS_NEW(memory_pool)
		CName(GPOS_NEW(memory_pool) CWStringConst(w_sid_col_name), true /*fOwnsMemory*/);
	tid_col_cname = GPOS_NEW(memory_pool)
		CName(GPOS_NEW(memory_pool) CWStringConst(w_tid_col_name), true /*fOwnsMemory*/);
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
	bound_statement = nullptr;
	pipelines.clear();
	logical_plan_output_col_names.clear();
	logical_plan_output_colrefs.clear();
	physical_plan_output_colrefs.clear();
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
		CBitSet *hash_merge_join_bitset = CXform::PbsHashMergeJoinXforms(mp);
		traceflag_bitset->Union(hash_merge_join_bitset);
		hash_merge_join_bitset->Release();	
		// disable nl join - need nlj for cartesian product
		//GPOPT_DISABLE_XFORM(CXform::ExfInnerJoin2NLJoin);
	}
	else if(config.HASH_JOIN_ONLY) {
		CBitSet *merge_index_join_bitset = CXform::PbsMergeIndexJoinXforms(mp);
		traceflag_bitset->Union(merge_index_join_bitset);
		merge_index_join_bitset->Release();	
	}
	else if(config.MERGE_JOIN_ONLY) {
		CBitSet *hash_index_join_bitste = CXform::PbsHashIndexJoinXforms(mp);
		traceflag_bitset->Union(hash_index_join_bitste);
		hash_index_join_bitste->Release();	
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

	//GPOS_UNSET_TRACE(gpos::EOptTraceFlag::EopttraceEnableLOJInNAryJoin);
	GPOS_SET_TRACE(gpos::EOptTraceFlag::EopttraceDisableOuterJoin2InnerJoinRewrite);
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
	
	auto hint = GPOS_NEW(mp) CHint(
			gpos::int_max, /* join_arity_for_associativity_commutativity */
			gpos::int_max, /* array_expansion_threshold */
			config.JOIN_ORDER_DP_THRESHOLD_CONFIG,			 /*ulJoinOrderDPLimit*/
			BROADCAST_THRESHOLD,				 /*broadcast_threshold*/
			true,								 /* enforce_constraint_on_dml */
			PUSH_GROUP_BY_BELOW_SETOP_THRESHOLD, /* push_group_by_below_setop_threshold */
			XFORM_BIND_THRESHOLD,				 /* xform_bind_threshold */
			SKEW_FACTOR							 /* skew_factor */
		);

	COptimizerConfig *optimizer_config =
		GPOS_NEW(mp) COptimizerConfig(
			GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/),
			CStatisticsConfig::PstatsconfDefault(mp),
			CCTEConfig::PcteconfDefault(mp), pcm, hint,
			CWindowOids::GetWindowOids(mp));
		
	// use the default constant expression evaluator which cannot evaluate any expression
	IConstExprEvaluator * pceeval = GPOS_NEW(mp) CConstExprEvaluatorDefault();
	COptCtxt *poctxt =
		COptCtxt::PoctxtCreate(mp, mda, pceeval, optimizer_config);
	poctxt->SetHasMasterOnlyTables();
	ITask::Self()->GetTls().Store(poctxt);
}

void * Planner::_orcaExec(void* planner_ptr) {
	boost::timer::cpu_timer orca_compile_timer;
	orca_compile_timer.start();
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
		LogicalPlan *logical_plan = planner->lGetLogicalPlan();
		CExpression* orca_logical_plan = logical_plan->getPlanExpr();
		
		{
			if(planner->config.DEBUG_PRINT) {
				std::cout << "[LOGICAL PLAN]" << std::endl;
				CWStringDynamic str(mp, L"\n");
				COstreamString oss(&str);
				orca_logical_plan->OsPrint(oss);
				GPOS_TRACE(str.GetBuffer());
			}
		}
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf(); // temp
		CEngine eng(mp);
		CQueryContext* pqc = planner->_orcaGenQueryCtxt(mp, orca_logical_plan);
	
		/* Register logical column colrefs / names */
		std::vector<CColRef*> output_columns;
		std::vector<std::string> output_names;
		logical_plan->getSchema()->getOutputColumns(output_columns);

		for(auto& col: output_columns) {
			// check if alternative column name exists on property_col_to_output_col_names_mapping
			if(planner->property_col_to_output_col_names_mapping.find(col) != planner->property_col_to_output_col_names_mapping.end()) {
				output_names.push_back(planner->property_col_to_output_col_names_mapping[col]);
				continue;
			}
			wstring col_name_ws = col->Name().Pstr()->GetBuffer();
			output_names.push_back(std::string(col_name_ws.begin(), col_name_ws.end()));

			// Create OIDS and push
			if (col->GetMdidTable() == NULL) {
				planner->logical_plan_output_col_oids.push_back(GPDB_UNKNOWN);
			} else {
				planner->logical_plan_output_col_oids.push_back(CMDIdGPDB::CastMdid(((CColRefTable*) col)->GetMdidTable())->Oid());
			}
		}
		D_ASSERT(output_columns.size() == output_names.size());
		planner->logical_plan_output_colrefs = output_columns;
		planner->logical_plan_output_col_names = output_names;
	
		/* LogicalRules */
		CExpression *orca_logical_plan_after_logical_opt = pqc->Pexpr();
		{
			if (planner->config.DEBUG_PRINT) {
				std::cout << "[PREPROCESSED LOGICAL PLAN]" << std::endl;
				CWStringDynamic str(mp, L"\n");
				COstreamString oss(&str);
				orca_logical_plan_after_logical_opt->OsPrint(oss);
				GPOS_TRACE(str.GetBuffer());
			}
		}

		eng.Init(pqc, NULL /*search_stage_array*/);
		eng.Optimize();
		CExpression *orca_physical_plan = eng.PexprExtractPlan();	// best plan
		(void) orca_physical_plan->PrppCompute(mp, pqc->Prpp());
		{
			if (planner->config.DEBUG_PRINT) {
				std::cout << "[PHYSICAL PLAN]" << std::endl;
				CWStringDynamic str(mp, L"\n");
				COstreamString oss(&str);
				orca_physical_plan->OsPrint(oss);
				GPOS_TRACE(str.GetBuffer());
			}
		}
		auto orca_compile_time_ms = orca_compile_timer.elapsed().wall / 1000000.0;
		std::cout << "\nCompile Time: "  << orca_compile_time_ms << " ms" << std::endl;
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

	/* inject per-operator-dependencies and per-pipeline dependencies
		- per-op: CypherPhysicalOperator::children
		- per-pipeline: child_executors / dep_executors
	*/

	std::vector<duckdb::CypherPipelineExecutor *> executors;

	if (generate_sfg) {
		D_ASSERT(pipelines.size() == sfgs.size());
	}

	for (auto pipe_idx = 0; pipe_idx < pipelines.size(); pipe_idx++) {
		auto &pipe = pipelines[pipe_idx];
		duckdb::SchemaFlowGraph *sfg = generate_sfg ? &sfgs[pipe_idx] : nullptr;

		// find children and deps - the child/dep ordering matters. 
		// must run in ascending order of the vector
		auto *new_ctxt = new duckdb::ExecutionContext(context);
		vector<duckdb::CypherPipelineExecutor *> child_executors;									// child : pipe's sink == op's source
		std::map<duckdb::CypherPhysicalOperator *, duckdb::CypherPipelineExecutor *> dep_executors;	// dep   : pipe's sink == op's operator

		// inject per-operator dependencies in a pipeline
		for (duckdb::idx_t op_idx = 1; op_idx < pipe->pipelineLength; op_idx++) {
			pipe->GetIdxOperator(op_idx)->children.push_back(
				pipe->GetIdxOperator(op_idx - 1)
			);
		}

		// find children pipeline
		for (auto& ce: executors) {
			// connect SOURCE with previous SINK
			if (pipe->GetSource() == ce->pipeline->GetSink()) {
				child_executors.push_back(ce);
			}
		}

		// find dependent pipeline
		for (auto& ce: executors) {
			// connect OPERATORS with previous SINK
			for (int op_idx = 0; op_idx < pipe->GetOperators().size(); op_idx++) {
				duckdb::CypherPhysicalOperator *op = pipe->GetOperators()[op_idx];
				if (op == ce->pipeline->GetSink()) {
					dep_executors.insert(std::make_pair(op,ce));
					// add previous of ce to children
					if (ce->pipeline->pipelineLength == 2) {
						op->children.push_back(ce->pipeline->GetSource());
					} else {
						op->children.push_back(ce->pipeline->GetIdxOperator(ce->pipeline->pipelineLength - 2));	// last one in operators
					}
				}
			}
		}
		duckdb::CypherPipelineExecutor *pipe_exec;
		if (generate_sfg) {
			pipe_exec = new duckdb::CypherPipelineExecutor(new_ctxt, pipe, *sfg, move(child_executors), move(dep_executors));
		} else {
			pipe_exec = new duckdb::CypherPipelineExecutor(new_ctxt, pipe, move(child_executors), move(dep_executors));
		}

		executors.push_back(pipe_exec);
	}

	return executors;
}

vector<string> Planner::getQueryOutputColNames(){
	
	// TODO no asserts?
	return logical_plan_output_col_names;
}

std::vector<OID> Planner::getQueryOutputOIDs(){
	return logical_plan_output_col_oids;
}

} // s62