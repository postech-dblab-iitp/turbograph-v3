#include "planner/planner.hpp"

namespace s62 {

Planner::Planner(duckdb::ClientContext* context)
	: context(context) {

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
		// TODO fix hard coding
		auto md_path = "../tbgpp-compiler/test/minidumps/gdb_test1_a-r-b.mdp";
		provider = new (mp, __FILE__, __LINE__) CMDProviderMemory(mp, md_path);
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
	CMDProviderMemory* provider = planner->_orcaGetProviderMemory();

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
	for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        lPlanQueryPart(*singleQuery.getQueryPart(i), &plan);
    }
	return plan;
}

void Planner::lPlanQueryPart(
	const NormalizedQueryPart& queryPart, CExpression** plan) {

	for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
        lPlanReadingClause(queryPart.getReadingClause(i), plan);
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

}

void Planner::lPlanReadingClause(
	BoundReadingClause* boundReadingClause, CExpression** plan) {

	auto readingClauseType = boundReadingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        lPlanMatchClause(boundReadingClause, plan);
    } break;
    case ClauseType::UNWIND: {
        lPlanUnwindClause(boundReadingClause, plan);
    } break;
    default:
        D_ASSERT(false);
    }
}

void Planner::lPlanMatchClause(
	BoundReadingClause* boundReadingClause, CExpression** plan) {

	auto boundMatchClause = (BoundMatchClause*)boundReadingClause;
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    expression_vector predicates = boundMatchClause->hasWhereExpression() ?
                          boundMatchClause->getWhereExpression()->splitOnAND() :
                          expression_vector{};

    if (boundMatchClause->getIsOptional()) {
        D_ASSERT(false);
    } else {
		lPlanRegularMatch(*queryGraphCollection, predicates, plan);
    }
	
	
}

void Planner::lPlanUnwindClause(
	BoundReadingClause* boundReadingClause, CExpression** plan) {
	D_ASSERT(false);
}

void Planner::lPlanRegularMatch(const QueryGraphCollection& queryGraphCollection,
	expression_vector& predicates, CExpression** plan) {

	// S62 this is a test evaluation
	// further need more

	D_ASSERT(queryGraphCollection.getNumQueryGraphs() == 1);

	auto* gq = queryGraphCollection.getQueryGraph(0);
	vector<shared_ptr<NodeExpression>> query_nodes = gq->getQueryNodes();
	vector<shared_ptr<RelExpression>> query_edges = gq->getQueryRels();

	vector<CExpression*> node_logical_plans;
	
	auto node1 = query_nodes[0].get();
	auto edge = query_edges[0].get();
	auto node2 = query_nodes[1].get();

	auto lnode1 = lExprLogicalGetNode();	// id, vp1, vp2
	auto ledge = lExprLogicalGetEdge();		// id, sid, tid, ep1, ep2
	auto lnode2 = lExprLogicalGetNode();	// id, vp1, vp2
	
	auto firstjoin = lExprLogicalJoinOnId(lnode1, ledge, 0, 1);	// id = sid
	auto secondjoin = lExprLogicalJoinOnId(firstjoin, lnode2, 5, 0);	// tid = id

	*plan = secondjoin;
}

CExpression * Planner::lExprLogicalGetNode() {
	CMemoryPool* mp = this->memory_pool;

	CWStringConst strName(GPOS_WSZ_LIT("V"));
	CTableDescriptor *ptabdesc =
		lCreateTableDesc(mp, 3,										// width 3	// TODO hard coded
					   GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, 10000, 1, 1),	// 6 10000 0 0 - node table
					   CName(&strName));										// basetable

	CWStringConst strAlias(GPOS_WSZ_LIT("V"));

	CLogicalGet *pop = GPOS_NEW(mp) CLogicalGet(
		mp, GPOS_NEW(mp) CName(mp, CName(&strAlias)), ptabdesc);

	CExpression *result = GPOS_NEW(mp) CExpression(mp, pop);
	CColRefArray *arr = pop->PdrgpcrOutput();
	for (ULONG ul = 0; ul < arr->Size(); ul++) {
		CColRef *ref = (*arr)[ul];
		ref->MarkAsUsed();
	}

	return result;
}

CExpression * Planner::lExprLogicalGetEdge() {
	CMemoryPool* mp = this->memory_pool;

	CWStringConst strName(GPOS_WSZ_LIT("E"));
	CTableDescriptor *ptabdesc =
		lCreateTableDesc(mp, 5,										// width 5	// TODO hard coded
					   GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, 20000, 1, 1),	// 6 20000 0 0 - edge table
					   CName(&strName));										// basetable

	CWStringConst strAlias(GPOS_WSZ_LIT("E"));

	CLogicalGet *pop = GPOS_NEW(mp) CLogicalGet(
		mp, GPOS_NEW(mp) CName(mp, CName(&strAlias)), ptabdesc);

	CExpression *result = GPOS_NEW(mp) CExpression(mp, pop);
	CColRefArray *arr = pop->PdrgpcrOutput();
	for (ULONG ul = 0; ul < arr->Size(); ul++) {
		CColRef *ref = (*arr)[ul];
		ref->MarkAsUsed();
	}

	return result;
}

CExpression * Planner::lExprLogicalJoinOnId(CExpression* lhs, CExpression* rhs, uint64_t lhs_pos, uint64_t rhs_pos) {
	CMemoryPool* mp = this->memory_pool;

	auto lcols = lhs->DeriveOutputColumns();
	auto rcols = rhs->DeriveOutputColumns();
	CColRef *pcrLeft = lGetIthColRef(lcols, lhs_pos);
	CColRef *pcrRight = lGetIthColRef(rcols, rhs_pos);

	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);

	auto result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprEquality);
	return result;
}

CTableDescriptor * Planner::lCreateTableDesc(CMemoryPool *mp, ULONG num_cols, IMDId *mdid,
						   const CName &nameTable, gpos::BOOL fPartitioned) {

	CTableDescriptor *ptabdesc = lTabdescPlainWithColNameFormat(mp, num_cols, mdid,
										  GPOS_WSZ_LIT("column_%04d"),
										  nameTable, true /* is_nullable */);
	if (fPartitioned) {
		D_ASSERT(false);
		ptabdesc->AddPartitionColumn(0);
	}
	// create a keyset containing the first column
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp, num_cols);
	pbs->ExchangeSet(0);
	ptabdesc->FAddKeySet(pbs);

	return ptabdesc;
}

CTableDescriptor * Planner::lTabdescPlainWithColNameFormat(
		CMemoryPool *mp, ULONG num_cols, IMDId *mdid, const WCHAR *wszColNameFormat,
		const CName &nameTable,
		gpos::BOOL is_nullable  // define nullable columns
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