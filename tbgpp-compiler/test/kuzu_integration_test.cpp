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

#include "planner/planner.hpp"

#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"

#include "BTNode.h"

#include "naucrates/traceflags/traceflags.h"



#include "mdprovider/MDProviderTBGPP.h"

using namespace antlr4;
using namespace gpopt;

CUnittest* m_rgut = NULL;
ULONG m_ulTests = 0;
ULONG m_ulNested = 0;
void (*m_pfConfig)() = NULL;
void (*m_pfCleanup)() = NULL;

// static array of all known unittest routines
static gpos::CUnittest rgut[] = {
	// naucrates
	GPOS_UNITTEST_STD(CEngineTest),
};

void ConfigureTests() {
	// initialize DXL support
	InitDXL();

	CMDCache::Init();

	// load metadata objects into provider file
	{
		CAutoMemoryPool amp;
		CMemoryPool *mp = amp.Pmp();
		CTestUtils::InitProviderFile(mp);

		// detach safety
		(void) amp.Detach();
	}

// #ifdef GPOS_DEBUG
	// reset xforms factory to exercise xforms ctors and dtors
	CXformFactory::Pxff()->Shutdown();
	GPOS_RESULT eres = CXformFactory::Init();

	GPOS_ASSERT(GPOS_OK == eres);
//#endif	// GPOS_DEBUG
}
void Cleanup() {
	CMDCache::Shutdown();
	CTestUtils::DestroyMDProvider();
}


static void * OrcaTestExec(void *pv) {
	
	std::cout << "[TEST] inside OrcaTestExec()" << std::endl;
	CMainArgs *pma = (CMainArgs *) pv;

	// content of CAutoConfig() - constructor
	m_pfConfig();
	// end of constructor

	CUnittest ut = GPOS_UNITTEST_STD(CEngineTest);
	GPOS_RESULT eres = CUnittest::EresExecute(&ut, 1 /*size*/);

	// content of CAutoConfig() - destructor
	m_pfCleanup();
	// end of destructor
	
	std::cout << "[TEST] eres=" << eres << std::endl;

	return NULL;
}

CExpression * genLogicalGet1(CMemoryPool *mp) {

	CWStringConst strName(GPOS_WSZ_LIT("User"));
	CTableDescriptor *ptabdesc =
		CTestUtils::PtabdescCreate(mp, 16,										// width 2
					   GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, 17165, 1, 1),	// 6 16467 1 1 - tpch-1
					   CName(&strName));										// basetable

	CWStringConst strAlias(GPOS_WSZ_LIT("A_User"));
	return CTestUtils::PexprLogicalGet(mp, ptabdesc, &strAlias);
}

CExpression * genLogicalGet2(CMemoryPool *mp) {

	CWStringConst strName(GPOS_WSZ_LIT("Knows"));
	CTableDescriptor *ptabdesc =
		CTestUtils::PtabdescCreate(mp, 16,										// width
					   GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, 1716555, 1, 1),	// 6 16467 1 1
					   CName(&strName));										// basetable

	CWStringConst strAlias(GPOS_WSZ_LIT("A_Knows"));

	// to generate logicalget, pass tablename / tabledesc(relwidth, mdid, name) / alias / + colmarkasused
	return CTestUtils::PexprLogicalGet(mp, ptabdesc, &strAlias);
}

static void * MyOrcaTestExec(void *pv) {
	std::cout << "[TEST] inside MyOrcaTestExec()" << std::endl;
	CMainArgs *pma = (CMainArgs *) pv;

// init dxl and cache
	InitDXL();
	CMDCache::Init();
// load metadata objects into provider file
	MDProviderTBGPP * provider = NULL;
	CMemoryPool *mp = NULL; 
	{
		CAutoMemoryPool amp;
		mp = amp.Pmp();
		//auto md_path = "../tbgpp-compiler/gpdb/src/backend/gporca/data/dxl/metadata/md.xml";
		//auto md_path = "../tbgpp-compiler/test/minidumps/TPCH_1_metaonly.mdp";
		//auto md_path = "../tbgpp-compiler/test/minidumps/q1_metaonly.mdp";
		//auto md_path = "../tbgpp-compiler/test/minidumps/pretty_mdp.mdp";
		auto md_path = "../tbgpp-compiler/test/minidumps/q1_metaonly-simplified.mdp";
		//auto md_path = "../tbgpp-compiler/test/minidumps/pretty_mdp.mdp";

		provider = new (mp, __FILE__, __LINE__) MDProviderTBGPP(mp);
		// detach safety
		(void) amp.Detach();
	}
	GPOS_ASSERT(mp != NULL);
	GPOS_ASSERT(provider != NULL);
// reset xforms factory to exercise xforms ctors and dtors
	{
		// TODO what is this?
		CXformFactory::Pxff()->Shutdown();
		GPOS_RESULT eres = CXformFactory::Init();
		//GPOS_ASSERT(GPOS_OK == eres);
	}
// generate plan
	{
	// no distribute
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
	// connect provider
		MDProviderTBGPP *pmdp = provider;
		pmdp->AddRef();

	// separate memory pool used for accessor
		CAutoMemoryPool amp;
		CMemoryPool *mp = amp.Pmp();
				
	// to generate accessor, provide local pool, global cache and provider
			// TODO what is m_sysidDefault for, systemid
		CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);
	// install opt context in TLS
		auto m_cost_model_params = GPOS_NEW(mp) CCostModelParamsGPDB(mp);
		//m_cost_model_params->SetParam(39, 1.0, 1.0, 1.0);	// single machine cost - may need to make this alive
		m_cost_model_params->SetParam(13, 0.0, 0.0, 1000000.0);	// gather cost
		m_cost_model_params->SetParam(14, 0.0, 0.0, 1000000.0);	// gather cost
		m_cost_model_params->SetParam(15, 10000000.0, 10000000.0, 10000000.0);	// redistribute cost
		m_cost_model_params->SetParam(16, 10000000.0, 10000000.0, 10000000.0);	// redistribute cs
		m_cost_model_params->SetParam(17, 10000000.0, 10000000.0, 10000000.0);	// broadcast cost
		m_cost_model_params->SetParam(18, 10000000.0, 10000000.0, 10000000.0);	// broadcast cost
		gpdbcost::CCostModelGPDB* pcm = GPOS_NEW(mp) CCostModelGPDB(mp, 1, m_cost_model_params);	// one segment

	// optimizer context (from CAutoOptCtxt.cpp)
		//CAutoOptCtxt aoc(mp, &mda, NULL, /* pceeval */ pcm);
		GPOS_ASSERT(NULL != pcm);
		// create default statistics configuration
		COptimizerConfig *optimizer_config =
			COptimizerConfig::PoconfDefault(mp, pcm);
		// use the default constant expression evaluator which cannot evaluate any expression
		IConstExprEvaluator * pceeval = GPOS_NEW(mp) CConstExprEvaluatorDefault();
		COptCtxt *poctxt =
			COptCtxt::PoctxtCreate(mp, &mda, pceeval, optimizer_config);
		poctxt->SetHasMasterOnlyTables();
		ITask::Self()->GetTls().Store(poctxt);


	// initialize engine
		CEngine eng(mp);
		
	// define join plan expression
		CExpression *lhs_get = genLogicalGet1(mp);
		// CExpression *rhs_get = genLogicalGet2(mp);
		// CExpression *pexpr = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs_get, rhs_get);
		CExpression* pexpr = lhs_get;

		{
			std::cout << "[TEST] logical plan string" << std::endl;
			CWStringDynamic str(mp);
			COstreamString oss(&str);
			pexpr->OsPrint(oss);
			GPOS_TRACE(str.GetBuffer());
		}
		
	
	// generate query context
	// TODO query context is not naive. we need to modify query context, not using testutils here.
		CQueryContext *pqc = nullptr;
		{
			CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
			pcrs->Include(pexpr->DeriveOutputColumns());
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

			pqc = GPOS_NEW(mp) CQueryContext(mp, pexpr, prpp, colref_array,
											pdrgpmdname, true /*fDeriveStats*/);
		}

	// Initialize engine
		eng.Init(pqc, NULL /*search_stage_array*/);

	// optimize query
		eng.Optimize();

	// extract plan
		std::cout << "[TEST] output string" << std::endl;
		CExpression *pexprPlan = eng.PexprExtractPlan();

		CWStringDynamic str(mp);
		COstreamString oss(&str);
		pexprPlan->OsPrint(oss);

		GPOS_TRACE(str.GetBuffer());

	// clean up
		pexpr->Release();
		pexprPlan->Release();
		GPOS_DELETE(pqc);
	// deallocate autooptctxt
		CTaskLocalStorageObject *ptlsobj =
			ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt);
		ITask::Self()->GetTls().Remove(ptlsobj);

		GPOS_DELETE(ptlsobj);
		
		// mp safely deallocated when exiting the scope
	}

	
// cleanup cache and mdprovider
	CMDCache::Shutdown();
	CRefCount::SafeRelease(provider);
	CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
}

int _main(int argc, char** argv) {
 
	std::cout << "compiler test start" << std::endl;
	std::vector <std::string> _tokens;
	for (int i=1; i < argc; ++i) {
		_tokens.push_back(std::string(argv[i]));
	}

	// std::string query = "MATCH (n) RETURN n;";
	//std::string query = "EXPLAIN MATCH (n) RETURN n;";
	//std::string query = "MATCH (n), (m)-[r]->(z) WITH n,m,r,z MATCH (x), (y) RETURN m,n,x,y UNION MATCH (n) RETURN n;";
	//std::string query = "MATCH (a:Person1:Person|Person3)-[x]->(b), (bbb)-[y]->(c) MATCH (c)-[z]->(d) MATCH (e) RETURN a.name,x,b,bbb,y,c,z,d,e";
	// TODO in return clause, cases like id(n) should be considered as well.

	std::string query = "";
	for (auto t: _tokens) {
		query += t;
	}
	
	//std::string query = "MATCH (n) RETURN n;";

	std::cout << "Query => " << std::endl << query << std::endl ;
	auto inputStream = ANTLRInputStream(query);
    //auto parserErrorListener = ParserErrorListener();

// Lexer
	std::cout << "[TEST] calling lexer" << std::endl;
    auto cypherLexer = CypherLexer(&inputStream);
    //cypherLexer.removeErrorListeners();
    //cypherLexer.addErrorListener(&parserErrorListener);
	auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

// Parser
	std::cout << "[TEST] generating and calling KuzuCypherParser" << std::endl;
	auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);

	// std::cout << (kcp.oC_AnyCypherOption()->oC_Explain() != nullptr) << std::endl;
	// std::cout << (kcp.oC_AnyCypherOption()->oC_Profile() != nullptr) << std::endl;

// Sematic parsing
	// Transformer
	std::cout << "[TEST] generating transformer" << std::endl;
	kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
	std::cout << "[TEST] calling transformer" << std::endl;
	auto statement = transformer.transform();
	
	// Binder
	std::cout << "[TEST] generating binder" << std::endl;
	D_ASSERT(false);
	// "deprecating!"
	auto binder = kuzu::binder::Binder(nullptr);
	std::cout << "[TEST] calling binder" << std::endl;
	auto boundStatement = binder.bind(*statement);
	kuzu::binder::BoundStatement * bst = boundStatement.get();
	BTTree<kuzu::binder::ParseTreeNode> printer(bst, &kuzu::binder::ParseTreeNode::getChildren, &kuzu::binder::BoundStatement::getName);
	// WARNING - printer should be disabled when processing following compilation step.
	std::cout << "Tree => " << std::endl;
	printer.print();
	std::cout << std::endl;

// our logical plan
	auto planner = s62::Planner();
	planner.execute(bst);

// bind orca (legacy)

	// std::cout << "[TEST] orca init / params" << std::endl;
	// struct gpos_init_params gpos_params = {NULL};
	
	// gpdxl_init();
	// gpopt_init();

	// INT iArgs = 3;
	// const std::vector<std::string> arguments = { "/turbograph-v3/build/tbgpp-compiler/kuzu_integration_test", "-U", "CEngineTest" };	// TODO argument not used
	// std::vector<const char*> argvv;
	// for (const auto& arg : arguments)
	// 	argvv.push_back((const char*)arg.data());
	// argvv.push_back(nullptr);
	// const CHAR** rgszArgs = argvv.data();

	// CMainArgs ma(iArgs, rgszArgs, "uU:d:xT:i:");
	// //CUnittest::Init(rgut, GPOS_ARRAY_SIZE(rgut), ConfigureTests, Cleanup);
	// // the static members in CUnitTest.cpp is re-defined in our file
	// m_rgut = rgut;
	// m_ulTests = GPOS_ARRAY_SIZE(rgut);
	// m_pfConfig = ConfigureTests;
	// m_pfCleanup = Cleanup;

	// gpos_exec_params params;
	// params.func = MyOrcaTestExec;
	// params.arg = &ma;
	// params.stack_start = &params;
	// params.error_buffer = NULL;
	// params.error_buffer_size = -1;
	// params.abort_requested = NULL;

	// // std::cout << "[TEST] orca memory pool" << std::endl;
	// std::cout << "[TEST] orca engine" << std::endl;
	// auto gpos_output_code = gpos_exec(&params);
	// std::cout << "[TEST] function outuput " << gpos_output_code << std::endl;

	// std::cout << "compiler test end" << std::endl;
	// return 0;
}

int main(int argc, char** argv) {
	// try{
	// 	int out = _main(argc, argv);
	// } catch (CException ex) {
	// 	std::cerr << ex.Major() << " " << ex.Minor() << std::endl;
	// }
	int out = _main(argc, argv);
}
