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
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformFactory.h"
#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "cypher_lexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"

#include "BTNode.h"

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

	CWStringConst strName(GPOS_WSZ_LIT("BaseTable1"));
	CTableDescriptor *ptabdesc =
		CTestUtils::PtabdescCreate(mp, 16,										// width 2
					   GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, 1716555, 1, 1),	// 6 16467 1 1 - tpch-1
					   CName(&strName));										// basetable

	CWStringConst strAlias(GPOS_WSZ_LIT("BaseTableAlias1"));
	return CTestUtils::PexprLogicalGet(mp, ptabdesc, &strAlias);
}

CExpression * genLogicalGet2(CMemoryPool *mp) {

	CWStringConst strName(GPOS_WSZ_LIT("BaseTable2"));
	CTableDescriptor *ptabdesc =
		CTestUtils::PtabdescCreate(mp, 16,										// width
					   GPOS_NEW(mp) CMDIdGPDB(IMDId::EmdidRel, 17165, 1, 1),	// 6 16467 1 1
					   CName(&strName));										// basetable

	CWStringConst strAlias(GPOS_WSZ_LIT("BaseTableAlias2"));

	// to generate logicalget, pass tablename / tabledesc(relwidth, mdid, name) / alias / + colmarkasused
	return CTestUtils::PexprLogicalGet(mp, ptabdesc, &strAlias);
}

CExpression * genPlan(gpos::CMemoryPool* mp) {
	// TODO genplan
	return nullptr;
}

static void * MyOrcaTestExec(void *pv) {
	std::cout << "[TEST] inside MyOrcaTestExec()" << std::endl;
	CMainArgs *pma = (CMainArgs *) pv;

	

// init dxl and cache
	InitDXL();
	CMDCache::Init();
// load metadata objects into provider file
	CMDProviderMemory * provider = NULL;
	CMemoryPool *mp = NULL; 
	{
		CAutoMemoryPool amp;
		mp = amp.Pmp();
		//auto md_path = "../tbgpp-compiler/gpdb/src/backend/gporca/data/dxl/metadata/md.xml";
		//auto md_path = "../tbgpp-compiler/test/minidumps/TPCH_1_metaonly.mdp";
		auto md_path = "../tbgpp-compiler/test/minidumps/q1_metaonly.mdp";

		provider = new (mp, __FILE__, __LINE__) CMDProviderMemory(mp, md_path);
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
	// connect provider
		CMDProviderMemory *pmdp = provider;
		pmdp->AddRef();

	// separate memory pool used for accessor
		CAutoMemoryPool amp;
		CMemoryPool *mp = amp.Pmp();
				
	// to generate accessor, provide local pool, global cache and provider
			// TODO what is m_sysidDefault for, systemid
		CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);	
	// install opt context in TLS
		gpdbcost::CCostModelGPDB* default_cost_model = GPOS_NEW(mp) CCostModelGPDB(mp, GPOPT_TEST_SEGMENTS);
		CAutoOptCtxt aoc(mp, &mda, NULL, /* pceeval */ default_cost_model);

	// initialize engine
		CEngine eng(mp);
		
	// define join plan expression
		CExpression *lhs_get = genLogicalGet1(mp);
		CExpression *rhs_get = genLogicalGet2(mp);
		CExpression *pexpr = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs_get, rhs_get);
		//CExpression* pexpr = lhs_get;

		{
			std::cout << "[TEST] logical plan string" << std::endl;
			CWStringDynamic str(mp);
			COstreamString oss(&str);
			pexpr->OsPrint(oss);
			GPOS_TRACE(str.GetBuffer());
		}
		
	
	// generate query context
		CQueryContext *pqc = CTestUtils::PqcGenerate(mp, pexpr);

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
		
		// mp safely deallocated when exiting the scope
	}

	

// cleanup cache and mdprovider
	CMDCache::Shutdown();
	CRefCount::SafeRelease(provider);
	CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
}

int _main(int argc, char** argv) {
 
	std::cout << "compiler test start" << std::endl;
	// std::string query = "MATCH (n) RETURN n;";
	//std::string query = "EXPLAIN MATCH (n) RETURN n;";
	std::string query = "MATCH (n), (m)-[r]->(z) WITH n,m,r,z MATCH (x), (y) RETURN m,n,x,y UNION MATCH (n) RETURN n;";
	
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
	auto binder = kuzu::binder::Binder();
	std::cout << "[TEST] calling binder" << std::endl;
	auto boundStatement = binder.bind(*statement);
	kuzu::binder::BoundStatement * bst = boundStatement.get();
	BTTree<kuzu::binder::ParseTreeNode> printer(bst, &kuzu::binder::ParseTreeNode::getChildren, &kuzu::binder::BoundStatement::getName);
	// WARNING - printer should be disabled when processing following compilation step.
	std::cout << "Tree => " << std::endl;
	printer.print();
	std::cout << std::endl;


// bind orca

	std::cout << "[TEST] orca init / params" << std::endl;
	struct gpos_init_params gpos_params = {NULL};

	gpos_init(&gpos_params);
	gpdxl_init();
	gpopt_init();

	INT iArgs = 3;
	const std::vector<std::string> arguments = { "/turbograph-v3/build/tbgpp-compiler/kuzu_integration_test", "-U", "CEngineTest" };
	std::vector<const char*> argvv;
	for (const auto& arg : arguments)
		argvv.push_back((const char*)arg.data());
	argvv.push_back(nullptr);
	const CHAR** rgszArgs = argvv.data();

	CMainArgs ma(iArgs, rgszArgs, "uU:d:xT:i:");
	//CUnittest::Init(rgut, GPOS_ARRAY_SIZE(rgut), ConfigureTests, Cleanup);
	// the static members in CUnitTest.cpp is re-defined in our file
	m_rgut = rgut;
	m_ulTests = GPOS_ARRAY_SIZE(rgut);
	m_pfConfig = ConfigureTests;
	m_pfCleanup = Cleanup;

	gpos_exec_params params;
	params.func = MyOrcaTestExec;
	params.arg = &ma;
	params.stack_start = &params;
	params.error_buffer = NULL;
	params.error_buffer_size = -1;
	params.abort_requested = NULL;

	// std::cout << "[TEST] orca memory pool" << std::endl;
	std::cout << "[TEST] orca engine" << std::endl;
	auto gpos_output_code = gpos_exec(&params);
	std::cout << "[TEST] function outuput " << gpos_output_code << std::endl;

	std::cout << "compiler test end" << std::endl;
	return 0;
}

int main(int argc, char** argv) {
	try{
		int out = _main(argc, argv);
	} catch (CException ex) {
		std::cerr << ex.Major() << " " << ex.Minor() << std::endl;
	}
}