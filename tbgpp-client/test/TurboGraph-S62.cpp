#include <iostream>
#include <iterator>
#include <cassert> 
#include <filesystem>
#include <string>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <memory>
#include <string>
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>


#include <nlohmann/json.hpp>	// TODO remove json and use that of boost
using json = nlohmann::json;

#include <icecream.hpp>

//#include "livegraph.hpp"
// #include "plans/query_plan_suite.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
// #include "storage/livegraph_catalog.hpp"

//#include "common/types/chunk_collection.hpp"

//#include "typedef.hpp"

#include "tblr.h"
using namespace tblr;

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"


#include "main/database.hpp"
#include "main/client_context.hpp"
#include "extent/extent_manager.hpp"
#include "extent/extent_iterator.hpp"
#include "index/index.hpp"
#include "index/art/art.hpp"
#include "cache/chunk_cache_manager.h"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/graph_csv_reader.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include "common/error_handler.hpp"

// compiler-related
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
#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "cypher_lexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"

#include "BTNode.h"

#include "mdprovider/MDProviderTBGPP.h"

using namespace antlr4;
using namespace gpopt;

CUnittest* m_rgut = NULL;
ULONG m_ulTests = 0;
ULONG m_ulNested = 0;
void (*m_pfConfig)() = NULL;
void (*m_pfCleanup)() = NULL;


using namespace duckdb;

vector<std::pair<string, string>> vertex_files;
vector<std::pair<string, string>> edge_files;
vector<std::pair<string, string>> edge_files_backward;
string workspace;

bool load_edge;
bool load_backward_edge;

typedef std::pair<idx_t, idx_t> LidPair;

void helper_deallocate_objects_in_shared_memory () {
  string server_socket = "/tmp/catalog_server";
  // setup unix domain socket with storage
  int server_conn_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_conn_ < 0) {
    perror("cannot socket");
    exit(-1);
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  strncpy(addr.sun_path, server_socket.c_str(), server_socket.size());
  addr.sun_family = AF_UNIX;
  int status = connect(server_conn_, (struct sockaddr *)&addr, sizeof(addr));
  if (status < 0) {
    perror("cannot connect to the store");
    exit(-1);
  }

  bool reinitialize_done = false;

  int nbytes_recv = recv(server_conn_, &reinitialize_done, sizeof(bool), 0);
  if (nbytes_recv != sizeof(bool)) {
    perror("error receiving the reinitialize_done bit");
    exit(-1);
  }

  if (!reinitialize_done) {
    std::cerr << "Re-initialize failure!" << std::endl;
    exit(-1);
  }

  fprintf(stdout, "Re-initialize shared memory\n");
}

void exportQueryPlanVisualizer(std::vector<CypherPipelineExecutor*>& executors, std::string start_time, int exec_time=0, bool is_debug=false);

class InputParser{
  public:
    InputParser (int &argc, char **argv){
      for (int i=1; i < argc; ++i) {
		this->tokens.push_back(std::string(argv[i]));
      }
    }
    void getCmdOption() const {
      std::vector<std::string>::const_iterator itr;
      for (itr = this->tokens.begin(); itr != this->tokens.end(); itr++) {
    	std::string current_str = *itr;
        if (std::strncmp(current_str.c_str(), "--nodes:", 8) == 0) {
        	std::pair<std::string, std::string> pair_to_insert;
        	pair_to_insert.first = std::string(*itr).substr(8);
        	itr++;
        	pair_to_insert.second = *itr;
        	vertex_files.push_back(pair_to_insert);
        } else if (std::strncmp(current_str.c_str(), "--relationships:", 16) == 0) {
        	std::pair<std::string, std::string> pair_to_insert;
        	pair_to_insert.first = std::string(*itr).substr(16);
        	itr++;
        	pair_to_insert.second = *itr;
        	edge_files.push_back(pair_to_insert);
        	load_edge = true;
        } else if (std::strncmp(current_str.c_str(), "--relationships_backward:", 25) == 0) {
        	// TODO check if a corresponding forward edge exists
        	std::pair<std::string, std::string> pair_to_insert;
        	pair_to_insert.first = std::string(*itr).substr(25);
        	itr++;
        	pair_to_insert.second = *itr;
        	edge_files_backward.push_back(pair_to_insert);
        	load_backward_edge = true;
        } else if (std::strncmp(current_str.c_str(), "--workspace:", 12) == 0) {
			workspace = std::string(*itr).substr(12);
		}
      }
    }
  private:
    std::vector <std::string> tokens;
};

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
	MDProviderTBGPP * provider = NULL;
	CMemoryPool *mp = NULL; 
	{
		CAutoMemoryPool amp;
		mp = amp.Pmp();
		//auto md_path = "../tbgpp-compiler/gpdb/src/backend/gporca/data/dxl/metadata/md.xml";
		//auto md_path = "../tbgpp-compiler/test/minidumps/TPCH_1_metaonly.mdp";
		auto md_path = "/home/tslee/turbograph-v3/tbgpp-compiler/test/minidumps/q1_metaonly.mdp";

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
		gpdbcost::CCostModelGPDB* default_cost_model = GPOS_NEW(mp) CCostModelGPDB(mp, GPOPT_TEST_SEGMENTS);
		CAutoOptCtxt aoc(mp, &mda, NULL, /* pceeval */ default_cost_model);

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
		
		// mp safely deallocated when exiting the scope
	}

	
// cleanup cache and mdprovider
	CMDCache::Shutdown();
	CRefCount::SafeRelease(provider);
	CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
}

int main(int argc, char** argv) {
icecream::ic.disable();
	// Initialize System
	InputParser input(argc, argv);
	input.getCmdOption();
	// set_signal_handler();
	// setbuf(stdout, NULL);

	// fprintf(stdout, "Initialize DiskAioParameters\n\n");
	// Initialize System Parameters
	DiskAioParameters::NUM_THREADS = 1;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
	DiskAioParameters::NUM_CPU_SOCKETS = 1;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = workspace;
	fprintf(stdout, "Workspace: %s\n", DiskAioParameters::WORKSPACE.c_str());
	
	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

	// Initialize ChunkCacheManager
	// fprintf(stdout, "\nInitialize ChunkCacheManager\n");
	ChunkCacheManager::ccm = new ChunkCacheManager(DiskAioParameters::WORKSPACE.c_str());

	// Run Catch Test
	argc = 1;

	// Initialize Database
	// helper_deallocate_objects_in_shared_memory(); // Initialize shared memory for Catalog
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(DiskAioParameters::WORKSPACE.c_str());
	
	// Initialize ClientContext
	icecream::ic.disable();
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());

	// bind orca

	std::cout << "[TEST] orca init / params" << std::endl;
	struct gpos_init_params gpos_params = {NULL};

	gpos_init(&gpos_params);
	gpdxl_init();
	gpopt_init();

	INT iArgs = 3;
	const std::vector<std::string> arguments = { "/home/tslee/turbograph-v3/build/tbgpp-client/TurboGraph-S62", "-U", "CEngineTest" };
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
	
	// run queries by query name
	std::string query_str;
	std::vector<CypherPipelineExecutor*> executors;
icecream::ic.disable();
// 	while(true) {
// 		std::cout << "TurboGraph-S62 >> "; std::getline(std::cin, query_str);
// 		// check termination
// 		if( query_str.compare(":exit") == 0 ) {
// 			break;
// 		}

// 		std::cout << "Query => " << std::endl << query_str << std::endl;
// 		auto inputStream = ANTLRInputStream(query_str);

// // Lexer
// 		std::cout << "[TEST] calling lexer" << std::endl;
// 		auto cypherLexer = CypherLexer(&inputStream);
// 		//cypherLexer.removeErrorListeners();
// 		//cypherLexer.addErrorListener(&parserErrorListener);
// 		auto tokens = CommonTokenStream(&cypherLexer);
// 		tokens.fill();

// // Parser
// 		std::cout << "[TEST] generating and calling KuzuCypherParser" << std::endl;
// 		auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);

// // Sematic parsing
// 		// Transformer
// 		std::cout << "[TEST] generating transformer" << std::endl;
// 		kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
// 		std::cout << "[TEST] calling transformer" << std::endl;
// 		auto statement = transformer.transform();
		
// 		// Binder
// 		std::cout << "[TEST] generating binder" << std::endl;
// 		auto binder = kuzu::binder::Binder();
// 		std::cout << "[TEST] calling binder" << std::endl;
// 		auto boundStatement = binder.bind(*statement);
// 		kuzu::binder::BoundStatement * bst = boundStatement.get();
// 		BTTree<kuzu::binder::ParseTreeNode> printer(bst, &kuzu::binder::ParseTreeNode::getChildren, &kuzu::binder::BoundStatement::getName);
// 		// WARNING - printer should be disabled when processing following compilation step.
// 		std::cout << "Tree => " << std::endl;
// 		printer.print();
// 		std::cout << std::endl;
// 	}

	std::cout << "[TEST] orca engine" << std::endl;
	auto gpos_output_code = gpos_exec(&params);
	std::cout << "[TEST] function outuput " << gpos_output_code << std::endl;

	std::cout << "compiler test end" << std::endl;
	// Goodbye
	std::cout << "Bye." << std::endl;

	// Destruct ChunkCacheManager
  	delete ChunkCacheManager::ccm;
	return 0;
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root, bool is_debug);
json* attachTime(json* j, CypherPhysicalOperator* op, bool is_root, float* accum_time);

void exportQueryPlanVisualizer(std::vector<CypherPipelineExecutor*>& executors, std::string start_time, int query_exec_time_ms, bool is_debug) {	// default = 0, false

	// output file
	
	std::replace( start_time.begin(), start_time.end(), ' ', '_');
	boost::filesystem::create_directories("execution-log/");

	std::string filename = "execution-log/" + start_time;
	if( is_debug ) filename += "_debug";
	if( ! is_debug ) {
		std::cout << "saving query profile result in : " << "build/execution-log/" << filename << ".html" << std::endl << std::endl;
	}
	std::ofstream file( filename + ".html" );

	// https://tomeko.net/online_tools/cpp_text_escape.php?lang=en
	std::string html_1 = "<script src=\"https://code.jquery.com/jquery-3.4.1.js\" integrity=\"sha256-WpOohJOqMqqyKL9FccASB9O0KwACQJpFTUBLTYOVvVU=\" crossorigin=\"anonymous\"></script>\n<script src=\"https://unpkg.com/vue@3.2.37/dist/vue.global.prod.js\"></script>\n<script src=\"https://unpkg.com/pev2/dist/pev2.umd.js\"></script>\n<link\n  href=\"https://unpkg.com/bootstrap@4.5.0/dist/css/bootstrap.min.css\"\n  rel=\"stylesheet\"\n/>\n<link rel=\"stylesheet\" href=\"https://unpkg.com/pev2/dist/style.css\" />\n\n<div id=\"app\">\n  <pev2 :plan-source=\"plan\" plan-query=\"\" />\n</div>\n\n<script>\n  const { createApp } = Vue\n  \n  const plan = `";
	std::string html_2 = "`\n\n  const app = createApp({\n    data() {\n      return {\n        plan: plan,\n      }\n    },\n  })\n  app.component(\"pev2\", pev2.Plan)\n  app.mount(\"#app\")\n$(\".plan-container\").css('height','100%')\n  </script>\n";

	json j = json::array( { json({}), } );
	if(!is_debug) {
		j[0]["Execution Time"] = query_exec_time_ms;
	}
	
	// reverse-iterate executors
	json* current_root = &(j[0]);
	bool isRootOp = true;	// is true for only one operator
	
	for (auto it = executors.crbegin() ; it != executors.crend(); ++it) {
  		duckdb::CypherPipeline* pipeline = (*it)->pipeline;
		// reverse operator
		for (auto it2 = pipeline->operators.crbegin() ; it2 != pipeline->operators.crend(); ++it2) {
			current_root = operatorToVisualizerJSON( current_root, *it2, isRootOp, is_debug );
			if( isRootOp ) { isRootOp = false; }
		}
		// source
		current_root = operatorToVisualizerJSON( current_root, pipeline->source, isRootOp, is_debug );
		if( isRootOp ) { isRootOp = false; }
	}
	
	file << html_1;
	file << j.dump(4);
	file << html_2;

	// close file
	file.close();
}

json* operatorToVisualizerJSON(json* j, CypherPhysicalOperator* op, bool is_root, bool is_debug) {
	json* content;
	if( is_root ) {
		(*j)["Plan"] = json({});
		content = &((*j)["Plan"]);
	} else {
		if( (*j)["Plans"].is_null() ) {
			// single child
			(*j)["Plans"] = json::array( { json({}), } );
		} else {
			// already made child with two childs. so pass
		}
		content = &((*j)["Plans"][0]);
	}
	(*content)["Node Type"] = op->ToString();

	if(!is_debug) {
		(*content)["*Duration (exclusive)"] = op->op_timer.elapsed().wall / 1000000.0; // + (*accum_time);
		// (*accum_time) += op->op_timer.elapsed().wall / 1000000.0 ;
		(*content)["Actual Rows"] = op->processed_tuples;
		(*content)["Actual Loops"] = 1; // meaningless
	}
	// output shcma
	(*content)["Output Schema"] = op->schema.toString();

	// add child when operator is 
	if( op->ToString().compare("AdjIdxJoin") == 0 ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "AdjIdxJoinBuild";
	} else if( op->ToString().compare("NodeIdSeek") == 0  ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "NodeIdSeekBuild";
	} else if( op->ToString().compare("EdgeIdSeek") == 0  ) {
		(*content)["Plans"] = json::array( { json({}), json({})} );
		auto& rhs_content = (*content)["Plans"][1];
		(rhs_content)["Node Type"] = "EdgeIdSeekBuild";
	}

	return content;
}
