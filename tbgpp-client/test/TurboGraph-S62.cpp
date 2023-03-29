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
#include "plans/query_plan_suite.hpp"
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

#include "planner/planner.hpp"	// planner should go ahead of kuzu/* to avoid ambiguity

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"

#include "BTNode.h"

#include "mdprovider/MDProviderTBGPP.h"

#include "catalog/catalog_wrapper.hpp"
#include "tbgppdbwrappers.hpp"

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
string input_query_string;
bool is_query_string_given = false;

s62::PlannerConfig planner_config;

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
		} else if (std::strncmp(current_str.c_str(), "--query:", 8) == 0) {
			input_query_string = std::string(*itr).substr(8);
			is_query_string_given = true;
		} else if (std::strncmp(current_str.c_str(), "--debug-orca", 12) == 0) {
			planner_config.ORCA_DEBUG_PRINT = true;
		} else if (std::strncmp(current_str.c_str(), "--explain", 9) == 0) {
			planner_config.DEBUG_PRINT = true;
		} else if (std::strncmp(current_str.c_str(), "--index-join-only", 17) == 0) {
			planner_config.INDEX_JOIN_ONLY = true;
		} else if (std::strncmp(current_str.c_str(), "--run-plan", 10) == 0) {
			planner_config.RUN_PLAN_WO_COMPILE = true;
		} else if (std::strncmp(current_str.c_str(), "--join-order-optimizer:", 23) == 0) {
			std::string optimizer_join_order = std::string(*itr).substr(23);
			if(optimizer_join_order == "query") {
				planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_IN_QUERY;
			} else if (optimizer_join_order == "greedy") {
				planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_GREEDY_SEARCH;
			} else if (optimizer_join_order == "exhaustive") {
				planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH;
			} else if (optimizer_join_order == "exhaustive2") {
				planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE2_SEARCH;
			} else {
				// default
				throw std::invalid_argument("wrong --join-order-optimizer parameter");
			}
		}
      }
    }
  private:
    std::vector <std::string> tokens;
};

void CompileAndRun(string& query_str, std::shared_ptr<ClientContext> client, s62::Planner& planner) {

	
	boost::timer::cpu_timer compile_timer;
	compile_timer.start();

/*
	COMPILATION
*/

	if (planner_config.DEBUG_PRINT) {
		std::cout << "Query => " << std::endl << query_str << std::endl;
	}

	if (!planner_config.RUN_PLAN_WO_COMPILE) {
		auto inputStream = ANTLRInputStream(query_str);

		// Lexer		
		auto cypherLexer = CypherLexer(&inputStream);
		//cypherLexer.removeErrorListeners();
		//cypherLexer.addErrorListener(&parserErrorListener);
		auto tokens = CommonTokenStream(&cypherLexer);
		tokens.fill();

		// Parser
		auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);

		// Sematic parsing
		// Transformer
		kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
		auto statement = transformer.transform();
		
		// Binder
		auto binder = kuzu::binder::Binder(client.get());
		auto boundStatement = binder.bind(*statement);
		kuzu::binder::BoundStatement * bst = boundStatement.get();

		if(planner_config.DEBUG_PRINT) {
			// BTTree<kuzu::binder::ParseTreeNode> printer(bst, &kuzu::binder::ParseTreeNode::getChildNodes, &kuzu::binder::BoundStatement::getName);
			// std::cout << "Tree => " << std::endl;
			// printer.print();
			// std::cout << std::endl;
		}

		boost::timer::cpu_timer orca_compile_timer;
		orca_compile_timer.start();
		planner.execute(bst);

		int compile_time_ms = compile_timer.elapsed().wall / 1000000.0;
		int orca_compile_time_ms = orca_compile_timer.elapsed().wall / 1000000.0;

	/*
		EXECUTE QUERY
	*/
		auto executors = planner.genPipelineExecutors();
		if( executors.size() == 0 ) { std::cerr << "Plan empty!!" << std::endl; return; }

		// start timer
		boost::timer::cpu_timer query_timer;
		query_timer.start();
		int idx = 0;
		std::cout << "[EXECUTION PLAN]" << std::endl;
		for( auto exec : executors ) { 
			if(planner_config.DEBUG_PRINT) {
				std::cout << "  (pipeline " << 1 + idx++ << ")" << std::endl;
				std::cout << exec->pipeline->toString() << std::endl;
			}
			exec->ExecutePipeline();
			// if(planner_config.DEBUG_PRINT) {
			// 	std::cout << "done pipeline execution!!" << std::endl;
			// }
		}
		// end_timer
		int query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;

	/*
		DUMP RESULT
	*/
		const auto BLUE = "\033[1;34m";
		const auto CLEAR = "\033[0m";
		const auto UNDERLINE = "\033[1;4m";
		const auto GREEN = "\033[1;32m";


		int LIMIT = 10;
		size_t num_total_tuples = 0;
		D_ASSERT( executors.back()->context->query_results != nullptr );
		auto& resultChunks = *(executors.back()->context->query_results);
		auto& schema = executors.back()->pipeline->GetSink()->schema;
		for (auto &it : resultChunks) num_total_tuples += it->size();
		std::cout << "===================================================" << std::endl;
		std::cout << "[ResultSetSummary] Total " <<  num_total_tuples << " tuples. ";
		if( LIMIT < num_total_tuples) {
			std::cout << "Showing top " << LIMIT <<":" << std::endl;
		} else {
			std::cout << std::endl;
		}
		
		Table t;
		t.layout(unicode_box_light_headerline());

		auto col_names = planner.getQueryOutputColNames();
		for( int i = 0; i < col_names.size(); i++ ) {
			t << col_names[i] ;
		}
		t << endr;

		if (num_total_tuples != 0) {
			auto& firstchunk = resultChunks[0];
			LIMIT = std::min( (int)(firstchunk->size()), LIMIT);

			// for( int i = 0; i < firstchunk->ColumnCount(); i++ ) {
			// 	t << firstchunk->GetTypes()[i].ToString(); 
			// }
			// t << endr;
			
			for( int idx = 0 ; idx < LIMIT ; idx++) {
				for( int i = 0; i < firstchunk->ColumnCount(); i++ ) {
					t << firstchunk->GetValue(i, idx).ToString();
				}
				t << endr;
			}
			std::cout << t << std::endl;
		}
		std::cout << "===================================================" << std::endl;
		std::cout << "\nCompile Time: "  << compile_time_ms << " ms (orca: " << orca_compile_time_ms << " ms) / " << "Query Execution Time: " << query_exec_time_ms << " ms" << std::endl << std::endl;
	} else { // For testing
		// load plans
		auto suite = QueryPlanSuite(*client.get());
		std::vector<CypherPipelineExecutor *> executors;

		executors = suite.getTest(query_str);
		if (executors.size() == 0) return;

		// start timer
		boost::timer::cpu_timer query_timer;
		query_timer.start();
		int idx = 0;
		for( auto exec : executors ) { 
			std::cout << "[Pipeline " << 1 + idx++ << "]" << std::endl;
			exec->ExecutePipeline();
			std::cout << "done pipeline execution!!" << std::endl;
		}
		// end_timer
		int query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;

		int LIMIT = 10;
		size_t num_total_tuples = 0;
		D_ASSERT( executors.back()->context->query_results != nullptr );
		auto& resultChunks = *(executors.back()->context->query_results);
		auto& schema = executors.back()->pipeline->GetSink()->schema;
		for (auto &it : resultChunks) num_total_tuples += it->size();
		std::cout << "===================================================" << std::endl;
		std::cout << "[ResultSetSummary] Total " <<  num_total_tuples << " tuples. ";
		if( LIMIT < num_total_tuples) {
			std::cout << "Showing top " << LIMIT <<":" << std::endl;
		} else {
			std::cout << std::endl;
		}
		
		Table t;
		t.layout(unicode_box_light_headerline());

		auto col_names = planner.getQueryOutputColNames();
		for( int i = 0; i < col_names.size(); i++ ) {
			t << col_names[i] ;
		}
		t << endr;

		if (num_total_tuples != 0) {
			auto& firstchunk = resultChunks[0];
			LIMIT = std::min( (int)(firstchunk->size()), LIMIT);

			// for( int i = 0; i < firstchunk->ColumnCount(); i++ ) {
			// 	t << firstchunk->GetTypes()[i].ToString(); 
			// }
			// t << endr;
			
			for( int idx = 0 ; idx < LIMIT ; idx++) {
				for( int i = 0; i < firstchunk->ColumnCount(); i++ ) {
					t << firstchunk->GetValue(i, idx).ToString();
				}
				t << endr;
			}
			std::cout << t << std::endl;
		}
		std::cout << "===================================================" << std::endl;
		std::cout << "\nQuery Execution Time: " << query_exec_time_ms << " ms" << std::endl << std::endl;
	}
}


int main(int argc, char** argv) {
icecream::ic.disable();

	// Init planner config
	planner_config = s62::PlannerConfig();

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
	duckdb::SetClientWrapper(client, make_shared<CatalogWrapper>(database->instance->GetCatalogWrapper()));

	// Run planner
	auto planner = s62::Planner(planner_config, s62::MDProviderType::TBGPP, client.get());
	
	// run queries by query name
	std::string query_str;
icecream::ic.disable();
	if (is_query_string_given) {
		// try {
		// 	// protected code
		// 	CompileAndRun(input_query_string, client);
		// } catch( std::exception e1 ) {
		// 	std::cerr << e1.what() << std::endl;
		// }
		CompileAndRun(input_query_string, client, planner);
	} else {
		while(true) {
			std::cout << "TurboGraph-S62 >> "; std::getline(std::cin, query_str);
			// check termination
			if( query_str.compare(":exit") == 0 ) {
				break;
			}

			try {
				// protected code
				CompileAndRun(query_str, client, planner);
			} catch( std::exception e1 ) {
				std::cerr << e1.what() << std::endl;
			}
		}
	}

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
