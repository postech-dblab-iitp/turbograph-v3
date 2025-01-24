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
#include <vector>
#include <memory>
#include <string>
#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>


#include <nlohmann/json.hpp>	// TODO remove json and use that of boost
using json = nlohmann::json;

#include <icecream.hpp>

#include "common/graph_csv_reader.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include "common/error_handler.hpp"

#include "plans/query_plan_suite.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"

#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"

#include "main/database.hpp"
#include "main/client_context.hpp"
#include "extent/extent_manager.hpp"
#include "extent/extent_iterator.hpp"
#include "index/index.hpp"
#include "index/art/art.hpp"
#include "statistics/histogram_generator.hpp"
#include "cache/chunk_cache_manager.h"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "catalog/catalog_entry/list.hpp"

// compiler-related
#include "gpos/_api.h"
#include "naucrates/init.h"
#include "gpopt/init.h"

#include "unittest/gpopt/engine/CEngineTest.h"
#include "gpos/test/CUnittest.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/base.h"
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

#include <readline/readline.h>
#include <readline/history.h>

using namespace antlr4;
using namespace gpopt;
using namespace duckdb;

CUnittest* m_rgut = NULL;
ULONG m_ulTests = 0;
ULONG m_ulNested = 0;
void (*m_pfConfig)() = NULL;
void (*m_pfCleanup)() = NULL;

vector<std::pair<string, string>> vertex_files;
vector<std::pair<string, string>> edge_files;
vector<std::pair<string, string>> edge_files_backward;
string workspace;
string input_query_string;
string dump_file_path;
bool is_query_string_given = false;
bool run_plan_wo_compile = false;
bool show_top_10_only = false;
bool dump_output = false;
bool is_compile_only = false;
bool is_orca_compile_only = false;
bool warmup = false;

s62::PlannerConfig planner_config;		// passed to query planner
bool enable_profile = false;			// passed to client context as config

bool load_edge;
bool load_backward_edge;

int eoq;

struct malloc_deleter
{
    template <class T>
    void operator()(T* p) { std::free(p); }
};

typedef std::unique_ptr<char, malloc_deleter> cstring_uptr;
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

// int
// bind_cr(int count, int key) {
// 	if (eoq == 1) {
// 		rl_done = 1;
// 		eoq = 0;
// 	}
// 	printf("\n");
// }

// int
// bind_eoq(int count, int key) {
// 	eoq = 1;

// 	printf(";");
// }

// int initialize_readline() {
//   eoq = 0;
// //   rl_bind_key('\n', bind_cr);
// //   rl_bind_key('\r', bind_cr);
// //   rl_bind_key(';', bind_eoq);
// }

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
		} else if (std::strncmp(current_str.c_str(), "--compile-only", 14) == 0) {
			is_compile_only = true;
		} else if (std::strncmp(current_str.c_str(), "--orca-compile-only", 19) == 0) {
			is_compile_only = true;
			planner_config.ORCA_COMPILE_ONLY = true;
		} else if (std::strncmp(current_str.c_str(), "--query:", 8) == 0) {
			input_query_string = std::string(*itr).substr(8);
			is_query_string_given = true;
		} else if (std::strncmp(current_str.c_str(), "--query-file:", 13) == 0) {
			std::string query_file_path = std::string(*itr).substr(13);
			std::ifstream query_file(query_file_path);
			if (query_file.is_open()) {
				std::stringstream buffer;
				buffer << query_file.rdbuf(); // Read the entire file content into a stringstream
				input_query_string = buffer.str();
				is_query_string_given = true;
				query_file.close();
			} else {
				std::cerr << "Error: Unable to open query file at " << query_file_path << std::endl;
				exit(-1); 
			}
		} else if (std::strncmp(current_str.c_str(), "--debug-orca", 12) == 0) {
			planner_config.ORCA_DEBUG_PRINT = true;
		} else if (std::strncmp(current_str.c_str(), "--explain", 9) == 0) {
			planner_config.DEBUG_PRINT = true;
		} else if (std::strncmp(current_str.c_str(), "--profile", 9) == 0) {
			enable_profile = true;
		} else if (std::strncmp(current_str.c_str(), "--index-join-only", 17) == 0) {
			planner_config.INDEX_JOIN_ONLY = true;
		} else if (std::strncmp(current_str.c_str(), "--hash-join-only", 16) == 0) {
			planner_config.HASH_JOIN_ONLY = true;
		} else if (std::strncmp(current_str.c_str(), "--merge-join-only", 17) == 0) {
			planner_config.MERGE_JOIN_ONLY = true;
		} else if (std::strncmp(current_str.c_str(), "--disable-merge-join", 20) == 0) {
			planner_config.DISABLE_MERGE_JOIN = true;
		} else if (std::strncmp(current_str.c_str(), "--run-plan", 10) == 0) {
			run_plan_wo_compile = true;
		} else if (std::strncmp(current_str.c_str(), "--show-top", 10) == 0) {
			show_top_10_only = true;
		} else if (std::strncmp(current_str.c_str(), "--dump-output", 10) == 0) {
			itr++;
			dump_file_path = std::string(*itr);
			dump_output = true;
		} else if (std::strncmp(current_str.c_str(), "--num-iterations:", 17) == 0) {
			std::string num_iter = std::string(*itr).substr(17);
			planner_config.num_iterations = std::stoi(num_iter);
		} else if (std::strncmp(current_str.c_str(), "--warmup", 8) == 0) {
			warmup = true;
		} else if (std::strncmp(current_str.c_str(), "--join-order-optimizer:", 23) == 0) {
			std::string optimizer_join_order = std::string(*itr).substr(23);
			if (optimizer_join_order == "query") {
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
		} else if (std::strncmp(current_str.c_str(), "--join-order-dp-threshold:", 26) == 0) {
			auto int_str = std::string(*itr).substr(26);
			uint8_t threshold =  std::stoi(int_str);
			planner_config.JOIN_ORDER_DP_THRESHOLD_CONFIG = threshold;
		}
      }
    }
  private:
    std::vector <std::string> tokens;
};

void printOutput(s62::Planner& planner, std::vector<unique_ptr<duckdb::DataChunk>> &resultChunks, duckdb::Schema &schema) {
	PropertyKeys col_names;
	col_names = planner.getQueryOutputColNames();
	if (col_names.size() == 0) {
		col_names = schema.getStoredColumnNames();
	}

	if (dump_output) {
		std::cout << "Dump Output File. Path: " << dump_file_path << std::endl;
		int chunk_idx = 0;
		std::ofstream dump_file;
		dump_file.open(dump_file_path.c_str());
		for (int i = 0; i < col_names.size(); i++) {
			dump_file << col_names[i];
			if (i != col_names.size() - 1) dump_file << "|";
		}
		dump_file << "\n";
		while (chunk_idx < resultChunks.size()) {
			auto &chunk = resultChunks[chunk_idx];
			auto num_cols = chunk->ColumnCount();
			for (int idx = 0 ; idx < chunk->size(); idx++) {
				for (int i = 0; i < num_cols; i++) {
					dump_file << chunk->GetValue(i, idx).ToString();
					if (i != num_cols - 1) dump_file << "|";
				}
				dump_file << "\n";
			}
			chunk_idx++;
		}
		std::cout << "Dump Done!" << std::endl << std::endl;;
	}

	OutputUtil::PrintQueryOutput(col_names, resultChunks, show_top_10_only);
}

void CompileAndRun(string& query_str, std::shared_ptr<ClientContext> client, s62::Planner& planner, kuzu::binder::Binder &binder) {
/*
	COMPILATION
*/
	show_top_10_only = planner_config.num_iterations > 1;

	if (!run_plan_wo_compile) {
		// start timer
		vector<double> query_execution_times;
		vector<double> query_compile_times;
		for (int i = 0; i < planner_config.num_iterations; i++) {
			boost::timer::cpu_timer compile_timer;
			// boost::timer::cpu_timer parse_timer;
			// boost::timer::cpu_timer transform_timer;
			// boost::timer::cpu_timer bind_timer;

			compile_timer.start();
			// parse_timer.start();

			auto inputStream = ANTLRInputStream(query_str);

			// Lexer		
			auto cypherLexer = CypherLexer(&inputStream);
			//cypherLexer.removeErrorListeners();
			//cypherLexer.addErrorListener(&parserErrorListener);
			auto tokens = CommonTokenStream(&cypherLexer);
			tokens.fill();

			if (planner_config.DEBUG_PRINT) {
				std::cout << "Parsing/Lexing Done" << std::endl;
			}

			// Parser
			auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);
			// parse_timer.stop();

			// Sematic parsing
			// Transformer
			// transform_timer.start();
			kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
			auto statement = transformer.transform();
			// transform_timer.stop();

			if (planner_config.DEBUG_PRINT) {
				std::cout << "Transformation Done" << std::endl;
			}
			
			// Binder
			// bind_timer.start();
			auto boundStatement = binder.bind(*statement);
			kuzu::binder::BoundStatement *bst = boundStatement.get();
			// bind_timer.stop();

			if (planner_config.DEBUG_PRINT) {
				BTTree<kuzu::binder::ParseTreeNode> printer(bst, &kuzu::binder::ParseTreeNode::getChildNodes, &kuzu::binder::BoundStatement::getName);
				std::cout << "Tree => " << std::endl;
				printer.print();
				std::cout << std::endl;
			}

			boost::timer::cpu_timer orca_compile_timer;
			orca_compile_timer.start();
			planner.execute(bst);

			auto compile_time_ms = compile_timer.elapsed().wall / 1000000.0;
			auto orca_compile_time_ms = orca_compile_timer.elapsed().wall / 1000000.0;
			// auto parse_time_ms = parse_timer.elapsed().wall / 1000000.0;
			// auto transform_time_ms = transform_timer.elapsed().wall / 1000000.0;
			// auto bind_time_ms = bind_timer.elapsed().wall / 1000000.0;
			query_compile_times.push_back(compile_time_ms);

			// std::cout << "\nCompile Time: "  << compile_time_ms << " ms (orca: " << orca_compile_time_ms << " ms)" << std::endl;

	/*
		EXECUTE QUERY
	*/

			if (!is_compile_only) {
				auto executors = planner.genPipelineExecutors();
				if (executors.size() == 0) { std::cerr << "Plan empty!!" << std::endl; return; }
				std::string curtime = boost::posix_time::to_simple_string(boost::posix_time::second_clock::universal_time());

				boost::timer::cpu_timer query_timer;
				auto &profiler = QueryProfiler::Get(*client.get());
				// start profiler
				profiler.StartQuery(query_str, enable_profile);	// is putting enable_profile ok?
				// initialize profiler for tree root
				profiler.Initialize(executors[executors.size()-1]->pipeline->GetSink()); // root of the query plan tree
				query_timer.start();
				int idx = 0;
				for (auto exec : executors) { 
					if (planner_config.DEBUG_PRINT) {
						std::cout << "[Pipeline " << 1 + idx++ << "]" << std::endl;
						std::cout << exec->pipeline->toString() << std::endl;
					}
				}
				idx=0;
				for (auto exec : executors) { 
					if (planner_config.DEBUG_PRINT) {
						std::cout << "[Pipeline " << 1 + idx++ << "]" << std::endl;
					}
					exec->ExecutePipeline();
					if (planner_config.DEBUG_PRINT) {
						std::cout << "done pipeline execution!!" << std::endl;
					}
				}

				// end_timer
				auto query_exec_time_ms = query_timer.elapsed().wall / 1000000.0;
				query_execution_times.push_back(query_exec_time_ms);
				// end profiler
				profiler.EndQuery();

			/*
				DUMP RESULT
			*/

				D_ASSERT(executors.back()->context->query_results != nullptr);
				auto &resultChunks = *(executors.back()->context->query_results);
				auto &schema = executors.back()->pipeline->GetSink()->schema;
				printOutput(planner, resultChunks, schema);
                std::cout << "\nCompile Time: " << compile_time_ms
                          << " ms (orca: " << orca_compile_time_ms << " ms) / "
                          << "Query Execution Time: " << query_exec_time_ms
                          << " ms" << std::endl
                          << std::endl;
                // std::cout << "Parse Time: " << parse_time_ms << " ms / "
                //           << "Transform Time: " << transform_time_ms << " ms / "
                //           << "Bind Time: " << bind_time_ms << " ms" << std::endl
                //           << std::endl;

                if (planner_config.num_iterations != 1) {
					std::cout << "Iteration " << i + 1 << " done" << std::endl;
					sleep(1);
				}
			}
		}
		if(warmup) {
			if (query_execution_times.size() > 0) {
				query_execution_times.erase(query_execution_times.begin());
			}
			if (query_compile_times.size() > 0) {
				query_compile_times.erase(query_compile_times.begin());
			}
		}
		double average_exec_time = std::accumulate(query_execution_times.begin(), query_execution_times.end(), 0.0) / query_execution_times.size();
		double average_compile_time = std::accumulate(query_compile_times.begin(), query_compile_times.end(), 0.0) / query_compile_times.size();

		std::cout << "Average Query Execution Time: " << average_exec_time << " ms" << std::endl;
		std::cout << "Average Compile Time: " << average_compile_time << " ms" << std::endl;
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

		D_ASSERT( executors.back()->context->query_results != nullptr );
		auto& resultChunks = *(executors.back()->context->query_results);
		auto& schema = executors.back()->pipeline->GetSink()->schema;
		// printOutput(planner, resultChunks, schema);
		
		std::cout << "\nQuery Execution Time: " << query_exec_time_ms << " ms" << std::endl << std::endl;
	}
}

int main(int argc, char** argv) {
	// Init planner config
	planner_config = s62::PlannerConfig();

	// Check validity
	if (planner_config.INDEX_JOIN_ONLY + planner_config.HASH_JOIN_ONLY + planner_config.MERGE_JOIN_ONLY
		+ planner_config.DISABLE_INDEX_JOIN + planner_config.DISABLE_HASH_JOIN + planner_config.DISABLE_MERGE_JOIN > 1) {
		std::cout << "Error: Only one of INDEX_JOIN_ONLY, HASH_JOIN_ONLY, MERGE_JOIN_ONLY can be true" << std::endl;
		return 1;
	}

	// Initialize System
	InputParser input(argc, argv);
	input.getCmdOption();
	using_history();
	read_history((workspace + "/.history").c_str());
	// if (isatty(STDIN_FILENO)) {
    // 	rl_startup_hook = initialize_readline;
	// }
	// set_signal_handler();
	// setbuf(stdout, NULL);

	// Initialize System Parameters
	DiskAioParameters::NUM_THREADS = 32;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 32;
	DiskAioParameters::NUM_CPU_SOCKETS = 2;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = workspace;
	fprintf(stdout, "\nWorkspace: %s\n\n", DiskAioParameters::WORKSPACE.c_str());
	
	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

	// Initialize ChunkCacheManager
	ChunkCacheManager::ccm = new ChunkCacheManager(DiskAioParameters::WORKSPACE.c_str());

	// Initialize Database
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(DiskAioParameters::WORKSPACE.c_str());
	
	// Initialize ClientContext
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());

	duckdb::SetClientWrapper(client, make_shared<CatalogWrapper>( database->instance->GetCatalogWrapper()));
	if (enable_profile) {
		client.get()->EnableProfiling();
	} else {
		client.get()->DisableProfiling();
	}

	// Run planner
	auto planner = s62::Planner(planner_config, s62::MDProviderType::TBGPP, client.get());
	auto binder = kuzu::binder::Binder(client.get());
	
	// run queries by query name
	cstring_uptr input_cmd;
	string shell_prompt = "TurboGraph-S62 >> ";
	string prev_query_str;
	string query_str;

	if (is_query_string_given) {
		if (input_query_string.compare("analyze") == 0) {
			HistogramGenerator hist_gen;
			hist_gen.CreateHistogram(client);
		} else if (input_query_string.compare("flush_file_meta") == 0) {
			ChunkCacheManager::ccm->FlushMetaInfo(DiskAioParameters::WORKSPACE.c_str());
		} else {
			CompileAndRun(input_query_string, client, planner, binder);
		}
	} else {
		while(true) {
			std::cout << "TurboGraph-S62 >> "; std::getline(std::cin, query_str, ';');
			std::cin.ignore();
			if (query_str.compare(":exit") == 0) {
				break;
			} else if (query_str.compare("analyze") == 0) {
				HistogramGenerator hist_gen;
				hist_gen.CreateHistogram(client);
			} else if (query_str.compare("flush_file_meta") == 0) {
				ChunkCacheManager::ccm->FlushMetaInfo(DiskAioParameters::WORKSPACE.c_str());
			} else {
				if (query_str != prev_query_str) {
					add_history(query_str.c_str());
					write_history((DiskAioParameters::WORKSPACE + "/.history").c_str());
					prev_query_str = query_str;
				}

				try {
					// protected code
					CompileAndRun(query_str, client, planner, binder);
				} catch (duckdb::Exception e) {
					std::cerr << e.what() << std::endl;
				} catch (std::exception &e1) {
					std::cerr << "Unexpected Exception" << std::endl;
					std::cerr << "Caught: " << e1.what() << std::endl;
					std::cerr << "Type: " << typeid(e1).name() << std::endl;
				}
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
		auto operators = pipeline->GetOperators();
		for (auto it2 = operators.crbegin() ; it2 != operators.crend(); ++it2) {
			current_root = operatorToVisualizerJSON( current_root, *it2, isRootOp, is_debug );
			if( isRootOp ) { isRootOp = false; }
		}
		// source
		current_root = operatorToVisualizerJSON( current_root, pipeline->GetSource(), isRootOp, is_debug );
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
		(*content)["Actual Loops"] = op->GetLoopCount(); // meaningless
	}
	// output shcma
	
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