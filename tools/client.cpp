#include <boost/timer/timer.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include "nlohmann/json.hpp"	// TODO remove json and use that of boost
#include "common/logger.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/cypher_pipeline_executor.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "storage/statistics/histogram_generator.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "planner/planner.hpp"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "catalog/catalog_wrapper.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include <readline/readline.h>
#include <readline/history.h>
#include <readline/rlstdc.h>
#include <readline/rltypedefs.h>

using namespace antlr4;
using namespace gpopt;
using namespace duckdb;
using json = nlohmann::json;
namespace po = boost::program_options;

struct ClientOptions {
	string workspace;
	string query;
	string output_file;
	bool slient = false;
	bool dump_output = false;
	bool compile_only = false;
	bool warmup = false;
	bool enable_profile = false;
	int iterations = 1;
	s62::PlannerConfig planner_config;
};

enum class ProcessQueryMode { SINGLE_QUERY, INTERACTIVE };

ProcessQueryMode getQueryMode(const ClientOptions& options) {
    return options.query.empty() ? ProcessQueryMode::INTERACTIVE : ProcessQueryMode::SINGLE_QUERY;
}

void ParseConfig(int argc, char** argv, ClientOptions& options) {
    po::options_description general_options("General Options");
    general_options.add_options()
        ("help,h", "Show help message")
        ("workspace", po::value<std::string>(), "Set workspace path")
        ("log-level", po::value<std::string>(), "Set logging level (trace/debug/info/warn/error)")
		("slient", "Disable output to console")
		("output-file", po::value<std::string>(), "Output file path");

	po::options_description compiler_options("Compiler Options");
	compiler_options.add_options()
		("compile-only", "Compile the query without execution")
		("orca-compile-only", "Compile using ORCA without execution")
		("index-join-only", "Enable only index join")
		("hash-join-only", "Enable only hash join")
		("merge-join-only", "Enable only merge join")
		("disable-merge-join", "Disable merge join optimization")
		("join-order-optimizer", po::value<std::string>(), "Set join order optimizer (query/greedy/exhaustive/exhaustive2)")
		("debug-orca", "Enable ORCA debug prints");

    po::options_description query_options("Execution Options");
    query_options.add_options()
        ("query", po::value<std::string>(), "Execute a query string")
        ("iterations", po::value<int>()->default_value(1), "Number of iterations for a query")
		("warmup", "Perform a warmup query execution")
		("profile", "Enable profiling")
        ("explain", "Print the query execution plan");

    po::options_description all_options;
    all_options.add(general_options).add(compiler_options).add(query_options);

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, all_options), vm);
    po::notify(vm);

    std::unordered_map<std::string, std::function<void()>> option_handlers = {
		{"workspace", [&]() { 
			options.workspace = vm["workspace"].as<std::string>(); 
			spdlog::trace("\nWorkspace: {}\n", options.workspace); 
		}},
		{"slient", [&]() {
			options.slient = true;
		}},
		{"output-file", [&]() {
			options.output_file = vm["output-file"].as<std::string>();
		}},
        {"compile-only", [&]() { 
			options.compile_only = true; 
		}},
        {"orca-compile-only", [&]() { 
			options.compile_only = true; 
			options.planner_config.ORCA_COMPILE_ONLY = true; 
		}},
        {"index-join-only", [&]() { 
			options.planner_config.INDEX_JOIN_ONLY = true; 
		}},
        {"hash-join-only", [&]() { 
			options.planner_config.HASH_JOIN_ONLY = true; 
		}},
        {"merge-join-only", [&]() { 
			options.planner_config.MERGE_JOIN_ONLY = true; 
		}},
        {"disable-merge-join", [&]() { 
			options.planner_config.DISABLE_MERGE_JOIN = true; 
		}},
		{"join-order-optimizer", [&]() {
			std::string optimizer = vm["join-order-optimizer"].as<std::string>();
            if (optimizer == "query") options.planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_IN_QUERY;
            else if (optimizer == "greedy") options.planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_GREEDY_SEARCH;
            else if (optimizer == "exhaustive") options.planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH;
            else if (optimizer == "exhaustive2") options.planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE2_SEARCH;
            else throw std::invalid_argument("Invalid --join-order-optimizer parameter");
		}},
        {"debug-orca", [&]() { 
			options.planner_config.ORCA_DEBUG_PRINT = true; 
		}},
		{"query", [&]() { 
			options.query = vm["query"].as<std::string>(); 
		}},
		{"iterations", [&]() {
			options.iterations = vm["iterations"].as<int>();
		}},
        {"warmup", [&]() { 
			options.warmup = true; 
		}},
        {"profile", [&]() { 
			options.enable_profile = true; 
		}},
		{"explain", [&]() { 
			options.planner_config.DEBUG_PRINT = true; 
		}}
    };

    for (const auto& [key, handler] : option_handlers) {
        if (vm.count(key)) handler();
    }

    if (vm.count("log-level")) {
        LogLevel level = getLogLevel(vm["log-level"].as<std::string>());
        setLogLevel(level);
    }
}

std::string GetQueryString(std::string &prompt) {
	std::string full_input;
    std::string current_input;

	std::string shell_prompt = prompt + " >> ";
	std::string prev_prompt = prompt + " -> ";

    while (true) {
        char* line = readline(full_input.empty() ? shell_prompt.c_str() : prev_prompt.c_str());
        if (!line) {
            break;
        }

        current_input = line;
        free(line);

        if (current_input.empty()) continue;

        size_t semi_pos = current_input.find(';');
        if (semi_pos != std::string::npos) {
            full_input += current_input.substr(0, semi_pos + 1);
            break;
        }

        full_input += current_input + " ";
    }

	return full_input;
}

void CompileQuery(const string& query, std::shared_ptr<ClientContext> client, s62::Planner& planner, double &compile_elapsed_time) {
	SCOPED_TIMER(CompileQuery, spdlog::level::info, spdlog::level::debug, compile_elapsed_time);
	auto inputStream = ANTLRInputStream(query);

	SUBTIMER_START(CompileQuery, "Lexing");
	auto cypherLexer = CypherLexer(&inputStream);
	auto tokens = CommonTokenStream(&cypherLexer);
	tokens.fill();
	SUBTIMER_STOP(CompileQuery, "Lexing");
	
	SUBTIMER_START(CompileQuery, "Parse");
	auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);
	SUBTIMER_STOP(CompileQuery, "Parse");

	SUBTIMER_START(CompileQuery, "Transform");
	kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
	auto statement = transformer.transform();
	SUBTIMER_STOP(CompileQuery, "Transform");
	
	SUBTIMER_START(CompileQuery, "Bind");
    kuzu::binder::Binder binder(client.get());
	auto boundStatement = binder.bind(*statement);
	kuzu::binder::BoundStatement *bst = boundStatement.get();
	SUBTIMER_STOP(CompileQuery, "Bind");

	SUBTIMER_START(CompileQuery, "Orca Compile");
	planner.execute(bst);
	SUBTIMER_STOP(CompileQuery, "Orca Compile");
}

void ExecuteQuery(const string& query, std::shared_ptr<ClientContext> client, ClientOptions& options, vector<duckdb::CypherPipelineExecutor *>& executors, double &exec_elapsed_time) {	
	if (executors.size() == 0) {
		spdlog::error("[ExecuteQuery] Plan Empty");
		return; 
	}

	auto &profiler = QueryProfiler::Get(*client.get());
	profiler.StartQuery(query, options.enable_profile);	
	profiler.Initialize(executors[executors.size()-1]->pipeline->GetSink());
	
	{
		SCOPED_TIMER(ExecuteQuery, spdlog::level::info, spdlog::level::info, exec_elapsed_time);
		for (int i = 0; i < executors.size(); i++) {
			auto exec = executors[i];
            spdlog::info("[ExecuteQuery] Pipeline {:02d} Plan\n{}", i, exec->pipeline->toString());
            spdlog::debug("[ExecuteQuery] Pipeline ID {:02d} Started", i);
			SUBTIMER_START(ExecuteQuery, "Pipeline " + std::to_string(i));
			exec->ExecutePipeline();
			SUBTIMER_STOP(ExecuteQuery, "Pipeline " + std::to_string(i));
            spdlog::debug("[ExecuteQuery] Pipeline ID {:02d} Finished", i);
		}
	}

	profiler.EndQuery();
}

void PrintOutputToFile(PropertyKeys col_names, 
                            std::vector<std::unique_ptr<duckdb::DataChunk>> *query_results_ptr, 
							duckdb::Schema &schema,
                            std::string &file_path) {
	if (!query_results_ptr) {
        spdlog::error("[PrintOutputToFile] Query Results Empty");
        return;
    }

    if (col_names.empty()) {
        col_names = schema.getStoredColumnNames();
    }

    auto &query_results = *query_results_ptr;

    std::ofstream dump_file(file_path, std::ios::trunc);
    if (!dump_file) {
        spdlog::error("[PrintOutputToFile] Failed to open dump file: {}", file_path);
        return;
    }

	spdlog::info("[PrintOutputToFile] Dumping Output File. Path: {}", file_path);

    for (size_t i = 0; i < col_names.size(); i++) {
        dump_file << col_names[i] << (i != col_names.size() - 1 ? "|" : "\n");
    }

    for (const auto &chunk : query_results) {
        size_t num_cols = chunk->ColumnCount();
        for (size_t idx = 0; idx < chunk->size(); idx++) {
            for (size_t i = 0; i < num_cols; i++) {
                dump_file << chunk->GetValue(i, idx).ToString();
                if (i != num_cols - 1) dump_file << "|";
            }
            dump_file << "\n";
        }
    }

	spdlog::info("[PrintOutputToFile] Dump Done");
}

void PrintOutputConsole(const PropertyKeys &col_names, 
                 std::vector<std::unique_ptr<duckdb::DataChunk>> *query_results_ptr, 
                 duckdb::Schema &schema) {
    if (!query_results_ptr) {
        spdlog::error("[PrintOutputConsole] Query Results Empty");
        return;
    }

    auto final_col_names = col_names.empty() ? schema.getStoredColumnNames() : col_names;
    auto &query_results = *query_results_ptr;
    
    OutputUtil::PrintQueryOutput(final_col_names, query_results);
}

void CompileAndExecuteIteration(const std::string &query_str,
                                std::shared_ptr<ClientContext> client,
                                ClientOptions &options, s62::Planner &planner,
                                std::vector<double> &compile_times,
                                std::vector<double> &execution_times)
{
    double compile_time = 0.0, exec_time = 0.0;

    CompileQuery(query_str, client, planner, compile_time);

    if (!options.compile_only) {
        auto executors = planner.genPipelineExecutors();
        ExecuteQuery(query_str, client, options, executors, exec_time);

        auto &query_results = executors.back()->context->query_results;
        auto &schema = executors.back()->pipeline->GetSink()->schema;
        auto col_names = planner.getQueryOutputColNames();

        if (!options.slient) {
            PrintOutputConsole(col_names, query_results, schema);
        }

        if (!options.output_file.empty()) {
        	PrintOutputToFile(col_names, query_results, schema, options.output_file);
        }
    }

    compile_times.push_back(compile_time);
    execution_times.push_back(exec_time);
}

double CalculateAverageTime(const std::vector<double> &times)
{
    return times.empty() ? 0.0
                         : std::accumulate(times.begin(), times.end(), 0.0) /
                               times.size();
}

void CompileExecuteQuery(const std::string &query_str,
                         std::shared_ptr<ClientContext> client,
                         ClientOptions &options, s62::Planner &planner)
{
    std::vector<double> compile_times, execution_times;
    
    if (options.warmup) options.iterations += 1;
    for (int i = 0; i < options.iterations; ++i) {
        spdlog::info("[CompileExecuteQuery] Iteration {} Started", i + 1);
        CompileAndExecuteIteration(query_str, client, options, planner, 
                                    compile_times, execution_times);
        spdlog::info("[CompileExecuteQuery] Iteration {} Finished", i + 1);
    }

    if (options.warmup && !execution_times.empty()) {
        execution_times.erase(execution_times.begin());
    }
    if (options.warmup && !compile_times.empty()) {
        compile_times.erase(compile_times.begin());
    }

	spdlog::info("Average Query Execution Time: {} ms", CalculateAverageTime(execution_times));
	spdlog::info("Average Compile Time: {} ms", CalculateAverageTime(compile_times));
}

void InitializeDiskAIO(string& workspace) {
	DiskAioParameters::NUM_THREADS = 32;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 32;
	DiskAioParameters::NUM_CPU_SOCKETS = 2;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = workspace;

	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
}

void InitializeClient(std::shared_ptr<ClientContext>& client, ClientOptions& options) {
    duckdb::SetClientWrapper(client, std::make_shared<CatalogWrapper>(client->db->GetCatalogWrapper()));
    options.enable_profile ? client->EnableProfiling() : client->DisableProfiling();
}

void ProcessQuery(const std::string& query, std::shared_ptr<ClientContext>& client, ClientOptions& options, s62::Planner& planner) {
    if (query == "analyze") {
        HistogramGenerator hist_gen;
        hist_gen.CreateHistogram(client);
    } else if (query == "flush_file_meta") {
        ChunkCacheManager::ccm->FlushMetaInfo(options.workspace.c_str());
    } else {
        CompileExecuteQuery(query, client, options, planner);
    }
}

void ProcessQueryInteractive(std::shared_ptr<ClientContext>& client, ClientOptions& options, s62::Planner& planner) {
	std::string shell_prompt = "S62";
	std::string prev_query_str;
	std::string query_str;

	while (true) {
		query_str = GetQueryString(shell_prompt);

		if (query_str == ":exit") break;

		if (query_str != prev_query_str) {
			add_history(query_str.c_str());
			write_history((options.workspace + "/.history").c_str());
			prev_query_str = query_str;
		}

		try {
			query_str.pop_back();
			ProcessQuery(query_str, client, options, planner);
		} catch (const duckdb::Exception& e) {
			spdlog::error("DuckDB Exception: {}", e.what());
		} catch (const std::exception& e) {
			spdlog::error("Unexpected Exception: {} (Type: {})", e.what(), typeid(e).name());
		}
	}
}

int main(int argc, char** argv) {
    SetupLogger();

	ClientOptions options;
    options.planner_config = s62::PlannerConfig();
	ParseConfig(argc, argv, options);

    using_history();
    read_history((options.workspace + "/.history").c_str());

    InitializeDiskAIO(options.workspace);

    ChunkCacheManager::ccm = new ChunkCacheManager(options.workspace.c_str());
    auto database = std::make_unique<DuckDB>(options.workspace.c_str());
    auto client = std::make_shared<ClientContext>(database->instance->shared_from_this());
    InitializeClient(client, options);
	
    s62::Planner planner(options.planner_config, s62::MDProviderType::TBGPP, client.get());

    switch (getQueryMode(options)) {
        case ProcessQueryMode::SINGLE_QUERY:
            ProcessQuery(options.query, client, options, planner);
            break;
        case ProcessQueryMode::INTERACTIVE:
            ProcessQueryInteractive(client, options, planner);
            break;
    }

  	delete ChunkCacheManager::ccm;
	return 0;
}