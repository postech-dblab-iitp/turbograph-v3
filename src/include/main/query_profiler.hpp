//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
// #include "common/enums/profiler_format.hpp"
#include "common/profiler.hpp"
#include "common/string_util.hpp"
#include "common/types/data_chunk.hpp"
#include "common/unordered_map.hpp"
#include "common/winapi.hpp"
//#include "execution/physical_operator.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/expression_executor_state.hpp"
#include <stack>
#include "common/pair.hpp"
#include "common/deque.hpp"

namespace duckdb {
class ClientContext;
class ExpressionExecutor;
class CypherPhysicalOperator;
class SQLStatement;

//! The ExpressionInfo keeps information related to an expression
struct ExpressionInfo {
	explicit ExpressionInfo() : hasfunction(false) {
	}
	// A vector of children
	vector<unique_ptr<ExpressionInfo>> children;
	// Extract ExpressionInformation from a given expression state
	void ExtractExpressionsRecursive(unique_ptr<ExpressionState> &state);

	//! Whether or not expression has function
	bool hasfunction;
	//! The function Name
	string function_name;
	//! The function time
	uint64_t function_time;
	//! Count the number of ALL tuples
	uint64_t tuples_count = 0;
	//! Count the number of tuples sampled
	uint64_t sample_tuples_count = 0;
};

//! The ExpressionRootInfo keeps information related to the root of an expression tree
struct ExpressionRootInfo {
	ExpressionRootInfo(ExpressionExecutorState &executor, string name);

	//! Count the number of time the executor called
	uint64_t total_count = 0;
	//! Count the number of time the executor called since last sampling
	uint64_t current_count = 0;
	//! Count the number of samples
	uint64_t sample_count = 0;
	//! Count the number of tuples in all samples
	uint64_t sample_tuples_count = 0;
	//! Count the number of tuples processed by this executor
	uint64_t tuples_count = 0;
	//! A vector which contain the pointer to root of each expression tree
	unique_ptr<ExpressionInfo> root;
	//! Name
	string name;
	//! Elapsed time
	double time;
	//! Extra Info
	string extra_info;
};

struct ExpressionExecutorInfo {
	explicit ExpressionExecutorInfo() {};
	explicit ExpressionExecutorInfo(ExpressionExecutor &executor, const string &name, int id);

	//! A vector which contain the pointer to all ExpressionRootInfo
	vector<unique_ptr<ExpressionRootInfo>> roots;
	//! Id, it will be used as index for executors_info vector
	int id;
};

struct OperatorInformation {
	explicit OperatorInformation(double time_ = 0, idx_t elements_ = 0) : time(time_), elements(elements_) {
	}

	double time = 0;
	idx_t elements = 0;
	string name;
	//! A vector of Expression Executor Info
	vector<unique_ptr<ExpressionExecutorInfo>> executors_info;
};

//! The OperatorProfiler measures timings of individual operators
class OperatorProfiler {
	friend class QueryProfiler;

public:
	DUCKDB_API explicit OperatorProfiler(bool enabled);

	DUCKDB_API void StartOperator(const CypherPhysicalOperator *phys_op);
	DUCKDB_API void EndOperator(DataChunk *chunk);
	DUCKDB_API void EndOperator(vector<shared_ptr<DataChunk>> &chunks);
	DUCKDB_API void Flush(const CypherPhysicalOperator *phys_op, ExpressionExecutor *expression_executor, const string &name,
	                      int id);

	~OperatorProfiler() {
	}

private:
	void AddTiming(const CypherPhysicalOperator *op, double time, idx_t elements);

	//! Whether or not the profiler is enabled
	bool enabled;
	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	const CypherPhysicalOperator *active_operator;
	//! A mapping of physical operators to recorded timings
	unordered_map<const CypherPhysicalOperator *, OperatorInformation> timings;
};

//! The QueryProfiler can be used to measure timings of queries
class QueryProfiler {
public:
	DUCKDB_API QueryProfiler(ClientContext &context);

public:
	struct TreeNode {
		PhysicalOperatorType type;
		string name;
		string extra_info;
		OperatorInformation info;
		vector<unique_ptr<TreeNode>> children;
		idx_t depth = 0;
	};

	// Propagate save_location, enabled, detailed_enabled and automatic_print_format.
	void Propagate(QueryProfiler &qp);

	using TreeMap = unordered_map<const CypherPhysicalOperator *, TreeNode *>;

private:
	unique_ptr<TreeNode> CreateTree(CypherPhysicalOperator *root, idx_t depth = 0);
	void Render(const TreeNode &node, std::ostream &str) const;

public:
	DUCKDB_API bool IsEnabled() const;
	DUCKDB_API bool IsDetailedEnabled() const;
	// DUCKDB_API ProfilerPrintFormat GetPrintFormat() const;
	// DUCKDB_API string GetSaveLocation() const;

	DUCKDB_API static QueryProfiler &Get(ClientContext &context);

	DUCKDB_API void StartQuery(string query, bool is_explain_analyze = false);
	DUCKDB_API void EndQuery();

	DUCKDB_API void StartExplainAnalyze();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);

	DUCKDB_API void StartPhase(string phase);
	DUCKDB_API void EndPhase();

	DUCKDB_API void Initialize(CypherPhysicalOperator *root);

	DUCKDB_API string ToString(bool print_optimizer_output = false) const;
	DUCKDB_API void ToStream(std::ostream &str, bool print_optimizer_output = false) const;
	DUCKDB_API void Print();

	// DUCKDB_API string ToJSON() const;
	// DUCKDB_API void WriteToFile(const char *path, string &info) const;

	idx_t OperatorSize() {
		return tree_map.size();
	}

	void Finalize(TreeNode &node);

private:
	ClientContext &context;

	//! Whether or not the query profiler is running
	bool running;
	//! The lock used for flushing information from a thread into the global query profiler
	mutex flush_lock;

	//! Whether or not the query requires profiling
	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<TreeNode> root;
	//! The query string
	string query;
	//! The timer used to time the execution time of the entire query
	Profiler main_query;
	//! A map of a Physical Operator pointer to a tree node
	TreeMap tree_map;
	//! Whether or not we are running as part of a explain_analyze query
	bool is_explain_analyze;

public:
	const TreeMap &GetTreeMap() const {
		return tree_map;
	}

private:
	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	using PhaseTimingStorage = unordered_map<string, double>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<string> phase_stack;

private:
	vector<PhaseTimingItem> GetOrderedPhaseTimings() const;

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(PhysicalOperatorType op_type);
};

//! The QueryProfilerHistory can be used to access the profiler of previous queries
class QueryProfilerHistory {
private:
	//! Previous Query profilers
	deque<pair<transaction_t, shared_ptr<QueryProfiler>>> prev_profilers;
	//! Previous Query profilers size
	uint64_t prev_profilers_size = 20;

public:
	deque<pair<transaction_t, shared_ptr<QueryProfiler>>> &GetPrevProfilers() {
		return prev_profilers;
	}
	QueryProfilerHistory() {
	}

	void SetPrevProfilersSize(uint64_t prevProfilersSize) {
		prev_profilers_size = prevProfilersSize;
	}
	uint64_t GetPrevProfilersSize() const {
		return prev_profilers_size;
	}

public:
	void SetProfilerHistorySize(uint64_t size) {
		this->prev_profilers_size = size;
	}
};
} // namespace duckdb
