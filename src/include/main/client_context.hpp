//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

//#include "catalog/catalog_entry/schema_catalog_entry.hpp"s
//#include "catalog/catalog_set.hpp"
//#include "common/enums/pending_execution_result.hpp"
//#include "common/deque.hpp"
#include "common/pair.hpp"
//#include "common/progress_bar.hpp"
#include "common/unordered_set.hpp"
#include "common/winapi.hpp"
//#include "main/prepared_statement.hpp"
//#include "main/stream_query_result.hpp"
//#include "main/table_description.hpp"
//#include "transaction/transaction_context.hpp"
//#include "main/pending_query_result.hpp"
#include "common/atomic.hpp"
#include "main/client_config.hpp"
//#include "main/external_dependencies.hpp"

#include "common/mutex.hpp"
#include "common/types/value.hpp"
#include "common/boost_typedefs.hpp"

#include "storage/graph_storage_wrapper.hpp"
#include "parallel/executor.hpp"


namespace duckdb {
class Appender;
class Catalog;
class CatalogSearchPath;
class ChunkCollection;
class DatabaseInstance;
class FileOpener;
class LogicalOperator;
class PreparedStatementData;
class BufferedFileWriter;
class QueryProfiler;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;
struct ActiveQueryContext;
struct ParserOptions;
struct ClientData;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext : public std::enable_shared_from_this<ClientContext> {
	//friend class PendingQueryResult;
	//friend class StreamQueryResult;
	//friend class TransactionManager;

public:
	DUCKDB_API explicit ClientContext(shared_ptr<DatabaseInstance> db);
	DUCKDB_API ~ClientContext();

	//! The database that this client is connected to
	shared_ptr<DatabaseInstance> db;

	//! A graph store API that execution engine connects to
	unique_ptr<iTbgppGraphStorageWrapper> graph_storage_wrapper;

	//! The set of client-specific data
	unique_ptr<ClientData> client_data;

	//! Data for the currently running transaction
	//TransactionContext transaction;
	//! Whether or not the query is interrupted
	//atomic<bool> interrupted;
	//! External Objects (e.g., Python objects) that views depend of
	//unordered_map<string, vector<shared_ptr<ExternalDependency>>> external_dependencies;
	//! The client configuration
	ClientConfig config;

	//! executor assigned for this class;
	unique_ptr<Executor> executor;

	// Catalog SHM (is this place appropriate?)
	fixed_managed_mapped_file *catalog_shm;

	//! Query profiler
	shared_ptr<QueryProfiler> profiler;

private:
	//! Lock on using the ClientContext in parallel
	mutex context_lock;
	//! The currently active query context
	//unique_ptr<ActiveQueryContext> active_query;
	//! The current query progress
	//atomic<double> query_progress;

public:
	/*DUCKDB_API Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	DUCKDB_API void Interrupt();
	*/

	unique_ptr<ClientContextLock> LockContext();

	//! Returns the current executor
	Executor &GetExecutor();

	//! Returns the catalog shm
	fixed_managed_mapped_file *GetCatalogSHM();

	//! Enable query profiling
	DUCKDB_API void EnableProfiling();
	//! Disable query profiling
	DUCKDB_API void DisableProfiling();

	//! get id for new physical operator
	idx_t GetNewPhyiscalOpId();


	/*
	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	// DUCKDB_API unique_ptr<QueryResult> Query(const string &query, bool allow_stream_result);
	// DUCKDB_API unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement, bool allow_stream_result);

	// //! Issues a query to the database and returns a Pending Query Result. Note that "query" may only contain
	// //! a single statement.
	// DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(const string &query);
	// //! Issues a query to the database and returns a Pending Query Result
	// DUCKDB_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement);

	// //! Destroy the client context
	// DUCKDB_API void Destroy();

	// //! Get the table info of a specific table, or nullptr if it cannot be found
	// DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
	// //! Appends a DataChunk to the specified table. Returns whether or not the append was successful.
	// DUCKDB_API void Append(TableDescription &description, ChunkCollection &collection);
	// //! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	// //! list with the set of returned columns
	// DUCKDB_API void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	// //! Execute a relation
	// DUCKDB_API unique_ptr<QueryResult> Execute(const shared_ptr<Relation> &relation);

	// //! Prepare a query
	// DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
	// //! Directly prepare a SQL statement
	// DUCKDB_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	// //! Create a pending query result from a prepared statement with the given name and set of parameters
	// //! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	// //! modified in between the prepared statement being bound and the prepared statement being run.
	// DUCKDB_API unique_ptr<PendingQueryResult>
	// PendingQuery(const string &query, shared_ptr<PreparedStatementData> &prepared, vector<Value> &values);

	// //! Execute a prepared statement with the given name and set of parameters
	// //! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	// //! modified in between the prepared statement being bound and the prepared statement being run.
	// DUCKDB_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	//                                            vector<Value> &values, bool allow_stream_result = true);

	// //! Gets current percentage of the query's progress, returns 0 in case the progress bar is disabled.
	// DUCKDB_API double GetProgress();

	// //! Register function in the temporary schema
	// DUCKDB_API void RegisterFunction(CreateFunctionInfo *info);

	// //! Parse statements from a query
	// DUCKDB_API vector<unique_ptr<SQLStatement>> ParseStatements(const string &query);

	// //! Extract the logical plan of a query
	// DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);
	// DUCKDB_API void HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements);

	// //! Runs a function with a valid transaction context, potentially starting a transaction if the context is in auto
	// //! commit mode.
	// DUCKDB_API void RunFunctionInTransaction(const std::function<void(void)> &fun,
	//                                          bool requires_valid_transaction = true);
	// //! Same as RunFunctionInTransaction, but does not obtain a lock on the client context or check for validation
	// DUCKDB_API void RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
	//                                                  bool requires_valid_transaction = true);

	// //! Equivalent to CURRENT_SETTING(key) SQL function.
	// DUCKDB_API bool TryGetCurrentSetting(const std::string &key, Value &result);

	// //! Returns the parser options for this client context
	// DUCKDB_API ParserOptions GetParserOptions();

	// DUCKDB_API unique_ptr<DataChunk> Fetch(ClientContextLock &lock, StreamQueryResult &result);

	// //! Whether or not the given result object (streaming query result or pending query result) is active
	// DUCKDB_API bool IsActiveResult(ClientContextLock &lock, BaseQueryResult *result);

	// //! Returns the current query string (if any)
	// const string &GetCurrentQuery();

	// //! Fetch a list of table names that are required for a given query
	// DUCKDB_API unordered_set<string> GetTableNames(const string &query);


private:
	//! Parse statements and resolve pragmas from a query
	/*bool ParseStatements(ClientContextLock &lock, const string &query, vector<unique_ptr<SQLStatement>> &result,
	                     string &error);
	//! Issues a query to the database and returns a Pending Query Result
	unique_ptr<PendingQueryResult> PendingQueryInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement,
	                                                    bool verify = true);
	unique_ptr<QueryResult> ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query,
	                                                    bool allow_stream_result);

	//! Parse statements from a query
	vector<unique_ptr<SQLStatement>> ParseStatementsInternal(ClientContextLock &lock, const string &query);
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	string VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement);

	void InitialCleanup(ClientContextLock &lock);
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal(ClientContextLock &lock, BaseQueryResult *result = nullptr,
	                     bool invalidate_transaction = false);
	string FinalizeQuery(ClientContextLock &lock, bool success);
	unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
	                                                                   unique_ptr<SQLStatement> statement,
	                                                                   shared_ptr<PreparedStatementData> &prepared,
	                                                                   vector<Value> *values);
	unique_ptr<PendingQueryResult> PendingPreparedStatement(ClientContextLock &lock,
	                                                        shared_ptr<PreparedStatementData> statement_p,
	                                                        vector<Value> bound_values);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	shared_ptr<PreparedStatementData> CreatePreparedStatement(ClientContextLock &lock, const string &query,
	                                                          unique_ptr<SQLStatement> statement,
	                                                          vector<Value> *values = nullptr);
	unique_ptr<PendingQueryResult> PendingStatementInternal(ClientContextLock &lock, const string &query,
	                                                        unique_ptr<SQLStatement> statement);
	unique_ptr<QueryResult> RunStatementInternal(ClientContextLock &lock, const string &query,
	                                             unique_ptr<SQLStatement> statement, bool allow_stream_result,
	                                             bool verify = true);
	unique_ptr<PreparedStatement> PrepareInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement);
	void LogQueryInternal(ClientContextLock &lock, const string &query);

	unique_ptr<QueryResult> FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending,
	                                            bool allow_stream_result);
	unique_ptr<DataChunk> FetchInternal(ClientContextLock &lock, Executor &executor, BaseQueryResult &result);
	*/
	
	/*
	bool UpdateFunctionInfoFromEntry(ScalarFunctionCatalogEntry *existing_function, CreateScalarFunctionInfo *new_info);

	void BeginTransactionInternal(ClientContextLock &lock, bool requires_valid_transaction);
	void BeginQueryInternal(ClientContextLock &lock, const string &query);
	string EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction);

	PendingExecutionResult ExecuteTaskInternal(ClientContextLock &lock, PendingQueryResult &result);

	unique_ptr<PendingQueryResult>
	PendingStatementOrPreparedStatementInternal(ClientContextLock &lock, const string &query,
	                                            unique_ptr<SQLStatement> statement,
	                                            shared_ptr<PreparedStatementData> &prepared, vector<Value> *values);

	unique_ptr<PendingQueryResult> PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
	                                                            shared_ptr<PreparedStatementData> &prepared,
	                                                            vector<Value> &values);*/

};

class ClientContextLock {
public:
	explicit ClientContextLock(mutex &context_lock) : client_guard(context_lock) {
	}

	~ClientContextLock() {
	}

private:
	lock_guard<mutex> client_guard;
};

class ClientContextWrapper {
public:
	DUCKDB_API explicit ClientContextWrapper(const shared_ptr<ClientContext> &context)
	    : client_context(context) {

	      };
	shared_ptr<ClientContext> GetContext() {
		auto actual_context = client_context.lock();
		if (!actual_context) {
			throw std::runtime_error("This connection is closed");
		}
		return actual_context;
	}

private:
	std::weak_ptr<ClientContext> client_context;
};

} // namespace duckdb
