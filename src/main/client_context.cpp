#include "main/client_context.hpp"

//#include "main/client_context_file_opener.hpp"
//#include "main/query_profiler.hpp"
//#include "catalog/catalog_entry/table_catalog_entry.hpp"
//#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
//#include "catalog/catalog_search_path.hpp"
//#include "common/serializer/buffered_deserializer.hpp"
//#include "common/serializer/buffered_serializer.hpp"
//#include "execution/physical_plan_generator.hpp"
//#include "main/materialized_query_result.hpp"
//#include "main/query_result.hpp"
//#include "main/stream_query_result.hpp"
//#include "optimizer/optimizer.hpp"
//#include "parser/parser.hpp"
//#include "parser/expression/constant_expression.hpp"
//#include "parser/parsed_data/create_function_info.hpp"
//#include "parser/statement/drop_statement.hpp"
//#include "parser/statement/explain_statement.hpp"
//#include "parser/statement/select_statement.hpp"
//#include "planner/operator/logical_execute.hpp"
//#include "transaction/transaction_manager.hpp"
//#include "transaction/transaction.hpp"
//#include "storage/data_table.hpp"
//#include "main/appender.hpp"
//#include "main/relation.hpp"
//#include "parser/statement/relation_statement.hpp"
//#include "parallel/task_scheduler.hpp"
//#include "common/serializer/buffered_file_writer.hpp"
//#include "planner/pragma_handler.hpp"
//#include "common/file_system.hpp"
//#include "execution/column_binding_resolver.hpp"

#include "main/database.hpp"
#include "main/client_data.hpp"
#include "main/query_profiler.hpp"

#include "common/types/value.hpp"

#include <memory>
using namespace std;

namespace duckdb {

/*struct ActiveQueryContext {
	//! The query that is currently being executed
	string query;
	//! The currently open result
	BaseQueryResult *open_result = nullptr;
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;
};*/

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(move(database)), //transaction(db->GetTransactionManager(), *this), interrupted(false),
      client_data(make_unique<ClientData>(*this)),
	  graph_storage_wrapper(make_unique<iTbgppGraphStorageWrapper>(*this)),
	  executor(make_unique<Executor>(*this)),
	  profiler(make_shared<QueryProfiler>(*this)) {
	this->catalog_shm = db->GetCatalogSHM();
}

ClientContext::~ClientContext() {
	if (Exception::UncaughtException()) {
		return;
	}
	// destroy the client context and rollback if there is an active transaction
	// but only if we are not destroying this client context as part of an exception stack unwind
	//Destroy();
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	return make_unique<ClientContextLock>(context_lock);
}

Executor &ClientContext::GetExecutor() {
	
}

fixed_managed_mapped_file *ClientContext::GetCatalogSHM() {
	return catalog_shm;
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = true;
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = false;
}

idx_t ClientContext::GetNewPhyiscalOpId() {
	auto lock = LockContext();
	return ClientData::Get(*this).physical_op_counter++;
}


/*
void ClientContext::Destroy() {
	auto lock = LockContext();
	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback();
		}
	}
	CleanupInternal(*lock);
}

unique_ptr<DataChunk> ClientContext::Fetch(ClientContextLock &lock, StreamQueryResult &result) {
	D_ASSERT(IsActiveResult(lock, &result));
	D_ASSERT(active_query->executor);
	return FetchInternal(lock, *active_query->executor, result);
}

unique_ptr<DataChunk> ClientContext::FetchInternal(ClientContextLock &lock, Executor &executor,
                                                   BaseQueryResult &result) {
	bool invalidate_query = true;
	try {
		// fetch the chunk and return it
		auto chunk = executor.FetchChunk();
		if (!chunk || chunk->size() == 0) {
			CleanupInternal(lock, &result);
		}
		return chunk;
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result.error = ex.what();
		invalidate_query = false;
	} catch (std::exception &ex) {
		result.error = ex.what();
	} catch (...) { // LCOV_EXCL_START
		result.error = "Unhandled exception in FetchInternal";
	} // LCOV_EXCL_STOP
	result.success = false;
	CleanupInternal(lock, &result, invalidate_query);
	return nullptr;
}

void ClientContext::BeginTransactionInternal(ClientContextLock &lock, bool requires_valid_transaction) {
	// check if we are on AutoCommit. In this case we should start a transaction
	D_ASSERT(!active_query);
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    transaction.ActiveTransaction().IsInvalidated()) {
		throw Exception("Failed: transaction has been invalidated!");
	}
	active_query = make_unique<ActiveQueryContext>();
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
}

void ClientContext::BeginQueryInternal(ClientContextLock &lock, const string &query) {
	BeginTransactionInternal(lock, false);
	LogQueryInternal(lock, query);
	active_query->query = query;
	query_progress = -1;
	ActiveTransaction().active_query = db->GetTransactionManager().GetQueryNumber();
}

string ClientContext::EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction) {
	client_data->profiler->EndQuery();

	D_ASSERT(active_query.get());
	string error;
	try {
		if (transaction.HasActiveTransaction()) {
			// Move the query profiler into the history
			auto &prev_profilers = client_data->query_profiler_history->GetPrevProfilers();
			prev_profilers.emplace_back(transaction.ActiveTransaction().active_query, move(client_data->profiler));
			// Reinitialize the query profiler
			client_data->profiler = make_shared<QueryProfiler>(*this);
			// Propagate settings of the saved query into the new profiler.
			client_data->profiler->Propagate(*prev_profilers.back().second);
			if (prev_profilers.size() >= client_data->query_profiler_history->GetPrevProfilersSize()) {
				prev_profilers.pop_front();
			}

			ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
			if (transaction.IsAutoCommit()) {
				if (success) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}
			} else if (invalidate_transaction) {
				D_ASSERT(!success);
				ActiveTransaction().Invalidate();
			}
		}
	} catch (std::exception &ex) {
		error = ex.what();
	} catch (...) { // LCOV_EXCL_START
		error = "Unhandled exception!";
	} // LCOV_EXCL_STOP
	active_query.reset();
	query_progress = -1;
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock, BaseQueryResult *result, bool invalidate_transaction) {
	if (!active_query) {
		// no query currently active
		return;
	}
	if (active_query->executor) {
		active_query->executor->CancelTasks();
	}
	active_query->progress_bar.reset();

	auto error = EndQueryInternal(lock, result ? result->success : false, invalidate_transaction);
	if (result && result->success) {
		// if an error occurred while committing report it in the result
		result->error = error;
		result->success = error.empty();
	}
	D_ASSERT(!active_query);
}
*/



/*
const string &ClientContext::GetCurrentQuery() {
	D_ASSERT(active_query);
	return active_query->query;
}

unique_ptr<QueryResult> ClientContext::FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending,
                                                           bool allow_stream_result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &pending);
	D_ASSERT(active_query->prepared);
	auto &prepared = *active_query->prepared;
	bool create_stream_result = prepared.allow_stream_result && allow_stream_result;
	if (create_stream_result) {
		active_query->progress_bar.reset();
		query_progress = -1;

		// successfully compiled SELECT clause and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		auto stream_result =
		    make_unique<StreamQueryResult>(pending.statement_type, shared_from_this(), pending.types, pending.names);
		active_query->open_result = stream_result.get();
		return move(stream_result);
	}
	// create a materialized result by continuously fetching
	auto result = make_unique<MaterializedQueryResult>(pending.statement_type, pending.types, pending.names);
	while (true) {
		auto chunk = FetchInternal(lock, GetExecutor(), *result);
		if (!chunk || chunk->size() == 0) {
			break;
		}
#ifdef DEBUG
		for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
			if (pending.types[i].id() == LogicalTypeId::VARCHAR) {
				chunk->data[i].UTFVerify(chunk->size());
			}
		}
#endif
		result->collection.Append(*chunk);
	}
	return move(result);
}

shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query,
                                                                         unique_ptr<SQLStatement> statement,
                                                                         vector<Value> *values) {
	StatementType statement_type = statement->type;
	auto result = make_shared<PreparedStatementData>(statement_type);

	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartPhase("planner");
	Planner planner(*this);
	if (values) {
		for (auto &value : *values) {
			planner.parameter_types.push_back(value.type());
		}
	}
	planner.CreatePlan(move(statement));
	D_ASSERT(planner.plan);
	profiler.EndPhase();

	auto plan = move(planner.plan);
#ifdef DEBUG
	plan->Verify();
#endif
	// extract the result column names from the plan
	result->read_only = planner.read_only;
	result->requires_valid_transaction = planner.requires_valid_transaction;
	result->allow_stream_result = planner.allow_stream_result;
	result->names = planner.names;
	result->types = planner.types;
	result->value_map = move(planner.value_map);
	result->catalog_version = Transaction::GetTransaction(*this).catalog_version;
	result->bound_all_parameters = planner.bound_all_parameters;

	if (config.enable_optimizer) {
		profiler.StartPhase("optimizer");
		Optimizer optimizer(*planner.binder, *this);
		plan = optimizer.Optimize(move(plan));
		D_ASSERT(plan);
		profiler.EndPhase();

#ifdef DEBUG
		plan->Verify();
#endif
	}

	profiler.StartPhase("physical_planner");
	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	auto physical_plan = physical_planner.CreatePlan(move(plan));
	profiler.EndPhase();

#ifdef DEBUG
	D_ASSERT(!physical_plan->ToString().empty());
#endif
	result->plan = move(physical_plan);
	return result;
}

double ClientContext::GetProgress() {
	return query_progress.load();
}

unique_ptr<PendingQueryResult> ClientContext::PendingPreparedStatement(ClientContextLock &lock,
                                                                       shared_ptr<PreparedStatementData> statement_p,
                                                                       vector<Value> bound_values) {
	D_ASSERT(active_query);
	auto &statement = *statement_p;
	if (ActiveTransaction().IsInvalidated() && statement.requires_valid_transaction) {
		throw Exception("Current transaction is aborted (please ROLLBACK)");
	}
	auto &db_config = DBConfig::GetConfig(*this);
	if (db_config.access_mode == AccessMode::READ_ONLY && !statement.read_only) {
		throw Exception(StringUtil::Format("Cannot execute statement of type \"%s\" in read-only mode!",
		                                   StatementTypeToString(statement.statement_type)));
	}

	// bind the bound values before execution
	statement.Bind(move(bound_values));

	active_query->executor = make_unique<Executor>(*this);
	auto &executor = *active_query->executor;
	if (config.enable_progress_bar) {
		active_query->progress_bar = make_unique<ProgressBar>(executor, config.wait_time);
		active_query->progress_bar->Start();
		query_progress = 0;
	}
	executor.Initialize(statement.plan.get());
	auto types = executor.GetTypes();
	D_ASSERT(types == statement.types);
	D_ASSERT(!active_query->open_result);

	auto pending_result = make_unique<PendingQueryResult>(shared_from_this(), *statement_p, move(types));
	active_query->prepared = move(statement_p);
	active_query->open_result = pending_result.get();
	return pending_result;
}

PendingExecutionResult ClientContext::ExecuteTaskInternal(ClientContextLock &lock, PendingQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &result);
	try {
		auto result = active_query->executor->ExecuteTask();
		if (active_query->progress_bar) {
			active_query->progress_bar->Update(result == PendingExecutionResult::RESULT_READY);
			query_progress = active_query->progress_bar->GetCurrentPercentage();
		}
		return result;
	} catch (std::exception &ex) {
		result.error = ex.what();
	} catch (...) { // LCOV_EXCL_START
		result.error = "Unhandled exception in ExecuteTaskInternal";
	} // LCOV_EXCL_STOP
	EndQueryInternal(lock, false, true);
	result.success = false;
	return PendingExecutionResult::EXECUTION_ERROR;
}

void ClientContext::InitialCleanup(ClientContextLock &lock) {
	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal(lock);
	interrupted = false;
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatements(const string &query) {
	auto lock = LockContext();
	return ParseStatementsInternal(*lock, query);
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal(ClientContextLock &lock, const string &query) {
	Parser parser(GetParserOptions());
	parser.ParseQuery(query);

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(lock, parser.statements);

	return move(parser.statements);
}

void ClientContext::HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements) {
	auto lock = LockContext();

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(*lock, statements);
}

unique_ptr<LogicalOperator> ClientContext::ExtractPlan(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw Exception("ExtractPlan can only prepare a single statement");
	}

	unique_ptr<LogicalOperator> plan;
	RunFunctionInTransactionInternal(*lock, [&]() {
		Planner planner(*this);
		planner.CreatePlan(move(statements[0]));
		D_ASSERT(planner.plan);

		plan = move(planner.plan);

		if (config.enable_optimizer) {
			Optimizer optimizer(*planner.binder, *this);
			plan = optimizer.Optimize(move(plan));
		}

		ColumnBindingResolver resolver;
		resolver.VisitOperator(*plan);

		plan->ResolveOperatorTypes();
	});
	return plan;
}

unique_ptr<PreparedStatement> ClientContext::PrepareInternal(ClientContextLock &lock,
                                                             unique_ptr<SQLStatement> statement) {
	auto n_param = statement->n_param;
	auto statement_query = statement->query;
	shared_ptr<PreparedStatementData> prepared_data;
	auto unbound_statement = statement->Copy();
	RunFunctionInTransactionInternal(
	    lock, [&]() { prepared_data = CreatePreparedStatement(lock, statement_query, move(statement)); }, false);
	prepared_data->unbound_statement = move(unbound_statement);
	return make_unique<PreparedStatement>(shared_from_this(), move(prepared_data), move(statement_query), n_param);
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, move(statement));
	} catch (std::exception &ex) {
		return make_unique<PreparedStatement>(ex.what());
	}
}

unique_ptr<PreparedStatement> ClientContext::Prepare(const string &query) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);

		// first parse the query
		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw Exception("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, move(statements[0]));
	} catch (std::exception &ex) {
		return make_unique<PreparedStatement>(ex.what());
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
                                                                           shared_ptr<PreparedStatementData> &prepared,
                                                                           vector<Value> &values) {
	try {
		InitialCleanup(lock);
	} catch (std::exception &ex) {
		return make_unique<PendingQueryResult>(ex.what());
	}
	return PendingStatementOrPreparedStatementInternal(lock, query, nullptr, prepared, &values);
}

unique_ptr<PendingQueryResult>
ClientContext::PendingQuery(const string &query, shared_ptr<PreparedStatementData> &prepared, vector<Value> &values) {
	auto lock = LockContext();
	return PendingQueryPreparedInternal(*lock, query, prepared, values);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               vector<Value> &values, bool allow_stream_result) {
	auto lock = LockContext();
	auto pending = PendingQueryPreparedInternal(*lock, query, prepared, values);
	if (!pending->success) {
		return make_unique<MaterializedQueryResult>(pending->error);
	}
	return pending->ExecuteInternal(*lock, allow_stream_result);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementInternal(ClientContextLock &lock, const string &query,
                                                                       unique_ptr<SQLStatement> statement) {
	// prepare the query for execution
	auto prepared = CreatePreparedStatement(lock, query, move(statement));
	// by default, no values are bound
	vector<Value> bound_values;
	// execute the prepared statement
	return PendingPreparedStatement(lock, move(prepared), move(bound_values));
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result, bool verify) {
	auto pending = PendingQueryInternal(lock, move(statement), verify);
	if (!pending->success) {
		return make_unique<MaterializedQueryResult>(move(pending->error));
	}
	return ExecutePendingQueryInternal(lock, *pending, allow_stream_result);
}

bool ClientContext::IsActiveResult(ClientContextLock &lock, BaseQueryResult *result) {
	if (!active_query) {
		return false;
	}
	return active_query->open_result == result;
}

static bool IsExplainAnalyze(SQLStatement *statement) {
	if (!statement) {
		return false;
	}
	if (statement->type != StatementType::EXPLAIN_STATEMENT) {
		return false;
	}
	auto &explain = (ExplainStatement &)*statement;
	return explain.explain_type == ExplainType::EXPLAIN_ANALYZE;
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatementInternal(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, vector<Value> *values) {
	// check if we are on AutoCommit. In this case we should start a transaction.
	if (statement && config.query_verification_enabled) {
		// query verification is enabled
		// create a copy of the statement, and use the copy
		// this way we verify that the copy correctly copies all properties
		auto copied_statement = statement->Copy();
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT: {
			// in case this is a select query, we verify the original statement
			string error;
			try {
				error = VerifyQuery(lock, query, move(statement));
			} catch (std::exception &ex) {
				error = ex.what();
			}
			if (!error.empty()) {
				// error in verifying query
				return make_unique<PendingQueryResult>(error);
			}
			statement = move(copied_statement);
			break;
		}
		case StatementType::INSERT_STATEMENT:
		case StatementType::DELETE_STATEMENT:
		case StatementType::UPDATE_STATEMENT: {
			auto sql = statement->ToString();
			Parser parser;
			parser.ParseQuery(sql);
			statement = move(parser.statements[0]);
			break;
		}
		default:
			statement = move(copied_statement);
			break;
		}
	}
	return PendingStatementOrPreparedStatement(lock, query, move(statement), prepared, values);
}

unique_ptr<PendingQueryResult>
ClientContext::PendingStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
                                                   unique_ptr<SQLStatement> statement,
                                                   shared_ptr<PreparedStatementData> &prepared, vector<Value> *values) {
	unique_ptr<PendingQueryResult> result;

	BeginQueryInternal(lock, query);
	// start the profiler
	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement ? statement.get() : prepared->unbound_statement.get()));
	bool invalidate_query = true;
	try {
		if (statement) {
			result = PendingStatementInternal(lock, query, move(statement));
		} else {
			auto &catalog = Catalog::GetCatalog(*this);
			if (prepared->unbound_statement &&
			    (catalog.GetCatalogVersion() != prepared->catalog_version || !prepared->bound_all_parameters)) {
				// catalog was modified: rebind the statement before execution
				auto new_prepared = CreatePreparedStatement(lock, query, prepared->unbound_statement->Copy(), values);
				if (prepared->types != new_prepared->types && prepared->bound_all_parameters) {
					throw BinderException("Rebinding statement after catalog change resulted in change of types");
				}
				D_ASSERT(new_prepared->bound_all_parameters);
				new_prepared->unbound_statement = move(prepared->unbound_statement);
				prepared = move(new_prepared);
				prepared->bound_all_parameters = false;
			}
			result = PendingPreparedStatement(lock, prepared, *values);
		}
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result = make_unique<PendingQueryResult>(ex.what());
		invalidate_query = false;
	} catch (std::exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_unique<PendingQueryResult>(ex.what());
	}
	if (!result->success) {
		// query failed: abort now
		EndQueryInternal(lock, false, invalidate_query);
		return result;
	}
	D_ASSERT(active_query->open_result == result.get());
	return result;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	if (!client_data->log_query_writer) {
#ifdef DUCKDB_FORCE_QUERY_LOG
		try {
			string log_path(DUCKDB_FORCE_QUERY_LOG);
			client_data->log_query_writer =
			    make_unique<BufferedFileWriter>(FileSystem::GetFileSystem(*this), log_path,
			                                    BufferedFileWriter::DEFAULT_OPEN_FLAGS, client_data->file_opener.get());
		} catch (...) {
			return;
		}
#else
		return;
#endif
	}
	// log query path is set: log the query
	client_data->log_query_writer->WriteData((const_data_ptr_t)query.c_str(), query.size());
	client_data->log_query_writer->WriteData((const_data_ptr_t) "\n", 1);
	client_data->log_query_writer->Flush();
	client_data->log_query_writer->Sync();
}

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	auto pending_query = PendingQuery(move(statement));
	return pending_query->Execute(allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	string error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_unique<MaterializedQueryResult>(move(error));
	}
	if (statements.empty()) {
		// no statements, return empty successful result
		return make_unique<MaterializedQueryResult>(StatementType::INVALID_STATEMENT);
	}

	unique_ptr<QueryResult> result;
	QueryResult *last_result = nullptr;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();
		bool stream_result = allow_stream_result && is_last_statement;
		auto pending_query = PendingQueryInternal(*lock, move(statement));
		unique_ptr<QueryResult> current_result;
		if (!pending_query->success) {
			current_result = make_unique<MaterializedQueryResult>(pending_query->error);
		} else {
			current_result = ExecutePendingQueryInternal(*lock, *pending_query, stream_result);
		}
		// now append the result to the list of results
		if (!last_result) {
			// first result of the query
			result = move(current_result);
			last_result = result.get();
		} else {
			// later results; attach to the result chain
			last_result->next = move(current_result);
			last_result = last_result->next.get();
		}
	}
	return result;
}

bool ClientContext::ParseStatements(ClientContextLock &lock, const string &query,
                                    vector<unique_ptr<SQLStatement>> &result, string &error) {
	try {
		InitialCleanup(lock);
		// parse the query and transform it into a set of statements
		result = ParseStatementsInternal(lock, query);
		return true;
	} catch (std::exception &ex) {
		error = ex.what();
		return false;
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query) {
	auto lock = LockContext();

	string error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_unique<PendingQueryResult>(move(error));
	}
	if (statements.size() != 1) {
		return make_unique<PendingQueryResult>("PendingQuery can only take a single statement");
	}
	return PendingQueryInternal(*lock, move(statements[0]));
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	return PendingQueryInternal(*lock, move(statement));
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   unique_ptr<SQLStatement> statement, bool verify) {
	auto query = statement->query;
	shared_ptr<PreparedStatementData> prepared;
	if (verify) {
		return PendingStatementOrPreparedStatementInternal(lock, query, move(statement), prepared, nullptr);
	} else {
		return PendingStatementOrPreparedStatement(lock, query, move(statement), prepared, nullptr);
	}
}

unique_ptr<QueryResult> ClientContext::ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query,
                                                                   bool allow_stream_result) {
	return query.ExecuteInternal(lock, allow_stream_result);
}

void ClientContext::Interrupt() {
	interrupted = true;
}
*/

/*
struct VerifyStatement {
	VerifyStatement(unique_ptr<SelectStatement> statement_p, string statement_name_p, bool require_equality = true)
	    : statement(move(statement_p)), statement_name(move(statement_name_p)), require_equality(require_equality),
	      select_list(statement->node->GetSelectList()) {
	}

	unique_ptr<SelectStatement> statement;
	string statement_name;
	bool require_equality;
	const vector<unique_ptr<ParsedExpression>> &select_list;
};

string ClientContext::VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// aggressive query verification

	// the purpose of this function is to test correctness of otherwise hard to test features:
	// Copy() of statements and expressions
	// Serialize()/Deserialize() of expressions
	// Hash() of expressions
	// Equality() of statements and expressions
	// ToString() of statements and expressions
	// Correctness of plans both with and without optimizers

	vector<VerifyStatement> verify_statements;

	// copy the statement
	auto select_stmt = (SelectStatement *)statement.get();
	auto copied_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());
	auto unoptimized_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());

	BufferedSerializer serializer;
	select_stmt->Serialize(serializer);
	BufferedDeserializer source(serializer);
	auto deserialized_stmt = SelectStatement::Deserialize(source);

	Parser parser;
	parser.ParseQuery(select_stmt->ToString());
	D_ASSERT(parser.statements.size() == 1);
	D_ASSERT(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	auto parsed_statement = move(parser.statements[0]);

	verify_statements.emplace_back(unique_ptr_cast<SQLStatement, SelectStatement>(move(statement)),
	                               "Original statement");
	verify_statements.emplace_back(move(copied_stmt), "Copied statement");
	verify_statements.emplace_back(move(deserialized_stmt), "Deserialized statement");
	verify_statements.emplace_back(unique_ptr_cast<SQLStatement, SelectStatement>(move(parsed_statement)),
	                               "Parsed statement", false);

	// all the statements should be equal
	for (idx_t i = 1; i < verify_statements.size(); i++) {
		if (!verify_statements[i].require_equality) {
			continue;
		}
		D_ASSERT(verify_statements[i].statement->Equals(verify_statements[0].statement.get()));
	}

	// now perform checking on the expressions
#ifdef DEBUG
	for (idx_t i = 1; i < verify_statements.size(); i++) {
		D_ASSERT(verify_statements[i].select_list.size() == verify_statements[0].select_list.size());
	}
	auto expr_count = verify_statements[0].select_list.size();
	auto &orig_expr_list = verify_statements[0].select_list;
	for (idx_t i = 0; i < expr_count; i++) {
		// run the ToString, to verify that it doesn't crash
		auto str = orig_expr_list[i]->ToString();
		for (idx_t v_idx = 0; v_idx < verify_statements.size(); v_idx++) {
			if (!verify_statements[v_idx].require_equality && orig_expr_list[i]->HasSubquery()) {
				continue;
			}
			// check that the expressions are equivalent
			D_ASSERT(orig_expr_list[i]->Equals(verify_statements[v_idx].select_list[i].get()));
			// check that the hashes are equivalent too
			D_ASSERT(orig_expr_list[i]->Hash() == verify_statements[v_idx].select_list[i]->Hash());

			verify_statements[v_idx].select_list[i]->Verify();
		}
		D_ASSERT(!orig_expr_list[i]->Equals(nullptr));

		if (orig_expr_list[i]->HasSubquery()) {
			continue;
		}
		// ToString round trip
		auto parsed_list = Parser::ParseExpressionList(str);
		D_ASSERT(parsed_list.size() == 1);
		D_ASSERT(parsed_list[0]->Equals(orig_expr_list[i].get()));
	}
	// perform additional checking within the expressions
	for (idx_t outer_idx = 0; outer_idx < orig_expr_list.size(); outer_idx++) {
		auto hash = orig_expr_list[outer_idx]->Hash();
		for (idx_t inner_idx = 0; inner_idx < orig_expr_list.size(); inner_idx++) {
			auto hash2 = orig_expr_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				D_ASSERT(!orig_expr_list[outer_idx]->Equals(orig_expr_list[inner_idx].get()));
			}
		}
	}
#endif

	// disable profiling if it is enabled
	auto &config = ClientConfig::GetConfig(*this);
	bool profiling_is_enabled = config.enable_profiler;
	if (profiling_is_enabled) {
		config.enable_profiler = false;
	}

	// see below
	auto statement_copy_for_explain = select_stmt->Copy();

	// execute the original statement
	vector<unique_ptr<MaterializedQueryResult>> results;
	for (idx_t i = 0; i < verify_statements.size(); i++) {
		interrupted = false;
		try {
			auto result = RunStatementInternal(lock, query, move(verify_statements[i].statement), false, false);
			results.push_back(unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result)));
		} catch (std::exception &ex) {
			results.push_back(make_unique<MaterializedQueryResult>(ex.what()));
		}
	}

	// check explain, only if q does not already contain EXPLAIN
	if (results[0]->success) {
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_unique<ExplainStatement>(move(statement_copy_for_explain));
		try {
			RunStatementInternal(lock, explain_q, move(explain_stmt), false, false);
		} catch (std::exception &ex) { // LCOV_EXCL_START
			return "EXPLAIN failed but query did not (" + string(ex.what()) + ")";
		} // LCOV_EXCL_STOP
	}

	config.enable_optimizer = true;

	if (profiling_is_enabled) {
		config.enable_profiler = true;
	}

	// now compare the results
	// the results of all runs should be identical
	for (idx_t i = 1; i < results.size(); i++) {
		auto name = verify_statements[i].statement_name;
		if (results[0]->success != results[i]->success) { // LCOV_EXCL_START
			string result = name + " differs from original result!\n";
			result += "Original Result:\n" + results[0]->ToString();
			result += name + ":\n" + results[i]->ToString();
			return result;
		}                                                             // LCOV_EXCL_STOP
		if (!results[0]->collection.Equals(results[i]->collection)) { // LCOV_EXCL_START
			string result = name + " differs from original result!\n";
			result += "Original Result:\n" + results[0]->ToString();
			result += name + ":\n" + results[i]->ToString();
			return result;
		} // LCOV_EXCL_STOP
	}

	return "";
}

bool ClientContext::UpdateFunctionInfoFromEntry(ScalarFunctionCatalogEntry *existing_function,
                                                CreateScalarFunctionInfo *new_info) {
	if (new_info->functions.empty()) {
		throw InternalException("Registering function without scalar function definitions!");
	}
	bool need_rewrite_entry = false;
	idx_t size_new_func = new_info->functions.size();
	for (idx_t exist_idx = 0; exist_idx < existing_function->functions.size(); ++exist_idx) {
		bool can_add = true;
		for (idx_t new_idx = 0; new_idx < size_new_func; ++new_idx) {
			if (new_info->functions[new_idx].Equal(existing_function->functions[exist_idx])) {
				can_add = false;
				break;
			}
		}
		if (can_add) {
			new_info->functions.push_back(existing_function->functions[exist_idx]);
			need_rewrite_entry = true;
		}
	}
	return need_rewrite_entry;
}

void ClientContext::RegisterFunction(CreateFunctionInfo *info) {
	RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*this);
		auto existing_function = (ScalarFunctionCatalogEntry *)catalog.GetEntry(
		    *this, CatalogType::SCALAR_FUNCTION_ENTRY, info->schema, info->name, true);
		if (existing_function) {
			if (UpdateFunctionInfoFromEntry(existing_function, (CreateScalarFunctionInfo *)info)) {
				// function info was updated from catalog entry, rewrite is needed
				info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			}
		}
		// create function
		catalog.CreateFunction(*this, info);
	});
}

void ClientContext::RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
                                                     bool requires_valid_transaction) {
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    transaction.ActiveTransaction().IsInvalidated()) {
		throw Exception("Failed: transaction has been invalidated!");
	}
	// check if we are on AutoCommit. In this case we should start a transaction
	bool require_new_transaction = transaction.IsAutoCommit() && !transaction.HasActiveTransaction();
	if (require_new_transaction) {
		D_ASSERT(!active_query);
		transaction.BeginTransaction();
	}
	try {
		fun();
	} catch (StandardException &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		}
		throw;
	} catch (std::exception &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		} else {
			ActiveTransaction().Invalidate();
		}
		throw;
	}
	if (require_new_transaction) {
		transaction.Commit();
	}
}

void ClientContext::RunFunctionInTransaction(const std::function<void(void)> &fun, bool requires_valid_transaction) {
	auto lock = LockContext();
	RunFunctionInTransactionInternal(*lock, fun, requires_valid_transaction);
}

unique_ptr<TableDescription> ClientContext::TableInfo(const string &schema_name, const string &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// obtain the table info
		auto &catalog = Catalog::GetCatalog(*this);
		auto table = catalog.GetEntry<TableCatalogEntry>(*this, schema_name, table_name, true);
		if (!table) {
			return;
		}
		// write the table info to the result
		result = make_unique<TableDescription>();
		result->schema = schema_name;
		result->table = table_name;
		for (auto &column : table->columns) {
			result->columns.emplace_back(column.name, column.type);
		}
	});
	return result;
}

void ClientContext::Append(TableDescription &description, ChunkCollection &collection) {
	RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*this);
		auto table_entry = catalog.GetEntry<TableCatalogEntry>(*this, description.schema, description.table);
		// verify that the table columns and types match up
		if (description.columns.size() != table_entry->columns.size()) {
			throw Exception("Failed to append: table entry has different number of columns!");
		}
		for (idx_t i = 0; i < description.columns.size(); i++) {
			if (description.columns[i].type != table_entry->columns[i].type) {
				throw Exception("Failed to append: table entry has different number of columns!");
			}
		}
		for (auto &chunk : collection.Chunks()) {
			table_entry->storage->Append(*table_entry, *this, *chunk);
		}
	});
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
#ifdef DEBUG
	D_ASSERT(!relation.GetAlias().empty());
	D_ASSERT(!relation.ToString().empty());
#endif
	RunFunctionInTransaction([&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto result = relation.Bind(*binder);
		D_ASSERT(result.names.size() == result.types.size());
		for (idx_t i = 0; i < result.names.size(); i++) {
			result_columns.emplace_back(result.names[i], result.types[i]);
		}
	});
}

unordered_set<string> ClientContext::GetTableNames(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("Expected a single statement");
	}

	unordered_set<string> result;
	RunFunctionInTransactionInternal(*lock, [&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		binder->SetBindingMode(BindingMode::EXTRACT_NAMES);
		binder->Bind(*statements[0]);
		result = binder->GetTableNames();
	});
	return result;
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	InitialCleanup(*lock);

	string query;
	if (config.query_verification_enabled) {
		// run the ToString method of any relation we run, mostly to ensure it doesn't crash
		relation->ToString();
		relation->GetAlias();
		if (relation->IsReadOnly()) {
			// verify read only statements by running a select statement
			auto select = make_unique<SelectStatement>();
			select->node = relation->GetQueryNode();
			RunStatementInternal(*lock, query, move(select), false);
		}
	}
	auto &expected_columns = relation->Columns();
	auto relation_stmt = make_unique<RelationStatement>(relation);

	unique_ptr<QueryResult> result;
	result = RunStatementInternal(*lock, query, move(relation_stmt), false);
	if (!result->success) {
		return result;
	}
	// verify that the result types and result names of the query match the expected result types/names
	if (result->types.size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->types.size(); i++) {
			if (result->types[i] != expected_columns[i].type || result->names[i] != expected_columns[i].name) {
				mismatch = true;
				break;
			}
		}
		if (!mismatch) {
			// all is as expected: return the result
			return result;
		}
	}
	// result mismatch
	string err_str = "Result mismatch in query!\nExpected the following columns: [";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		if (i > 0) {
			err_str += ", ";
		}
		err_str += expected_columns[i].name + " " + expected_columns[i].type.ToString();
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->types.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->names[i] + " " + result->types[i].ToString();
	}
	err_str += "]";
	return make_unique<MaterializedQueryResult>(err_str);
}

bool ClientContext::TryGetCurrentSetting(const std::string &key, Value &result) {
	// first check the built-in settings
	auto &db_config = DBConfig::GetConfig(*this);
	auto option = db_config.GetOptionByName(key);
	if (option) {
		result = option->get_setting(*this);
		return true;
	}

	// then check the session values
	const auto &session_config_map = config.set_variables;
	const auto &global_config_map = db_config.set_variables;

	auto session_value = session_config_map.find(key);
	bool found_session_value = session_value != session_config_map.end();
	auto global_value = global_config_map.find(key);
	bool found_global_value = global_value != global_config_map.end();
	if (!found_session_value && !found_global_value) {
		return false;
	}

	result = found_session_value ? session_value->second : global_value->second;
	return true;
}

ParserOptions ClientContext::GetParserOptions() {
	ParserOptions options;
	options.preserve_identifier_case = ClientConfig::GetConfig(*this).preserve_identifier_case;
	return options;
}
*/
} // namespace duckdb
