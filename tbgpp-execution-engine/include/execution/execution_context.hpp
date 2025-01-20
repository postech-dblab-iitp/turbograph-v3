//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "main/client_context.hpp"
#include "execution/schema_flow_graph.hpp"

namespace s62 {

class ClientContext;
class ThreadContext;

class ExecutionContext {
public:
	// ExecutionContext(ClientContext *client_p, ThreadContext &thread_p) : client(client_p), thread(thread_p) {
	// }
	ExecutionContext(ClientContext *client_p) : client(client_p) {
		// NOTE; thread should be manually set after calling constructor
	}

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext *client;
	//! The thread-local context for this execution
	ThreadContext *thread;
	//! The schema flow graph for this execution
	SchemaFlowGraph *sfg;
	//! The intermediate schema infos for this execution
	vector<Schema> *schema_infos;
	
	//! if this is the last pipeline, store pointer to query results here
	vector<unique_ptr<DataChunk>> *query_results;
	
};

} // namespace s62
