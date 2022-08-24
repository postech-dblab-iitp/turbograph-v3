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

namespace duckdb {

class ClientContext;
class ThreadContext;

class ExecutionContext {
public:
	// ExecutionContext(ClientContext &client_p, ThreadContext &thread_p) : client(client_p), thread(thread_p) {
	// }
	ExecutionContext(ClientContext *client_p) : client(client_p) { }
	// TODO Add more contexts depending on what we need


	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext *client; 
	//! The thread-local context for this execution
	// ThreadContext &thread;
};

} // namespace duckdb
