
#include "execution/cypher_pipeline_executor.hpp"

#include "duckdb/execution/physical_operator.hpp"


#include "duckdb/common/limits.hpp"
#include "storage/graph_store.hpp"
#include "livegraph.hpp"

#include <cassert>

using namespace duckdb;

CypherPipelineExecutor::CypherPipelineExecutor(CypherPipeline* pipe, GraphStore* g) {

	// store pipeline and graph
	pipeline = pipe;
	graphstore = g;
	// initialize interm chunks
	for( int i = 0; i < pipe->pipelineLength - 1; i++) {	// from source to operators ; not sink.
		auto opOutputChunk = std::make_unique<DataChunk>();
		opOutputChunk->Initialize(pipe->GetIdxOperator(i)->GetTypes());
		opOutputChunks.push_back(move(opOutputChunk));
	}

	assert( opOutputChunks.size() == (pipe->pipelineLength - 1) );
	// generate global states for each operator
		// no global states for this demo!
	// Manage local states
	local_source_state = pipeline->source->GetLocalSourceState();
	local_sink_state = pipeline->sink->GetLocalSinkState();
	for( auto op: pipeline->GetOperators() ) {
		local_operator_states.push_back(op->GetOperatorState());
	}
}

void CypherPipelineExecutor::ExecutePipeline() {
	
	// init source chunk
	while(true) {
		
		auto& source_chunk = *opOutputChunks[0];
		source_chunk.Reset();
		FetchFromSource(source_chunk);
		if( source_chunk.size() == 0 ) { break; }

		auto sourceProcessResult = ProcessSingleSourceChunk(source_chunk);
			// this will always result NEED_MORE_INPUT;
	}
	// do we need pushfinalize?
		// when limit operator reports early finish, the caches must be finished after all.
		// we need these anyways, but i believe this can be embedded in to the regular logic.
			// this is an invariant to the main logic when the pipeline is terminated early

	std::cout << "calling combine for sink (which is printing out the result)" << std::endl;
	pipeline->GetSink()->Combine(*local_sink_state);

}

void CypherPipelineExecutor::FetchFromSource(DataChunk &result) {

	std::cout << "starting (source) operator" << std::endl;
	pipeline->GetSource()->GetData( graphstore, result, *local_source_state );
	std::cout << "done (source) operator" << std::endl;
}

OperatorResultType CypherPipelineExecutor::ProcessSingleSourceChunk(DataChunk &source, idx_t initial_idx) {

	auto pipeOutputChunk = std::make_unique<DataChunk>();
	pipeOutputChunk->Initialize( pipeline->GetIdxOperator(pipeline->pipelineLength - 2)->GetTypes() );
	// handle source until need_more_input;
	while(true) {
		final_chunk.Reset();
		
		// call execute pipe
		auto pipeResult = ExecutePipe(source, *pipeOutputChunk);
		// call sink
		std::cout << "starting (sink) operator" << std::endl;
		auto sinkResult = pipeline->GetSink()->Sink(
			source, *local_sink_state
		);
		// break when pipes for single chunk finishes
		if( pipeResult == OperatorResultType::NEED_MORE_INPUT ) {
			break;
		}
	}
	// clear pipeline states.
	in_process_operators = stack<idx_t>();

	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType CypherPipelineExecutor::ExecutePipe(DataChunk &input, DataChunk &result) {

	// determine this pipe's start point
	int current_idx = 1;
	if (!in_process_operators.empty()) {
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
	assert( current_idx > 0 && "cannot designate a source operator (idx=0)" );

	// start pipe from current_idx;
	for(;;) {
		bool isCurrentSink =
			pipeline->GetIdxOperator(current_idx) == pipeline->GetSink();
		if( isCurrentSink ) { break; }

		auto& current_output_chunk =
			current_idx >= (pipeline->pipelineLength - 2) ? result : *opOutputChunks[current_idx]; // connect result when at last operator
		auto& prev_output_chunk = *opOutputChunks[current_idx-1];
		current_output_chunk.Reset();

		// start current operator
		std::cout << "starting (interm) operator" << std::endl;
		// give interm as input and interm as output
		auto opResult = pipeline->GetIdxOperator(current_idx)->Execute(
			prev_output_chunk, current_output_chunk, *local_operator_states[current_idx-1]
		);
		// if result needs more output, push index to stack
		if( opResult == OperatorResultType::HAVE_MORE_OUTPUT) {
			in_process_operators.push(current_idx);
		}
		// what is chunk cache for?
		// increment
		current_idx += 1;
	}
	// pipe done as we reached the sink
	return in_process_operators.empty() ?
		OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}