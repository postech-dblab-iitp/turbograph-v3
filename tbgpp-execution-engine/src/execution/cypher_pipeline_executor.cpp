
#include "execution/cypher_pipeline_executor.hpp"

#include "execution/physical_operator/physical_operator.hpp"

#include "common/limits.hpp"
#include "main/client_context.hpp"

#include "execution/execution_context.hpp"

//#include "livegraph.hpp"
#include "common/types.hpp"

#include "icecream.hpp"

#include <cassert>

namespace duckdb {

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext* context, CypherPipeline* pipeline, vector<CypherPipelineExecutor*> childs_p): 
	CypherPipelineExecutor(context, pipeline) { childs = childs_p; }

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext* context, CypherPipeline* pipeline): 
	context(context), pipeline(pipeline) {

	// initialize interm chunks
	for( int i = 0; i < pipeline->pipelineLength - 1; i++) {	// from source to operators ; not sink.
		auto opOutputChunk = std::make_unique<DataChunk>();
		opOutputChunk->Initialize(pipeline->GetIdxOperator(i)->GetTypes());
		opOutputChunks.push_back(move(opOutputChunk));
	}

	assert( opOutputChunks.size() == (pipeline->pipelineLength - 1) );
	// std::cout << "pipelinelength=" << pipe->pipelineLength << std::endl;
	// generate global states for each operator
		// no global states for this demo!
	// Manage local states
	local_source_state = pipeline->source->GetLocalSourceState(*context);
	local_sink_state = pipeline->sink->GetLocalSinkState(*context);
	for( auto op: pipeline->GetOperators() ) {
		local_operator_states.push_back(op->GetOperatorState(*context));
	}
}

void CypherPipelineExecutor::ExecutePipeline() {
	
	// init source chunk
	while(true) {
		// std::cout << "fetching!!" << std::endl;
		auto& source_chunk = *(opOutputChunks[0]);
		// std::cout << "why?!!" << std::endl;
		// source_chunk.Reset();
		source_chunk.Destroy();
		source_chunk.Initialize( pipeline->GetSource()->GetTypes());
		FetchFromSource(source_chunk);
		// std::cout << "fetched!!" << std::endl;
		if( source_chunk.size() == 0 ) { break; }

		auto sourceProcessResult = ProcessSingleSourceChunk(source_chunk);
			// this will always result NEED_MORE_INPUT;
	}
	// do we need pushfinalize?
		// when limit operator reports early finish, the caches must be finished after all.
		// we need these anyways, but i believe this can be embedded in to the regular logic.
			// this is an invariant to the main logic when the pipeline is terminated early

// std::cout << "calling combine for sink (which is printing out the result)" << std::endl;
	pipeline->GetSink()->Combine(*context, *local_sink_state);

	// TODO delete op-states after pipeline finishes

}

void CypherPipelineExecutor::FetchFromSource(DataChunk &result) {

// std::cout << "starting (source) operator" << std::endl;
	// timer start
	if( ! pipeline->GetSource()->timer_started ){
		pipeline->GetSource()->op_timer.start();
		pipeline->GetSource()->timer_started = true;
	} else {
		pipeline->GetSource()->op_timer.resume();
	}
	// call
// FIXME
icecream::ic.enable();
//IC(pipeline->GetSource()->ToString());
icecream::ic.disable();
	switch( childs.size() ) {
		// no child pipeline
		case 0: { pipeline->GetSource()->GetData( *context, result, *local_source_state ); break;}
		// single child
		case 1: { pipeline->GetSource()->GetData( *context, result, *local_source_state, *(childs[0]->local_sink_state) ); break; }
	}
	pipeline->GetSource()->processed_tuples += result.size();	
	
	// timer stop
	pipeline->GetSource()->op_timer.stop();
// icecream::ic.enable();
//IC( pipeline->GetSource()->ToString(), pipeline->GetSource()->op_timer.elapsed().wall / 1000000.0 );
// icecream::ic.disable();
}

OperatorResultType CypherPipelineExecutor::ProcessSingleSourceChunk(DataChunk &source, idx_t initial_idx) {

	auto pipeOutputChunk = std::make_unique<DataChunk>();
	pipeOutputChunk->Initialize( pipeline->GetIdxOperator(pipeline->pipelineLength - 2)->GetTypes() );
	// handle source until need_more_input;
	while(true) {
		//pipeOutputChunk->Reset(); // TODO huh?
		
		// call execute pipe
		// std::cout << "call execute pipe!!" << std::endl;
		// IC();
		auto pipeResult = ExecutePipe(source, *pipeOutputChunk);
		// call sink
			// timer start
		if( ! pipeline->GetSink()->timer_started ){
			pipeline->GetSink()->op_timer.start();
			pipeline->GetSink()->timer_started = true;
		} else {
			pipeline->GetSink()->op_timer.resume();
		}
		// std::cout << "call sink!!" << std::endl;
// FIXME
icecream::ic.enable();
//IC(pipeline->GetSink()->ToString());
icecream::ic.disable();
		auto sinkResult = pipeline->GetSink()->Sink(
			*context, *pipeOutputChunk, *local_sink_state
		);
		// count produced tuples only on ProduceResults operator
		// TODO actually, ProduceResults also does not need processed_tuples too since it does not push results upwards, but returns it to the user.
		if( pipeline->GetSink()->ToString().find("ProduceResults") != std::string::npos ) {
			pipeline->GetSink()->processed_tuples += pipeOutputChunk->size();
		}
		// timer stop
pipeline->GetSink()->op_timer.stop();
// icecream::ic.enable();
// IC( pipeline->GetSink()->ToString(), pipeline->GetSink()->op_timer.elapsed().wall / 1000000.0 );
// icecream::ic.disable();

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
	if( pipeline->pipelineLength == 2) { // nothing passes through pipe.
		result.Reference(input);
	}

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
// std::cout << "starting (interm) operator" << std::endl;
		// give interm as input and interm as output
			// timer start
		if( ! pipeline->GetIdxOperator(current_idx)->timer_started ){
			pipeline->GetIdxOperator(current_idx)->op_timer.start();
			pipeline->GetIdxOperator(current_idx)->timer_started = true;
		} else {
			pipeline->GetIdxOperator(current_idx)->op_timer.resume();
		}
			// call operator
// FIXME
icecream::ic.enable();
//IC(pipeline->GetIdxOperator(current_idx)->ToString());
icecream::ic.disable();
		auto opResult = pipeline->GetIdxOperator(current_idx)->Execute(
			 *context, prev_output_chunk, current_output_chunk, *local_operator_states[current_idx-1]
		);
		pipeline->GetIdxOperator(current_idx)->processed_tuples += current_output_chunk.size();
			// timer stop
		pipeline->GetIdxOperator(current_idx)->op_timer.stop();
// icecream::ic.enable();
// IC( pipeline->GetIdxOperator(current_idx)->ToString(), pipeline->GetIdxOperator(current_idx)->op_timer.elapsed().wall / 1000000.0 );
// icecream::ic.disable();
		// if result needs more output, push index to stack
		if( opResult == OperatorResultType::HAVE_MORE_OUTPUT) {
			in_process_operators.push(current_idx);
		}
		// what is chunk cache for?
		// increment
		current_idx += 1;
	}
	// pipe done as we reached the sink
	// TODO need to add one more case : terminate pipe for e.g. for LIMIT query.
	return in_process_operators.empty() ?
		OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}
}