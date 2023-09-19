
#include "execution/cypher_pipeline_executor.hpp"
#include "execution/physical_operator/physical_operator.hpp"
#include "execution/execution_context.hpp"
#include "common/limits.hpp"
#include "common/types.hpp"
#include "main/client_context.hpp"

#include "icecream.hpp"

#include <cassert>

// #define DEBUG_PRINT_PIPELINE

namespace duckdb {

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext* context, CypherPipeline* pipeline): 
	CypherPipelineExecutor(context, pipeline, std::move(vector<CypherPipelineExecutor*>()), std::move(std::map<CypherPhysicalOperator*, CypherPipelineExecutor*>()) ) { }

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext* context, CypherPipeline* pipeline, vector<CypherPipelineExecutor*> childs_p): 
	CypherPipelineExecutor(context, pipeline, childs_p, std::move(std::map<CypherPhysicalOperator*, CypherPipelineExecutor*>())) { }	// need to deprecate

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext* context_p, CypherPipeline* pipeline_p, vector<CypherPipelineExecutor*> childs_p, std::map<CypherPhysicalOperator*, CypherPipelineExecutor*> deps_p): 
	 pipeline(pipeline_p), thread(*(context_p->client)), context(context_p), childs(std::move(childs_p)), deps(std::move(deps_p)) {
		// set context.thread
		context->thread = &thread;

		// initialize interm chunks
		for( int i = 0; i < pipeline->pipelineLength - 1; i++) {	// from source to operators ; not sink.
			auto opOutputChunk = std::make_unique<DataChunk>();
			opOutputChunk->Initialize(pipeline->GetIdxOperator(i)->GetTypes());
			opOutputChunks.push_back(std::move(opOutputChunk));
		}
		D_ASSERT( opOutputChunks.size() == (pipeline->pipelineLength - 1) );
		local_source_state = pipeline->source->GetLocalSourceState(*context);
		local_sink_state = pipeline->sink->GetLocalSinkState(*context);
		for( auto op: pipeline->GetOperators() ) {
			local_operator_states.push_back(op->GetOperatorState(*context));
		}
	}

void CypherPipelineExecutor::ExecutePipeline() {
	// init source chunk
	while(true) {
		auto& source_chunk = *(opOutputChunks[0]);
		// source_chunk.Reset();
		source_chunk.Destroy();
		source_chunk.Initialize( pipeline->GetSource()->GetTypes());
		FetchFromSource(source_chunk);

#ifdef DEBUG_PRINT_PIPELINE
		std::cout << "[FetchFromSource (" << pipeline->GetSource()->ToString() << ")] num_tuples: " << source_chunk.size() << std::endl;
#endif
		if( source_chunk.size() == 0 ) { break; }

		auto sourceProcessResult = ProcessSingleSourceChunk(source_chunk);
		D_ASSERT( sourceProcessResult == OperatorResultType::NEED_MORE_INPUT ||
				 sourceProcessResult == OperatorResultType::FINISHED
		);
		if( sourceProcessResult == OperatorResultType::FINISHED ) { break; }
	}
	// do we need pushfinalize?
		// when limit operator reports early finish, the caches must be finished after all.
		// we need these anyways, but i believe this can be embedded in to the regular logic.
			// this is an invariant to the main logic when the pipeline is terminated early

#ifdef DEBUG_PRINT_PIPELINE
	std::cout << "[Sink-Combine (" << pipeline->GetSink()->ToString() << ")]" << std::endl;
#endif
	StartOperator(pipeline->GetSink());
	pipeline->GetSink()->Combine(*context, *local_sink_state);
	EndOperator(pipeline->GetSink(), nullptr);

	// flush profiler contents	
	// TODO operators may need to flush expressionexecutor stats on termination. refer to OperatorState::Finalize()
	context->client->profiler->Flush(thread.profiler);
	// TODO delete op-states after pipeline finishes
}

void CypherPipelineExecutor::FetchFromSource(DataChunk &result) {

	StartOperator(pipeline->GetSource());
	switch( childs.size() ) {
		// no child pipeline
		case 0: { pipeline->GetSource()->GetData( *context, result, *local_source_state ); break;}
		// single child
		case 1: { pipeline->GetSource()->GetData( *context, result, *local_source_state, *(childs[0]->local_sink_state) ); break; }
	}
	EndOperator(pipeline->GetSource(), &result);
	pipeline->GetSource()->processed_tuples += result.size();	
	
}

OperatorResultType CypherPipelineExecutor::ProcessSingleSourceChunk(DataChunk &source, idx_t initial_idx) {

	auto pipeOutputChunk = std::make_unique<DataChunk>();
	pipeOutputChunk->Initialize( pipeline->GetIdxOperator(pipeline->pipelineLength - 2)->GetTypes() );
	// handle source until need_more_input;
	while(true) {
		//pipeOutputChunk->Reset(); // TODO huh?

		auto pipeResult = ExecutePipe(source, *pipeOutputChunk);
		// shortcut returning execution finished for this pipeline
		if( pipeResult == OperatorResultType::FINISHED ) {
			return OperatorResultType::FINISHED;
		}
#ifdef DEBUG_PRINT_PIPELINE
		std::cout << "[Sink (" << pipeline->GetSink()->ToString() << ")] num_tuples: " << pipeOutputChunk->size() << std::endl;
#endif
		StartOperator(pipeline->GetSink());
		auto sinkResult = pipeline->GetSink()->Sink(
			*context, *pipeOutputChunk, *local_sink_state
		);
		EndOperator(pipeline->GetSink(), nullptr);
		// count produced tuples only on ProduceResults operator
		if( pipeline->GetSink()->ToString().find("ProduceResults") != std::string::npos ) {
			pipeline->GetSink()->processed_tuples += pipeOutputChunk->size();
		}

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
#ifdef DEBUG_PRINT_PIPELINE
		std::cout << "[ExecutePipe - " << current_idx << "(" << pipeline->GetIdxOperator(current_idx)->ToString() <<")] prev num_tuples: " << prev_output_chunk.size() << std::endl;
#endif

		duckdb::OperatorResultType opResult;
		StartOperator(pipeline->GetIdxOperator(current_idx));
		if( ! pipeline->GetIdxOperator(current_idx)->IsSink() ) {
			// standalone operators e.g. filter, projection, adjidxjoin
			opResult = pipeline->GetIdxOperator(current_idx)->Execute(
			 	*context, prev_output_chunk, current_output_chunk, *local_operator_states[current_idx-1]
		);
		} else {
			// operator with related sink e.g. hashjoin, ..
			opResult = pipeline->GetIdxOperator(current_idx)->Execute(
			 	*context, prev_output_chunk, current_output_chunk, *local_operator_states[current_idx-1],
				*(deps.find(pipeline->GetIdxOperator(current_idx))->second->local_sink_state)
			);
		}
		EndOperator(pipeline->GetIdxOperator(current_idx), &current_output_chunk);

		pipeline->GetIdxOperator(current_idx)->processed_tuples += current_output_chunk.size();
	
		// if result needs more output, push index to stack
		if( opResult == OperatorResultType::HAVE_MORE_OUTPUT) {
			in_process_operators.push(current_idx);
		} else if( opResult == OperatorResultType::FINISHED ) {
			return OperatorResultType::FINISHED;
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

void CypherPipelineExecutor::StartOperator(CypherPhysicalOperator *op) {
	// if (context.client.interrupted) {
	// 	throw InterruptException();
	// }
	context->thread->profiler.StartOperator(op);
}

void CypherPipelineExecutor::EndOperator(CypherPhysicalOperator *op, DataChunk *chunk) {
	context->thread->profiler.EndOperator(chunk);

	// if (chunk) {
	// 	chunk->Verify();
	// }
}

}