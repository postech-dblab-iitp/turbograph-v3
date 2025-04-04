
#include "execution/cypher_pipeline_executor.hpp"
#include "common/limits.hpp"
#include "common/types.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "execution/execution_context.hpp"
#include "execution/physical_operator/physical_operator.hpp"
#include "execution/schema_flow_graph.hpp"
#include "main/client_context.hpp"

#include "icecream.hpp"

#include <cassert>

namespace duckdb {

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext *context,
                                               CypherPipeline *pipeline)
    : CypherPipelineExecutor(
          context, pipeline, std::move(vector<CypherPipelineExecutor *>()),
          std::move(
              std::map<CypherPhysicalOperator *, CypherPipelineExecutor *>()))
{}

CypherPipelineExecutor::CypherPipelineExecutor(
    ExecutionContext *context, CypherPipeline *pipeline,
    vector<CypherPipelineExecutor *> childs_p)
    : CypherPipelineExecutor(
          context, pipeline, childs_p,
          std::move(
              std::map<CypherPhysicalOperator *, CypherPipelineExecutor *>()))
{}  // need to deprecate

CypherPipelineExecutor::CypherPipelineExecutor(
    ExecutionContext *context_p, CypherPipeline *pipeline_p,
    vector<CypherPipelineExecutor *> childs_p,
    std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> deps_p)
    : pipeline(pipeline_p),
      thread(*(context_p->client)),
      context(context_p),
      childs(std::move(childs_p)),
      deps(std::move(deps_p))
{
	// set context.thread
	context->thread = &thread;

	// initialize interm chunks
	// from source to operators ; not sink. // TODO pipelinelength - 1 -> pipelinelength
    for (int i = 0; i < pipeline->pipelineLength - 1; i++) {
		auto opOutputChunk = std::make_unique<DataChunk>();
		opOutputChunk->Initialize(pipeline->GetIdxOperator(i)->GetTypes());
		opOutputChunks.push_back(std::vector<unique_ptr<DataChunk>>());
		opOutputChunks[i].push_back(std::move(opOutputChunk));
		opOutputSchemaIdx.push_back(0);
	}
	D_ASSERT(opOutputChunks.size() == (pipeline->pipelineLength - 1));
	local_source_state = pipeline->GetSource()->GetLocalSourceState(*context);
	local_sink_state = pipeline->GetSink()->GetLocalSinkState(*context);
	for (auto op: pipeline->GetOperators()) {
		local_operator_states.push_back(op->GetOperatorState(*context));
	}
}

CypherPipelineExecutor::CypherPipelineExecutor(ExecutionContext *context,
                                               CypherPipeline *pipeline,
                                               SchemaFlowGraph &sfg)
    : CypherPipelineExecutor(
          context, pipeline, sfg, std::move(vector<CypherPipelineExecutor *>()),
          std::move(
              std::map<CypherPhysicalOperator *, CypherPipelineExecutor *>()))
{}

CypherPipelineExecutor::CypherPipelineExecutor(
    ExecutionContext *context, CypherPipeline *pipeline, SchemaFlowGraph &sfg,
    vector<CypherPipelineExecutor *> childs_p)
    : CypherPipelineExecutor(
          context, pipeline, sfg, childs_p,
          std::move(
              std::map<CypherPhysicalOperator *, CypherPipelineExecutor *>()))
{}

CypherPipelineExecutor::CypherPipelineExecutor(
    ExecutionContext *context_p, CypherPipeline *pipeline_p,
    SchemaFlowGraph &sfg, vector<CypherPipelineExecutor *> childs_p,
    std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> deps_p)
    : pipeline(pipeline_p),
      thread(*(context_p->client)),
      context(context_p),
      sfg(std::move(sfg)),
      childs(std::move(childs_p)),
      deps(std::move(deps_p))
{
	// set context.thread
	context->thread = &thread;

	// initialize interm chunks
	for (int i = 0; i < pipeline->pipelineLength; i++) { 
		Schema &output_schema = pipeline->GetIdxOperator(i)->schema;
		opOutputChunks.push_back(std::vector<unique_ptr<DataChunk>>());
		// we maintain only one chunk for source node (union schema)
		size_t num_datachunks = 1;
        for (int j = 0; j < num_datachunks; j++) {
            pipeline->GetIdxOperator(i)->InitializeOutputChunks(
                opOutputChunks[i], output_schema, j);
        }
		opOutputSchemaIdx.push_back(0);
	}
	D_ASSERT(opOutputChunks.size() == (pipeline->pipelineLength));
	local_source_state = pipeline->GetSource()->GetLocalSourceState(*context);
	local_sink_state = pipeline->GetSink()->GetLocalSinkState(*context);
	for (auto op: pipeline->GetOperators()) {
		local_operator_states.push_back(op->GetOperatorState(*context));
	}

#ifdef DEBUG_PRINT_PIPELINE
    sfg.printSchemaGraph();
#endif
}

CypherPipelineExecutor::~CypherPipelineExecutor() {
	local_source_state.reset();
	local_sink_state.reset();
	for (auto &op_state : local_operator_states) {
		op_state.reset();
	}
}

void CypherPipelineExecutor::ReinitializePipeline()
{
	if(!pipeline->IsSinkSingleton()) {
		throw std::runtime_error("ReinitializePipeline is only for singleton sink pipelines");
	}

	auto sinkOutputChunk = std::move(opOutputChunks.back());
	opOutputChunks.clear();
	for (int i = 0; i < pipeline->pipelineLength-1; i++) { 
		Schema &output_schema = pipeline->GetIdxOperator(i)->schema;
		opOutputChunks.push_back(std::vector<unique_ptr<DataChunk>>());
		// we maintain only one chunk for source node (union schema)
		size_t num_datachunks = 1;
        for (int j = 0; j < num_datachunks; j++) {
            pipeline->GetIdxOperator(i)->InitializeOutputChunks(
                opOutputChunks[i], output_schema, j);
        }
		opOutputSchemaIdx.push_back(0);
	}
	opOutputChunks.push_back(std::move(sinkOutputChunk));
	D_ASSERT(opOutputChunks.size() == (pipeline->pipelineLength));
	local_source_state = pipeline->GetSource()->GetLocalSourceState(*context);
	local_operator_states.clear();
	for (auto op: pipeline->GetOperators()) {
		local_operator_states.push_back(op->GetOperatorState(*context));
	}
}

void CypherPipelineExecutor::ExecutePipeline()
{
    // init source chunk
    while (true) {
        auto &source_chunk = *(opOutputChunks[0][0]);
        if (sfg.IsSFGExists()) {
            /**
			 * This is temporal code.
			 * TODO: need to be refactored.
			*/
			if (sfg.IsSchemaChanged()) {
				// fprintf(stdout, "%s\n", sfg.GetOutputSchema(0, sfg.GetCurSourceIdx()).printStoredTypes().c_str());
				source_chunk.InitializeValidCols(
                	sfg.GetOutputSchema(0, sfg.GetCurSourceIdx()).getStoredTypes());
			}
			source_chunk.Reset();
			opOutputSchemaIdx[0] = sfg.GetCurSourceIdx();
		} else {
			source_chunk.Destroy();
			source_chunk.Initialize(pipeline->GetSource()->GetTypes());
		}
        FetchFromSource(source_chunk);

#ifdef DEBUG_PRINT_PIPELINE
        std::cout << "[FetchFromSource (" << pipeline->GetSource()->ToString()
                  << ")] num_tuples: " << source_chunk.size() << std::endl;
#endif
#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
        PrintOutputChunk(pipeline->GetSource()->ToString(), source_chunk);
#endif
		bool isSourceDataFinished = false;
		switch (childs.size()) {
			case 0: { isSourceDataFinished = !pipeline->GetSource()->IsSourceDataRemaining(*local_source_state); break; }
			case 1: { isSourceDataFinished = !pipeline->GetSource()->IsSourceDataRemaining(*local_source_state, *(childs[0]->local_sink_state)); break; }
		}

		if (source_chunk.size() > 0) {
			auto sourceProcessResult = ProcessSingleSourceChunk(source_chunk);
			D_ASSERT(sourceProcessResult == OperatorResultType::NEED_MORE_INPUT ||
					sourceProcessResult == OperatorResultType::FINISHED ||
					sourceProcessResult == OperatorResultType::POSTPONE_OUTPUT);
			if (sourceProcessResult == OperatorResultType::FINISHED) { break; }
		}

		if (isSourceDataFinished) {
			if (sfg.AdvanceCurSourceIdx()) continue;
			else {
				// TODO process remaining postponed outputs
				if (pipeline->AdvanceGroup()) {
					sfg.ReplaceToOtherSourceSchema();
					ReinitializePipeline();
					continue;
				}
				else break;
			}
		}
		// if (++num_pipeline_executions == max_pipeline_executions) { break; } // for debugging
	}
	// do we need pushfinalize?
		// when limit operator reports early finish, the caches must be finished after all.
		// we need these anyways, but i believe this can be embedded in to the regular logic.
			// this is an invariant to the main logic when the pipeline is terminated early

#ifdef DEBUG_PRINT_PIPELINE
    std::cout << "[Sink-Combine (" << pipeline->GetSink()->ToString() << ")]"
              << std::endl;
#endif
    StartOperator(pipeline->GetReprSink());
    pipeline->GetSink()->Combine(*context, *local_sink_state);
	if (pipeline->GetSink()->type == PhysicalOperatorType::PRODUCE_RESULTS) {
    	EndOperator(pipeline->GetReprSink(), *context->query_results);
	}
	else {
    	EndOperator(pipeline->GetReprSink(), (DataChunk*) nullptr);
	}

    // flush profiler contents
    // TODO operators may need to flush expressionexecutor stats on termination. refer to OperatorState::Finalize()
    context->client->profiler->Flush(thread.profiler);
    // TODO delete op-states after pipeline finishes
}

void CypherPipelineExecutor::FetchFromSource(DataChunk &result)
{
    StartOperator(pipeline->GetReprSource());
    switch (childs.size()) {
        // no child pipeline
        case 0: {
            pipeline->GetSource()->GetData(*context, result,
                                           *local_source_state);
            break;
        }
        // single child
        case 1: {
            pipeline->GetSource()->GetData(*context, result,
                                           *local_source_state,
                                           *(childs[0]->local_sink_state));
            break;
        }
    }
    EndOperator(pipeline->GetReprSource(), &result);
    pipeline->GetSource()->processed_tuples += result.size();
}

OperatorResultType CypherPipelineExecutor::ProcessSingleSourceChunk(DataChunk &source, idx_t initial_idx) {
	DataChunk *pipeOutputChunk = nullptr;
	idx_t output_schema_idx;
	// handle source until need_more_input;
	/**
	 * Why pipeline->pipelineLength - 2?
	 * Here, pipeOutputChunk means the input to the sink.
	 * The index of the sink operator is pipeline->pipelineLength - 1.
	 * The index of the last intermeidate operator is therefore pipeline->pipelineLength - 2.
	*/
	while (true) {
		OperatorResultType pipeResult;
		if (pipeline->pipelineLength == 2) { // nothing passes through pipe.
			idx_t src_schema_idx = source.GetSchemaIdx();
			idx_t output_schema_idx = 0;
			// idx_t output_schema_idx = sfg.GetNextSchemaIdx(pipeline->pipelineLength - 2, src_schema_idx);
			// pipeOutputChunk = opOutputChunks[pipeline->pipelineLength - 2][output_schema_idx].get();
			// pipeOutputChunk->Reference(source);
			pipeOutputChunk = &source;
			pipeResult = OperatorResultType::NEED_MORE_INPUT;
			D_ASSERT(in_process_operators.empty()); // TODO: In this case it should definitely be like this... but check plz
		} else {
			pipeResult = ExecutePipe(source, output_schema_idx);
			D_ASSERT(output_schema_idx < opOutputChunks[pipeline->pipelineLength - 2].size());
			pipeOutputChunk = opOutputChunks[pipeline->pipelineLength - 2][output_schema_idx].get();
		}
		
		// shortcut returning execution finished/postponed for this pipeline
		if (pipeResult == OperatorResultType::FINISHED || 
			pipeResult == OperatorResultType::POSTPONE_OUTPUT) {
			return pipeResult;
		} else if (pipeResult == OperatorResultType::OUTPUT_EMPTY) {
			// no data to sink
			if (in_process_operators.empty()) { // NEED_MORE_INPUT
				break;
			} else { // HAVE_MORE_OUTPUT
				continue;
			}
		}
        
#ifdef DEBUG_PRINT_PIPELINE
        std::cout << "[Sink (" << pipeline->GetSink()->ToString()
                  << ")] num_tuples: " << pipeOutputChunk->size() << std::endl;
#endif
#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
        PrintInputChunk(pipeline->GetSink()->ToString(), *pipeOutputChunk);
#endif
        StartOperator(pipeline->GetReprSink());
        auto sinkResult = pipeline->GetSink()->Sink(*context, *pipeOutputChunk,
                                                    *local_sink_state);
        EndOperator(pipeline->GetReprSink(), (DataChunk*) nullptr);
#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
        PrintOutputChunk(
            pipeline->GetSink()->ToString(),
            pipeline->GetSink()->GetLastSinkedData(*local_sink_state));
#endif
        // count produced tuples only on ProduceResults operator
        if (pipeline->GetSink()->ToString().find("ProduceResults") !=
            std::string::npos) {
            pipeline->GetSink()->processed_tuples += pipeOutputChunk->size();
        }

        // break when pipes for single chunk finishes
        if (pipeResult == OperatorResultType::NEED_MORE_INPUT) {
            break;
        }
    }
    // clear pipeline states.
    in_process_operators = stack<idx_t>();
    return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType CypherPipelineExecutor::ExecutePipe(DataChunk &input, idx_t &output_schema_idx) {
	// determine this pipe's start point
	int current_idx = 1;
	if (!in_process_operators.empty()) {
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
	assert(current_idx > 0 && "cannot designate a source operator (idx=0)");

	// start pipe from current_idx;
	// DataChunk *prev_output_chunk = opOutputChunks[current_idx - 1][0].get();
	idx_t prev_output_schema_idx = current_idx == 1 ? 0 : opOutputSchemaIdx[current_idx - 1];
	DataChunk *prev_output_chunk = opOutputChunks[current_idx - 1][prev_output_schema_idx].get();
	DataChunk *current_output_chunk;
	vector<unique_ptr<DataChunk>> *current_output_chunks;
	for (;pipeline->GetIdxOperator(current_idx) != pipeline->GetSink(); current_idx++) {
		D_ASSERT(prev_output_chunk != nullptr);
		auto prev_output_schema_idx = prev_output_chunk->GetSchemaIdx();
		OperatorType cur_op_type = sfg.GetOperatorType(current_idx);
		cur_op_type = OperatorType::UNARY;
		idx_t current_output_schema_idx;

		/**
		 * Unary operator does uni-schema processing.
		 * Binary operator does multi-schema processing,
		 * which means a single execution can generate multiple schemas.
		*/	

		if (cur_op_type == OperatorType::UNARY) {
			/**
			 * Q. (tslee) Why we need reset?
			 * A. (jhha) Since we are reusing the chunk, we need to reset the chunk. 
			 * The Execute() function assumes that the chunk is empty.
			 * Based on the assumption, it determines return value after the execution.
			 * If empty, NEED_MORE_INPUT. Else, HAVE_MORE_OUTPUT.
			 * See HashJoin for the example. 
			 * 
			 * Suppose this pipeline. OP1 -> OP2 -> OP3 (all are unary operators).
			 * Assume after an execution, OP2 and OP3 outputs HAVE_MORE_OUTPUT.
			 * Due to in_process_operators logic, OP3 will be executed first, until it returns NEED_MORE_INPUT.
			 * Then, OP2 will be executed. In this time, the output of OP2, which is the input of OP3, is resetted.
			 * If not, OP3 can have invalid input.
			*/
			// D_ASSERT(prev_output_schema_idx == opOutputSchemaIdx[current_idx-1]);
			// current_output_schema_idx = sfg.GetNextSchemaIdx(current_idx, prev_output_schema_idx);
			current_output_schema_idx = 0;
			current_output_chunk = opOutputChunks[current_idx][current_output_schema_idx].get();
			current_output_chunk->Reset(); 
			current_output_chunk->SetSchemaIdx(current_output_schema_idx);
			output_schema_idx = current_output_schema_idx;
		} else if (cur_op_type == OperatorType::BINARY) {
			/**
			 * How binary operator executes.
			 * For example, consider ProduceResults-Join-NodeScan
			 * Assume that schema graph is as follows:
			 *       
			 * SCH1 | SCH2 | SCH3
			 *  	    |
			 *          |
			 * SCH1 | SCH2 | SCH3
			 *          | 
			 *          |
			 *        SCH1
			 * 
			 * On each execuion of Join, it generates chunk for SCH1, SCH2, SCH3.
			 * Join keeps track of the number of remaining tuples for each schema.
			 * Join processes each schema completely and then moves to the next schema.
			 * See IdSeekState.has_remaining_output
			*/

			/**
			 * Regarding reset of binary operators.
			 * Unlike unary operators, binary operators have multiple of chunks.
			 * Among them, we have to reset the chunk that is processed.
			 * Unfortunatelly, we don't have a way to know which chunk is processed, currently.
			 * Therefore, we have to maintain those information in PipelineExecutor.
			*/

			current_output_chunks = &opOutputChunks[current_idx];
			D_ASSERT(opOutputSchemaIdx[current_idx] < current_output_chunks->size());
			current_output_chunks->at(opOutputSchemaIdx[current_idx])->Reset();
		}
#ifdef DEBUG_PRINT_PIPELINE
        if (cur_op_type == OperatorType::UNARY) {
            std::cout << "[ExecutePipe(" << pipeline->GetPipelineId() << ") - "
                      << current_idx << "(#"
                      << pipeline->GetIdxOperator(current_idx)->GetOperatorId()
                      << " "
                      << pipeline->GetIdxOperator(current_idx)->ToString()
                      << ")] prev num_tuples: " << prev_output_chunk->size()
                      << ", schema_idx " << prev_output_schema_idx << " -> "
                      << current_output_schema_idx << std::endl;
        }
        else if (cur_op_type == OperatorType::BINARY) {
            std::cout << "[ExecutePipe(" << pipeline->GetPipelineId() << ") - "
                      << current_idx << "(#"
                      << pipeline->GetIdxOperator(current_idx)->GetOperatorId()
                      << " "
                      << pipeline->GetIdxOperator(current_idx)->ToString()
                      << ")] prev num_tuples: " << prev_output_chunk->size()
                      << std::endl;
        }
#endif

#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
        PrintInputChunk(pipeline->GetIdxOperator(current_idx)->ToString(),
                        *prev_output_chunk);
#endif

		duckdb::OperatorResultType opResult;
		StartOperator(pipeline->GetReprIdxOperator(current_idx));
		if (cur_op_type == OperatorType::UNARY) {
			// execute operator
			if (!pipeline->GetIdxOperator(current_idx)->IsSink()) {
				opResult = pipeline->GetIdxOperator(current_idx)->Execute(
					*context, *prev_output_chunk, *current_output_chunk, *local_operator_states[current_idx-1]);
			} else {
				opResult = pipeline->GetIdxOperator(current_idx)->Execute(
					*context, *prev_output_chunk, *current_output_chunk, *local_operator_states[current_idx-1],
					*(deps.find(pipeline->GetIdxOperator(current_idx))->second->local_sink_state));
			}

			// register output schema index
			opOutputSchemaIdx[current_idx] = current_output_schema_idx;

			// record statistics
			EndOperator(pipeline->GetReprIdxOperator(current_idx), current_output_chunk);
			pipeline->GetIdxOperator(current_idx)->processed_tuples += current_output_chunk->size();

#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
            PrintOutputChunk(pipeline->GetIdxOperator(current_idx)->ToString(),
                             *current_output_chunk);
#endif
			prev_output_chunk = current_output_chunk;
		} else if (cur_op_type == OperatorType::BINARY) {
			// execute operator
			if (!pipeline->GetIdxOperator(current_idx)->IsSink()) {
				opResult = pipeline->GetIdxOperator(current_idx)->Execute(
					*context, *prev_output_chunk, *current_output_chunks, *local_operator_states[current_idx-1], output_schema_idx);
			}
			else {
				/**
				 * TODO: Implement multi-schema handling (currently, this assumes UNION schema)
				*/
				opResult = pipeline->GetIdxOperator(current_idx)->Execute(
					*context, *prev_output_chunk, *((*current_output_chunks)[0]), *local_operator_states[current_idx-1],
					*(deps.find(pipeline->GetIdxOperator(current_idx))->second->local_sink_state));
				(*current_output_chunks)[0]->SetSchemaIdx(0);
				output_schema_idx = 0;
			}

			// register output schema index
			opOutputSchemaIdx[current_idx] = output_schema_idx;

			// record statistics
			if (opResult == OperatorResultType::POSTPONE_OUTPUT ||
				opResult == OperatorResultType::OUTPUT_EMPTY) {
				EndOperator(pipeline->GetReprIdxOperator(current_idx), (DataChunk*) nullptr);
				prev_output_chunk = nullptr;
			} else {
				D_ASSERT(output_schema_idx < current_output_chunks->size());
				EndOperator(pipeline->GetReprIdxOperator(current_idx), current_output_chunks->at(output_schema_idx).get());
				pipeline->GetIdxOperator(current_idx)->processed_tuples += current_output_chunks->at(output_schema_idx)->size();

#ifdef DEBUG_PRINT_OP_INPUT_OUTPUT
                PrintOutputChunk(
                    pipeline->GetIdxOperator(current_idx)->ToString(),
                    *(current_output_chunks->at(output_schema_idx)));
#endif
                prev_output_chunk =
                    current_output_chunks->at(output_schema_idx).get();
            }
        }

        // if result needs more output, push index to stack
        if (opResult == OperatorResultType::HAVE_MORE_OUTPUT) {
            in_process_operators.push(current_idx);
        }
        else if (opResult == OperatorResultType::FINISHED) {
            return OperatorResultType::FINISHED;
        }
        else if (opResult == OperatorResultType::POSTPONE_OUTPUT) {
            // TODO handle remaining outputs
            return OperatorResultType::POSTPONE_OUTPUT;
        }
		else if (opResult == OperatorResultType::OUTPUT_EMPTY) {
			return OperatorResultType::OUTPUT_EMPTY;
		}
    }
    // pipe done as we reached the sink
    // TODO need to add one more case : terminate pipe for e.g. for LIMIT query.
    return in_process_operators.empty() ? OperatorResultType::NEED_MORE_INPUT
                                        : OperatorResultType::HAVE_MORE_OUTPUT;
}

void CypherPipelineExecutor::StartOperator(CypherPhysicalOperator *op)
{
    // if (context.client.interrupted) {
    // 	throw InterruptException();
    // }
    context->thread->profiler.StartOperator(op);
}

void CypherPipelineExecutor::EndOperator(CypherPhysicalOperator *op,
                                         DataChunk *chunk)
{
    context->thread->profiler.EndOperator(chunk);
}

void CypherPipelineExecutor::EndOperator(CypherPhysicalOperator *op,
                                         vector<shared_ptr<DataChunk>> &chunks)
{
    context->thread->profiler.EndOperator(chunks);
}

void CypherPipelineExecutor::PrintInputChunk(std::string opname,
                                             DataChunk &input)
{
    fprintf(stdout, "[%s-input] input schema idx: %ld, # tuples = %ld\n",
            opname.c_str(), input.GetSchemaIdx(), input.size());
    OutputUtil::PrintTop10TuplesInDataChunk(input);
}

void CypherPipelineExecutor::PrintOutputChunk(std::string opname,
                                              DataChunk &output)
{
    fprintf(stdout, "[%s-output] output schema idx: %ld, # tuples = %ld\n",
            opname.c_str(), output.GetSchemaIdx(), output.size());
    OutputUtil::PrintTop10TuplesInDataChunk(output);
}

}  // namespace duckdb