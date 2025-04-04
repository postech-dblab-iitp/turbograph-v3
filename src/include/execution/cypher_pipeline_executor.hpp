#ifndef CYPHER_PIPELINE_EXECUTOR
#define CYPHER_PIPELINE_EXECUTOR

#include "common/output_util.hpp"
#include "common/types/data_chunk.hpp"
#include "common/stack.hpp"
#include "execution/cypher_pipeline.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "storage/graph_storage_wrapper.hpp"
#include "execution/physical_operator/physical_operator.hpp"
#include "parallel/thread_context.hpp"

#include <functional>
#include <map>

// #define DEBUG_PRINT_OP_INPUT_OUTPUT
// #define DEBUG_PRINT_PIPELINE

namespace duckdb {

struct LogicalType;
class Executor;
class ClientContext;
class SchemaFlowGraph;
// class SchemalessDataChunk;

//! The Pipeline class represents an execution pipeline
class CypherPipelineExecutor {
	static constexpr const idx_t CACHE_THRESHOLD = 64;

public:
	
	CypherPipelineExecutor(ExecutionContext *context, CypherPipeline *pipeline);
	CypherPipelineExecutor(ExecutionContext *context, CypherPipeline *pipeline, vector<CypherPipelineExecutor *> childs_p);
	CypherPipelineExecutor(ExecutionContext *context, CypherPipeline *pipeline, vector<CypherPipelineExecutor *> childs_p,
		std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> deps_p);
	CypherPipelineExecutor(ExecutionContext *context, CypherPipeline *pipeline, SchemaFlowGraph &sfg);
	CypherPipelineExecutor(ExecutionContext *context, CypherPipeline *pipeline, SchemaFlowGraph &sfg,
		vector<CypherPipelineExecutor *> childs_p);
	CypherPipelineExecutor(ExecutionContext *context, CypherPipeline *pipeline, SchemaFlowGraph &sfg,
		 vector<CypherPipelineExecutor *> childs_p, std::map<CypherPhysicalOperator *, CypherPipelineExecutor *> deps_p);

	~CypherPipelineExecutor();
	
	//! Fully execute a pipeline with a source and a sink until the source is completely exhausted
	virtual void ExecutePipeline();

	//! Push a single input DataChunk into the pipeline.
	// //! Returns either OperatorResultType::NEED_MORE_INPUT or OperatorResultType::FINISHED
	// //! If OperatorResultType::FINISHED is returned, more input will not change the result anymore
	// OperatorResultType ExecutePush(DataChunk &input);

	//! Called after depleting the source: finalizes the execution of this pipeline executor
	//! This should only be called once per PipelineExecutor
	// void PushFinalize();

	//! Initializes a chunk with the types that will flow out of ExecutePull
	//void InitializeChunk(DataChunk &chunk);

	//! The pipeline to process
	ExecutionContext *context;
	CypherPipeline *pipeline;
	SchemaFlowGraph sfg;
	//! The thread context of this executor
	ThreadContext thread;


	// TODO add statistics reporter.


	//! Intermediate chunks for the operators
	vector<vector<unique_ptr<DataChunk>>> opOutputChunks;
	// vector<vector<unique_ptr<SchemalessDataChunk>>> opOutputChunks;

	//! Selected output schema indexes for the operators
	vector<idx_t> opOutputSchemaIdx;

	// in duckdb, each stated is stored in:
		// .            local        |  global
		// source   pipelineexec     pipeline
	 	// operator pipelineexec        op
		// sink     pipelineexec        op
	
	// in our demo it is sufficient to keep only local states
	vector<unique_ptr<OperatorState>> local_operator_states;
	unique_ptr<LocalSourceState> local_source_state;
	unique_ptr<LocalSinkState> local_sink_state;


	//! The operators that are not yet finished executing and have data remaining
	//! If the stack of in_process_operators is empty, we fetch from the source instead
	stack<idx_t> in_process_operators;
	//! Whether or not the pipeline has been finalized (used for verification only)
	//bool finalized = false;
	//! Whether or not the pipeline has finished processing
	int32_t finished_processing_idx = -1;
	//! Whether or not this pipeline requires keeping track of the batch index of the source
	//bool requires_batch_index = false;

	//! Cached chunks for any operators that require caching
	vector<unique_ptr<DataChunk>> cached_chunks;

	//! Child executors - to access sink information of the source 			// child : pipe's sink == op's source
	vector<CypherPipelineExecutor *> childs;
	//! Dependent executors - to access sink information of the operator	// dep   : pipe's sink == op's operator
	std::map<duckdb::CypherPhysicalOperator*, duckdb::CypherPipelineExecutor*> deps;

private:
	void StartOperator(CypherPhysicalOperator *op);
	void EndOperator(CypherPhysicalOperator *op, DataChunk *chunk);
	void EndOperator(CypherPhysicalOperator *op, vector<shared_ptr<DataChunk>> &chunks);

	//! Reset the operator index to the first operator
	void GoToSource(idx_t &current_idx, idx_t initial_idx);
	void FetchFromSource(DataChunk &result);

	void FinishProcessing(int32_t operator_idx = -1);
	bool IsFinished();

	OperatorResultType ProcessSingleSourceChunk(DataChunk &input, idx_t initial_idx = 0);
	//! Pushes a chunk through the pipeline and returns a single result chunk
	//! Returns whether or not a new input chunk is needed, or whether or not we are finished
	// OperatorResultType ExecutePipe(DataChunk &input, DataChunk &result);
	OperatorResultType ExecutePipe(DataChunk &input, idx_t &output_schema_idx);

	static bool CanCacheType(const LogicalType &type);
	void CacheChunk(DataChunk &input, idx_t operator_idx);

	void PrintInputChunk(std::string opname, DataChunk &input);
	void PrintOutputChunk(std::string opname, DataChunk &output);
	void ReinitializePipeline();
};
}

#endif