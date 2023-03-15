#include "execution/physical_operator/physical_produce_results.hpp"
//#include "common/types/chunk_collection.hpp"

#include <algorithm>
#include <cassert>

#include "icecream.hpp"

namespace duckdb {
	
class ProduceResultsState : public LocalSinkState {
public:
	explicit ProduceResultsState() {
	}
	
public:
	//duckdb::ChunkCollection resultChunks;
	vector<DataChunk*> resultChunks;
};

unique_ptr<LocalSinkState> PhysicalProduceResults::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<ProduceResultsState>();
}

SinkResultType PhysicalProduceResults::Sink(ExecutionContext& context, DataChunk &input, LocalSinkState &lstate) const {
	auto &state = (ProduceResultsState &)lstate;
	// std::cout << "sinked tuples: " << input.size() << std::endl;

	auto copyChunk = new DataChunk();
	copyChunk->Initialize( input.GetTypes() );
	copyChunk->Reference(input);
	state.resultChunks.push_back(copyChunk);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalProduceResults::Combine(ExecutionContext& context, LocalSinkState& lstate) const {
	auto& state = (ProduceResultsState &) lstate;

	// register sinked results to execution context
	context.query_results = &(((ProduceResultsState &)lstate).resultChunks);

	return;
}


std::string PhysicalProduceResults::ParamsToString() const {
	return "getresults-param";
}

std::string PhysicalProduceResults::ToString() const {
	return "ProduceResults";
}
}