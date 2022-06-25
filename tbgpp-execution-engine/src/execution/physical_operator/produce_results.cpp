#include "execution/physical_operator/produce_results.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

#include "duckdb/execution/physical_operator.hpp"

#include <cassert>

class ProduceResultsState : public LocalSinkState {
public:
	explicit ProduceResultsState() {
	}
	
public:
	duckdb::ChunkCollection resultChunks;
};

unique_ptr<LocalSinkState> ProduceResults::GetLocalSinkState() const {
	return make_unique<ProduceResultsState>();
}

SinkResultType ProduceResults::Sink(DataChunk &input, LocalSinkState &lstate) const {
	auto &state = (ProduceResultsState &)lstate;

	state.resultChunks.Append(input);

	return SinkResultType::NEED_MORE_INPUT;
}

void ProduceResults::Combine(LocalSinkState& lstate) const {
	auto& state = (ProduceResultsState &) lstate;

	state.resultChunks.Print();
	return;
}


std::string ProduceResults::ParamsToString() const {
	return "getresults-param";
}