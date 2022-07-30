#include "execution/physical_operator/produce_results.hpp"
//#include "common/types/chunk_collection.hpp"

#include "execution/physical_operator.hpp"

#include <algorithm>
#include <cassert>
namespace duckdb {
class ProduceResultsState : public LocalSinkState {
public:
	explicit ProduceResultsState() {
	}
	
public:
	//duckdb::ChunkCollection resultChunks;
	vector<DataChunk*> resultChunks;
};

unique_ptr<LocalSinkState> ProduceResults::GetLocalSinkState() const {
	return make_unique<ProduceResultsState>();
}

SinkResultType ProduceResults::Sink(DataChunk &input, LocalSinkState &lstate) const {
	auto &state = (ProduceResultsState &)lstate;
	//std::cout << "sinked tuples: " << input.size() << std::endl;

	auto copyChunk = new DataChunk();
	copyChunk->Initialize( input.GetTypes() );
	input.Copy(*copyChunk, 0);
	state.resultChunks.push_back(copyChunk);
	//state.resultChunks.Append(*copyChunk);

	return SinkResultType::NEED_MORE_INPUT;
}

void ProduceResults::Combine(LocalSinkState& lstate) const {
	auto& state = (ProduceResultsState &) lstate;

	int LIMIT = 10;
	size_t num_total_tuples = 0;
	for (auto &it : state.resultChunks) num_total_tuples += it->size();
	std::cout << "===================================================" << std::endl;
	std::cout << "[ResultSetSummary] Total " <<  num_total_tuples << " tuples. Showing top " << LIMIT <<":" << std::endl;
	auto& firstchunk = state.resultChunks[0];
	LIMIT = std::min( (int)(firstchunk->size()), LIMIT);
	// TODO print column schema
	for( int idx = 0 ; idx < LIMIT ; idx++) {
		for( auto& colIdx: schema.getColumnIndicesForResultSet() ) {
			std::cout << "\t" << firstchunk->GetValue(colIdx, idx).ToString();
		}
		std::cout << std::endl;
	}
	std::cout << "===================================================" << std::endl;

	return;
}


std::string ProduceResults::ParamsToString() const {
	return "getresults-param";
}

std::string ProduceResults::ToString() const {
	return "ProduceResults";
}
}