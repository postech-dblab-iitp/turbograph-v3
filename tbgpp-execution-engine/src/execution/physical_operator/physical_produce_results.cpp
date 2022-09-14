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
	// std::cout << "A: " << input.size() << std::endl;
	copyChunk->Initialize( input.GetTypes() );
	// std::cout << "B" << input.size() << std::endl;
	// std::cout << input.ColumnCount() << "\n" << input.ToString(1) << std::endl;
	input.Copy(*copyChunk, 0);
	// std::cout << "C " << input.size() << std::endl;
	state.resultChunks.push_back(copyChunk);

	//state.resultChunks.Append(*copyChunk);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalProduceResults::Combine(ExecutionContext& context, LocalSinkState& lstate) const {
	auto& state = (ProduceResultsState &) lstate;


	int LIMIT = 10;
	size_t num_total_tuples = 0;
	for (auto &it : state.resultChunks) num_total_tuples += it->size();
	std::cout << "===================================================" << std::endl;
	std::cout << "[ResultSetSummary] Total " <<  num_total_tuples << " tuples. Showing top " << LIMIT <<":" << std::endl;
	if (num_total_tuples != 0) {
		auto& firstchunk = state.resultChunks[0];
		LIMIT = std::min( (int)(firstchunk->size()), LIMIT);
		for( auto& colIdx: schema.getColumnIndicesForResultSet() ) {
			std::cout << "\t" << firstchunk->GetTypes()[colIdx].ToString();
		}
		std::cout << std::endl;
		for( int idx = 0 ; idx < LIMIT ; idx++) {
			for( auto& colIdx: schema.getColumnIndicesForResultSet() ) {
				std::cout << "\t" << firstchunk->GetValue(colIdx, idx).ToString();
			}
			std::cout << std::endl;
		}
	}
	std::cout << "===================================================" << std::endl;

	return;
}


std::string PhysicalProduceResults::ParamsToString() const {
	return "getresults-param";
}

std::string PhysicalProduceResults::ToString() const {
	return "ProduceResults";
}
}