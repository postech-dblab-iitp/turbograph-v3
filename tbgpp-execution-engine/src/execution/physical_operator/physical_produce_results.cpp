#include "execution/physical_operator/physical_produce_results.hpp"
//#include "common/types/chunk_collection.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cassert>

#include "icecream.hpp"

namespace duckdb {
	
class ProduceResultsState : public LocalSinkState {
public:
	explicit ProduceResultsState(std::vector<uint8_t> projection_mapping)
		: projection_mapping(projection_mapping) {
		isResultTypeDetermined = false;
	}

	void DetermineResultTypes(const std::vector<LogicalType> input_types) {
		if(projection_mapping.size() == 0){
			result_types = input_types;
		} else {
			for(auto& target: projection_mapping) {
				result_types.push_back(input_types[target]);
			}
		}
		isResultTypeDetermined = true;
	}
	
public:
	vector<DataChunk*> resultChunks;

	bool isResultTypeDetermined;
	std::vector<uint8_t> projection_mapping;
	std::vector<duckdb::LogicalType> result_types;
};

unique_ptr<LocalSinkState> PhysicalProduceResults::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<ProduceResultsState>(projection_mapping);
}

SinkResultType PhysicalProduceResults::Sink(ExecutionContext& context, DataChunk &input, LocalSinkState &lstate) const {
	auto &state = (ProduceResultsState &)lstate;
	// std::cout << "sinked tuples: " << input.size() << std::endl;

	if(!state.isResultTypeDetermined) { state.DetermineResultTypes(input.GetTypes()); }

	auto copyChunk = new DataChunk();
	copyChunk->Initialize( state.result_types );
	if( projection_mapping.size() == 0) {			// projection mapping not provided
		input.Copy(*copyChunk, 0);	
	} else {										// projection mapping provided
		for(idx_t idx=0; idx<copyChunk->ColumnCount(); idx++) {
			VectorOperations::Copy(input.data[projection_mapping[idx]], copyChunk->data[idx], input.size(), 0, 0);
		}
		copyChunk->SetCardinality(input.size());
	}
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