#include "execution/physical_operator/physical_produce_results.hpp"
#include "common/types/chunk_collection.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <cassert>

#include "icecream.hpp"

namespace duckdb {

class ProduceResultsState : public LocalSinkState {
   public:
    explicit ProduceResultsState(std::vector<uint64_t> projection_mapping, std::vector<std::vector<uint64_t>> projection_mappings)
        : projection_mapping(projection_mapping), projection_mappings(projection_mappings)
    {
        isResultTypeDetermined = false;
    }

    void DetermineResultTypes(const std::vector<LogicalType> input_types)
    {
        if (projection_mapping.size() == 0 && projection_mappings.size() == 0) {
            result_types = input_types;
        }
        else {
            if (projection_mapping.size() != 0) {
                for (auto &target : projection_mapping) {
                    result_types.push_back(input_types[target]);
                }
            }
            else if (projection_mappings.size() != 0) {
                idx_t schema_idx = 0;
                for (auto &target : projection_mappings[schema_idx]) {
                    result_types.push_back(input_types[target]);
                }
            }
            else {
                D_ASSERT(false);
            }
        }
        isResultTypeDetermined = true;
    }

   public:
    ChunkCollection resultCollection;

    bool isResultTypeDetermined;
    std::vector<uint64_t> projection_mapping;
    std::vector<std::vector<uint64_t>> projection_mappings;
    std::vector<duckdb::LogicalType> result_types;
};

unique_ptr<LocalSinkState> PhysicalProduceResults::GetLocalSinkState(
    ExecutionContext &context) const
{
    return make_unique<ProduceResultsState>(projection_mapping, projection_mappings);
}

SinkResultType PhysicalProduceResults::Sink(ExecutionContext &context,
                                            DataChunk &input,
                                            LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;

    // // for debugging
    // for (auto i = 0; i < input.ColumnCount(); i++) {
    //     auto &validity = FlatVector::Validity(input.data[i]);
    //     num_nulls += (input.size() - validity.CountValid(input.size()));
    // }

    if (!state.isResultTypeDetermined) {
        state.DetermineResultTypes(input.GetTypes());
    }

    if (projection_mapping.size() == 0 && projection_mappings.size() == 0) {
        state.resultCollection.Append(input);
    }
    else {
        if (projection_mapping.size() != 0) {
            state.resultCollection.Append(input,state.result_types, state.projection_mapping);
        }
        else if (projection_mappings.size() != 0) {
            state.resultCollection.Append(input, state.result_types, state.projection_mappings[0]);
        }
        else {
            D_ASSERT(false);
        }
    }

    // auto copyChunk = make_unique<DataChunk>();
    // copyChunk->Initialize(state.result_types, input, projection_mappings, input.size());
    // if (projection_mapping.size() == 0 &&
    //     projection_mappings.size() == 0) {  // projection mapping not provided
    //     input.Copy(*copyChunk, 0);
    // }
    // else {  // projection mapping provided
    //     if (projection_mapping.size() != 0) {
    //         for (idx_t idx = 0; idx < copyChunk->ColumnCount(); idx++) {
    //             VectorOperations::Copy(input.data[projection_mapping[idx]],
    //                                    copyChunk->data[idx], input.size(), 0,
    //                                    0);
    //         }
    //     }
    //     else if (projection_mappings.size() != 0) {
    //         // idx_t schema_idx = input.GetSchemaIdx();
    //         for (idx_t idx = 0; idx < copyChunk->ColumnCount(); idx++) {
    //             if (projection_mappings[0][idx] ==
    //                 std::numeric_limits<uint8_t>::max()) {
    //                 // TODO use is_valid flag
    //                 FlatVector::Validity(copyChunk->data[idx])
    //                     .SetAllInvalid(input.size());
    //                 continue;
    //             }
    //             VectorOperations::Copy(
    //                 input.data[projection_mappings[0][idx]],
    //                 copyChunk->data[idx], input.size(), 0, 0);
    //         }
    //         copyChunk->SetSchemaIdx(0);
    //     }
    //     else {
    //         D_ASSERT(false);
    //     }
    //     copyChunk->SetCardinality(input.size());
    // }
    // state.resultChunks.push_back(move(copyChunk));

    return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalProduceResults::Combine(ExecutionContext &context,
                                     LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;
    context.query_results = ((ProduceResultsState &)lstate).resultCollection.ChunksUnsafe();
    // printf("num_nulls = %ld\n", num_nulls);
    return;
}

DataChunk &PhysicalProduceResults::GetLastSinkedData(
    LocalSinkState &lstate) const
{
    auto &state = (ProduceResultsState &)lstate;
    return state.resultCollection.GetChunk(state.resultCollection.ChunkCount() - 1);
}

std::string PhysicalProduceResults::ParamsToString() const
{
    return "getresults-param";
}

std::string PhysicalProduceResults::ToString() const
{
    return "ProduceResults";
}

}  // namespace duckdb