#include "common/types/filtered_data_chunk.hpp"

namespace duckdb {

void FilteredChunkBuffer::Initialize(vector<LogicalType> types)
{
    slice_buffer = make_unique<DataChunk>();
    slice_buffer->Initialize(types, STANDARD_VECTOR_SIZE);
    for (auto i = 0; i < FILTERED_CHUNK_BUFFER_SIZE; i++) {
        auto buffer_chunk = std::make_unique<DataChunk>();
        buffer_chunk->Initialize(types, STANDARD_VECTOR_SIZE);
        buffer_chunks[i] = std::move(buffer_chunk);
    }
    buffer_idx = 0;
}

void FilteredChunkBuffer::Reset(vector<LogicalType> types) {
    slice_buffer->Reset();
    slice_buffer->InitializeValidCols(types);
    GetNextFilteredChunk()->Reset();
    GetNextFilteredChunk()->InitializeValidCols(types);
}

void FilteredChunkBuffer::ReferenceAndSwitch(DataChunk& output) {
    output.Reference(*(GetFilteredChunk().get()));
    SwitchBuffer();
}

void FilteredChunkBuffer::SwitchBuffer() {
    buffer_idx = (buffer_idx + 1) % FILTERED_CHUNK_BUFFER_SIZE;
}

}