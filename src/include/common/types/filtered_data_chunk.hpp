#pragma once

#include "common/types/data_chunk.hpp"

namespace duckdb {

#define FILTERED_CHUNK_BUFFER_SIZE 2

class FilteredChunkBuffer {
public:
    FilteredChunkBuffer(): buffer_idx(0) {
        slice_buffer = nullptr;
        for (auto i = 0; i < FILTERED_CHUNK_BUFFER_SIZE; i++) {
            buffer_chunks.push_back(nullptr);
        }
    }
    
    void Initialize(vector<LogicalType> types);

    void Reset(vector<LogicalType> types);

    void ReferenceAndSwitch(DataChunk& output);
    void SwitchBuffer();

    unique_ptr<DataChunk> &GetSliceBuffer() {
        return slice_buffer;
    }

    unique_ptr<DataChunk> &GetFilteredChunk() {
        return buffer_chunks[buffer_idx];
    }

    unique_ptr<DataChunk> &GetNextFilteredChunk() {
        return GetFilteredChunk((buffer_idx + 1) % FILTERED_CHUNK_BUFFER_SIZE);
    }

    unique_ptr<DataChunk> &GetFilteredChunk(uint64_t idx) {
        idx = idx % FILTERED_CHUNK_BUFFER_SIZE;
        return buffer_chunks[idx];
    }

private:
    std::unique_ptr<DataChunk> slice_buffer;
	std::vector<std::unique_ptr<DataChunk>> buffer_chunks;
	idx_t buffer_idx;
};

}