#include "common/types/schemaless_data_chunk.hpp"
#include "common/types/vector_cache.hpp"

#include "icecream.hpp"

namespace duckdb {

SchemalessDataChunk::SchemalessDataChunk() : DataChunk() {}

SchemalessDataChunk::~SchemalessDataChunk() {}

void SchemalessDataChunk::CreateRowCol(
    const vector<uint32_t> &columns_to_be_grouped, idx_t capacity)
{
    for (auto i = 0; i < columns_to_be_grouped.size(); i++) {
        indirection_idx[i] = -((int32_t)schemaless_data.size()) - 1;
    }

    VectorCache cache(LogicalType::ROWCOL, capacity);
    schemaless_data.emplace_back(cache, capacity);
    schemaless_vector_caches.push_back(move(cache));
}

Vector &SchemalessDataChunk::GetRowCol(idx_t column_idx)
{
    int32_t rowcol_idx = indirection_idx[column_idx];
    D_ASSERT(rowcol_idx < 0);

    auto &rowcol = schemaless_data[-1 - rowcol_idx];
    return rowcol;
}

void SchemalessDataChunk::CreateRowMajorStore(size_t size)
{
    // TODO optimize this code by avoiding dynamic memory allocation
    unique_ptr<char[]> data = unique_ptr<char[]>(new char[size]);
    row_major_datas.push_back(std::move(data));
}

char *SchemalessDataChunk::GetRowMajorStore(idx_t column_idx)
{
    int32_t rowcol_idx = indirection_idx[column_idx];
    D_ASSERT(rowcol_idx < 0);

    return row_major_datas[-1 - rowcol_idx].get();
}

}  // namespace duckdb
