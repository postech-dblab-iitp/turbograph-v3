#include <numeric>
#include "common/types/schemaless_data_chunk.hpp"
#include "common/types/vector_cache.hpp"

#include "icecream.hpp"

namespace s62 {

SchemalessDataChunk::SchemalessDataChunk() : DataChunk() {}

SchemalessDataChunk::~SchemalessDataChunk() {}

void SchemalessDataChunk::CreateRowCol(
    const vector<uint32_t> &columns_to_be_grouped, idx_t capacity)
{
    if (indirection_idx.size() == 0) {
        indirection_idx.resize(this->ColumnCount());
        std::iota(indirection_idx.begin(), indirection_idx.end(), 0);
    }
    for (auto i = 0; i < columns_to_be_grouped.size(); i++) {
        indirection_idx[columns_to_be_grouped[i]] = -((int32_t)schemaless_data.size()) - 1;
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

Vector &SchemalessDataChunk::GetIthCol(idx_t column_idx)
{
    int32_t rowcol_idx = indirection_idx[column_idx];
    if (rowcol_idx >= 0) {
        return this->data[column_idx];
    } else {
        auto &rowcol = schemaless_data[-1 - rowcol_idx];
        return rowcol;
    }
}

bool SchemalessDataChunk::isIthColStoredInRowStore(idx_t column_idx)
{
    int32_t rowcol_idx = indirection_idx[column_idx];
    return rowcol_idx < 0;
}

void SchemalessDataChunk::CreateRowMajorStore(size_t size)
{
    
    unique_ptr<char[]> data = unique_ptr<char[]>(new char[size]);
    row_major_datas.push_back(std::move(data));
}

char *SchemalessDataChunk::GetRowMajorStore(idx_t column_idx)
{
    int32_t rowcol_idx = indirection_idx[column_idx];
    D_ASSERT(rowcol_idx < 0);

    return row_major_datas[-1 - rowcol_idx].get();
}

}  // namespace s62
