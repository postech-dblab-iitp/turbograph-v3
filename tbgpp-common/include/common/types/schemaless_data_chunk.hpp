#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "common/types/vector.hpp"
#include "common/winapi.hpp"

namespace duckdb {
class Vector;
class VectorCache;

class SchemalessDataChunk : public DataChunk {
   public:
    //! Creates an empty DataChunk
    DUCKDB_API SchemalessDataChunk();
    DUCKDB_API ~SchemalessDataChunk();

    //! The schemaless vectors owned by the SchemalessDataChunk.
    vector<Vector> schemaless_data;

   public:
    void CreateRowCol(const vector<uint32_t> &columns_to_be_grouped,
                      idx_t capacity);
    Vector &GetRowCol(idx_t column_idx);

    void CreateRowMajorStore(size_t size);
    char *GetRowMajorStore(idx_t column_idx);

   private:
    vector<int32_t> indirection_idx;
    vector<bool> valid_column;  // TODO bitmap?

    //! Vector caches, used to store data when ::CreateRowCol is called
    vector<VectorCache> schemaless_vector_caches;
    //! row major format data // TODO use heap?
    vector<unique_ptr<char[]>> row_major_datas;
};

};  // namespace duckdb