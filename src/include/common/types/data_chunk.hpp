//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/vector.hpp"
#include "common/winapi.hpp"

namespace duckdb {
class VectorCache;

//!  A Data Chunk represents a set of vectors.
/*!
    The data chunk class is the intermediate representation used by the
   execution engine of DuckDB. It effectively represents a subset of a relation.
   It holds a set of vectors that all have the same length.

    DataChunk is initialized using the DataChunk::Initialize function by
   providing it with a vector of TypeIds for the Vector members. By default,
   this function will also allocate a chunk of memory in the DataChunk for the
   vectors and all the vectors will be referencing vectors to the data owned by
   the chunk. The reason for this behavior is that the underlying vectors can
   become referencing vectors to other chunks as well (i.e. in the case an
   operator does not alter the data, such as a Filter operator which only adds a
   selection vector).

    In addition to holding the data of the vectors, the DataChunk also owns the
   selection vector that underlying vectors can point to.
*/
class DataChunk {
public:
	//! Creates an empty DataChunk
	DUCKDB_API DataChunk();
	DUCKDB_API ~DataChunk();

	//! The vectors owned by the DataChunk.
	vector<Vector> data;

public:
	inline idx_t size() const { // NOLINT
		return count;
	}
	inline idx_t ColumnCount() const {
		return data.size();
	}
	inline void SetCardinality(idx_t count_p) {
		D_ASSERT(count_p <= capacity);
		this->count = count_p;
	}
	inline void SetCardinality(const DataChunk &other) {
		this->count = other.size();
	}
	inline void SetCapacity(const DataChunk &other) {
		this->capacity = other.capacity;
	}
	inline void SetSchemaIdx(idx_t schema_idx) {
		this->schema_idx = schema_idx;
	}
	idx_t GetSchemaIdx() {
		return this->schema_idx;
	}
	inline void SetHasRowChunk(bool has_row_chunk) {
		this->has_row_chunk = has_row_chunk;
	}
	bool HasRowChunk() {
		return this->has_row_chunk;
	}
	bool IsAllInvalid() {
		bool is_invalid = true;
		for (auto &col : data) {
			if (col.GetIsValid()) {
				is_invalid = false;
				break;
			}
		}
		return is_invalid;
	}
	void IncreaseSize(size_t num_new) {
		D_ASSERT(IsAllInvalid());
		size_t new_count = num_new + count;
		if (new_count > capacity) {
			capacity = new_count;
		}
		count = new_count;
	}

	DUCKDB_API Value GetValue(idx_t col_idx, idx_t index) const;
	DUCKDB_API void SetValue(idx_t col_idx, idx_t index, const Value &val);
	DUCKDB_API void SimpleSetValue(idx_t col_idx, idx_t index, const Value &val);

	//! Set the DataChunk to reference another data chunk
	DUCKDB_API void Reference(DataChunk &chunk);
	//! Set the DataChunk to own the data of data chunk, destroying the other chunk in the process
	DUCKDB_API void Move(DataChunk &chunk);

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! This will create one vector of the specified type for each LogicalType in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	DUCKDB_API void Initialize(const vector<LogicalType> &types, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	DUCKDB_API void Initialize(const vector<LogicalType> &types, DataChunk &other, const vector<vector<uint64_t>> &projection_mappings, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	DUCKDB_API void Initialize(const vector<LogicalType> &types, DataChunk &other, const vector<vector<uint64_t>> &projection_mappings, const vector<bool> &rowvec_column, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	DUCKDB_API void Initialize(const vector<LogicalType> &types, DataChunk &other, const vector<uint64_t> &projection_mappings, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	DUCKDB_API void Initialize(const vector<LogicalType> &types, vector<data_ptr_t> &datas, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	DUCKDB_API void InitializeValidCols(const vector<LogicalType> &types, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	//! Initializes an empty DataChunk with the given types. The vectors will *not* have any data allocated for them.
	DUCKDB_API void InitializeEmpty(const vector<LogicalType> &types, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	//! Initializes row columns
	DUCKDB_API void InitializeRowColumn(const vector<uint32_t> &columns_to_be_grouped, idx_t capacity_ = STANDARD_VECTOR_SIZE);
	//! Initializes row major store
	DUCKDB_API void CreateRowMajorStore(const vector<uint32_t> &columns_to_be_grouped, uint64_t row_store_size, schema_mask_ptr_t schema_mask_ptr);
	//! Assign row major store
	DUCKDB_API void AssignRowMajorStore(const vector<uint32_t> &columns_to_be_grouped, buffer_ptr<VectorBuffer> buffer);
	//! Get row major store
	DUCKDB_API char *GetRowMajorStore(idx_t col_idx);
	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Throws an exception if there
	//! is not enough space in the chunk and resize is not allowed.
	DUCKDB_API void Append(const DataChunk &other, bool resize = false, SelectionVector *sel = nullptr,
	                       idx_t count = 0);
	DUCKDB_API void Append(DataChunk &other, std::vector<uint64_t>& projection_mapping,
							bool resize = false, SelectionVector *sel = nullptr, idx_t count = 0);

	//! Destroy all data and columns owned by this DataChunk
	DUCKDB_API void Destroy();

	//! Copies the data from this vector to another vector.
	DUCKDB_API void Copy(DataChunk &other, idx_t offset = 0) const;
	DUCKDB_API void Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count,
	                     const idx_t offset = 0) const;

	//! Splits the DataChunk in two
	DUCKDB_API void Split(DataChunk &other, idx_t split_idx);

	//! Fuses a DataChunk onto the right of this one, and destroys the other. Inverse of Split.
	DUCKDB_API void Fuse(DataChunk &other);

	//! MappedFuse a DataChunk onto the right of this one, and destroys the other. Inverse of Split.
	DUCKDB_API void MappedFuse(DataChunk &other, const vector<uint32_t>& projection_map);

	//! Turn all the vectors from the chunk into flat vectors
	DUCKDB_API void Normalify();

	DUCKDB_API unique_ptr<VectorData[]> Orrify(bool normalify_row = true);

	DUCKDB_API void Slice(const SelectionVector &sel_vector, idx_t count);
	DUCKDB_API void Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	DUCKDB_API void Reset();
	DUCKDB_API void Reset(idx_t capacity_);

	//! Serializes a DataChunk to a stand-alone binary blob
	//DUCKDB_API void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a DataChunk
	//DUCKDB_API void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	DUCKDB_API void Hash(Vector &result);
	DUCKDB_API void Hash(Vector &result, const vector<uint32_t> &target_cols);

	//! Returns a list of types of the vectors of this data chunk
	DUCKDB_API vector<LogicalType> GetTypes();

	//! Converts this DataChunk to a printable string representation
	DUCKDB_API string ToString() const;
	DUCKDB_API string ToString(size_t size_to_print) const;
	//DUCKDB_API void Print();

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	DUCKDB_API void Verify();

	//! export data chunk as a arrow struct array that can be imported as arrow record batch
	//DUCKDB_API void ToArrowArray(ArrowArray *out_array);

	DUCKDB_API void ConvertIsValidToValidityMap(DataChunk& source_chunk, std::vector<uint64_t>& projection_mapping);

private:
	//! The amount of tuples stored in the data chunk
	idx_t count;
	//! The amount of tuples that can be stored in the data chunk
	idx_t capacity;
	//! Vector caches, used to store data when ::Initialize is called
	vector<VectorCache> vector_caches;
	//! Vector caches, used to store data when ::CreateRowCol is called
    vector<VectorCache> row_vector_caches;
	bool has_row_chunk = false;
	idx_t schema_idx;
};
} // namespace duckdb
