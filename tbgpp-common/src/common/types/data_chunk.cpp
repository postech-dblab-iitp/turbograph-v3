#include "common/types/data_chunk.hpp"

#include "common/array.hpp"
//#include "common/arrow.hpp"
#include "common/exception.hpp"
#include "common/helper.hpp"
//#include "common/printer.hpp"
//#include "common/serializer.hpp"
#include "common/to_string.hpp"
#include "common/types/date.hpp"
#include "common/types/interval.hpp"
#include "common/types/null_value.hpp"
#include "common/types/sel_cache.hpp"
#include "common/types/timestamp.hpp"
#include "common/types/vector_cache.hpp"
#include "common/unordered_map.hpp"
#include "common/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"
//#include "common/types/arrow_aux_data.hpp"
#include "common/types/uuid.hpp"

#include "icecream.hpp"

namespace duckdb {

DataChunk::DataChunk() : count(0), capacity(STANDARD_VECTOR_SIZE) {
}

DataChunk::~DataChunk() {
}

void DataChunk::InitializeEmpty(const vector<LogicalType> &types, idx_t capacity_) {
	capacity = capacity_;
	D_ASSERT(data.empty());   // can only be initialized once
	D_ASSERT(!types.empty()); // empty chunk not allowed
	data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		data.emplace_back(Vector(types[i], nullptr));
		if (types[i].id() == LogicalTypeId::SQLNULL) data[i].is_valid = false;
	}
}

void DataChunk::Initialize(const vector<LogicalType> &types, idx_t capacity_) {
	D_ASSERT(data.empty());   // can only be initialized once
	D_ASSERT(!types.empty()); // empty chunk not allowed
	capacity = capacity_;
	data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		VectorCache cache(types[i], capacity);
		data.emplace_back(cache, capacity);
		vector_caches.push_back(move(cache));
		if (types[i].id() == LogicalTypeId::SQLNULL) data[i].is_valid = false;
	}
}

void DataChunk::Initialize(const vector<LogicalType> &types, vector<data_ptr_t> &datas, idx_t capacity_) {
	capacity = capacity_;
	D_ASSERT(data.empty());   // can only be initialized once
	D_ASSERT(!types.empty()); // empty chunk not allowed
	data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		data.emplace_back(Vector(types[i], datas[i]));
		if (types[i].id() == LogicalTypeId::SQLNULL) data[i].is_valid = false;
	}
}

void DataChunk::InitializeRowColumn(const vector<uint32_t> &columns_to_be_grouped, idx_t count) {
	// create rowcol_t array to be shared by all row columns
	// TODO is this data removed?
	VectorCache cache(LogicalType::ROWCOL, count);

	for (idx_t i = 0; i < columns_to_be_grouped.size(); i++) {
		D_ASSERT(columns_to_be_grouped[i] < data.size());
		data[columns_to_be_grouped[i]].CreateRowColumn(cache, count);
		data[columns_to_be_grouped[i]].SetVectorType(VectorType::ROW_VECTOR);
	}
}

void DataChunk::CreateRowMajorStore(const vector<uint32_t> &columns_to_be_grouped, uint64_t row_store_size) {
	// TODO optimize this code by avoiding dynamic memory allocation?
	auto row_store = make_buffer<VectorRowStoreBuffer>();
	row_store->Reserve(row_store_size);
	for (idx_t i = 0; i < columns_to_be_grouped.size(); i++) {
		D_ASSERT(columns_to_be_grouped[i] < data.size());
		data[columns_to_be_grouped[i]].AssignRowMajorStore(row_store);
		data[columns_to_be_grouped[i]].SetRowColIdx(i);
	}
}

char *DataChunk::GetRowMajorStore(idx_t col_idx) {
	D_ASSERT(col_idx < data.size());
	return data[col_idx].GetRowMajorStore();
}

void DataChunk::Reset() {
	if (data.empty()) {
		return;
	}
	if (vector_caches.size() != data.size()) {
		throw InternalException("VectorCache and column count mismatch in DataChunk::Reset");
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].ResetFromCache(vector_caches[i]);
	}
	capacity = STANDARD_VECTOR_SIZE;
	SetCardinality(0);
}

void DataChunk::Reset(idx_t capacity_) {
	if (data.empty()) {
		return;
	}
	if (vector_caches.size() != data.size()) {
		throw InternalException("VectorCache and column count mismatch in DataChunk::Reset");
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].ResetFromCache(vector_caches[i]);
	}
	capacity = capacity_;
	SetCardinality(0);
}

void DataChunk::Destroy() {
	data.clear();
	vector_caches.clear();
	capacity = 0;
	SetCardinality(0);
}

Value DataChunk::GetValue(idx_t col_idx, idx_t index) const {
	D_ASSERT(index < size());
	return data[col_idx].GetValue(index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, const Value &val) {
	data[col_idx].SetValue(index, val);
}

void DataChunk::SimpleSetValue(idx_t col_idx, idx_t index, const Value &val) {
	data[col_idx].SimpleSetValue(index, val);
}

void DataChunk::Reference(DataChunk &chunk) {
	D_ASSERT(chunk.ColumnCount() <= ColumnCount());
	SetCardinality(chunk);
	SetCapacity(chunk);
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		data[i].Reference(chunk.data[i]);
	}
	SetSchemaIdx(chunk.GetSchemaIdx());
}

void DataChunk::Move(DataChunk &chunk) {
	SetCardinality(chunk);
	SetCapacity(chunk);
	data = move(chunk.data);
	vector_caches = move(chunk.vector_caches);

	chunk.Destroy();
}

void DataChunk::Copy(DataChunk &other, idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], size(), offset, 0);
	}
	other.SetCardinality(size() - offset);
}

void DataChunk::Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count, const idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);
	D_ASSERT((offset + source_count) <= size());

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], sel, source_count, offset, 0);
	}
	other.SetCardinality(source_count - offset);
}

void DataChunk::Split(DataChunk &other, idx_t split_idx) {
	D_ASSERT(other.size() == 0);
	D_ASSERT(other.data.empty());
	D_ASSERT(split_idx < data.size());
	const idx_t num_cols = data.size();
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		other.data.push_back(move(data[col_idx]));
		other.vector_caches.push_back(move(vector_caches[col_idx]));
	}
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		data.pop_back();
		vector_caches.pop_back();
	}
	other.SetCardinality(*this);
	other.SetCapacity(*this);
}

void DataChunk::Fuse(DataChunk &other) {
	D_ASSERT(other.size() == size());
	const idx_t num_cols = other.data.size();
	for (idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		data.emplace_back(move(other.data[col_idx]));
		vector_caches.emplace_back(move(other.vector_caches[col_idx]));
	}
	other.Destroy();
}

void DataChunk::MappedFuse(DataChunk &other, const vector<uint32_t> &projection_map) {
	D_ASSERT(other.size() == size());
	const idx_t num_cols = other.data.size();

	vector<int> inverse_projection_map(other.size(), std::numeric_limits<uint32_t>::max());
	for (idx_t i = 0; i < projection_map.size(); ++i) {
		if (projection_map[i] != std::numeric_limits<uint32_t>::max()) {
			inverse_projection_map[projection_map[i]] = i;
		}
	}

	for (idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		if (inverse_projection_map[col_idx] != std::numeric_limits<uint32_t>::max()) {
			data.emplace_back(move(other.data[inverse_projection_map[col_idx]]));
			vector_caches.emplace_back(move(other.vector_caches[inverse_projection_map[col_idx]]));
		}
	}

	other.Destroy();
}

void DataChunk::Append(const DataChunk &other, bool resize, SelectionVector *sel, idx_t sel_count) {
	idx_t new_size = sel ? size() + sel_count : size() + other.size();
	if (other.size() == 0) {
		return;
	}
	if (ColumnCount() != other.ColumnCount()) {
		throw InternalException("Column counts of appending chunk doesn't match!");
	}
	if (new_size > capacity) {
		if (resize) {
			for (idx_t i = 0; i < ColumnCount(); i++) {
				data[i].Resize(size(), new_size);
			}
			capacity = new_size;
		} else {
			throw InternalException("Can't append chunk to other chunk without resizing");
		}
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		if (sel) {
			VectorOperations::Copy(other.data[i], data[i], *sel, sel_count, 0, size());
		} else {
			VectorOperations::Copy(other.data[i], data[i], other.size(), 0, size());
		}
	}
	SetCardinality(new_size);
}

void DataChunk::Normalify() {
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Normalify(size());
	}
}

vector<LogicalType> DataChunk::GetTypes() {
	vector<LogicalType> types;
	for (idx_t i = 0; i < ColumnCount(); i++) {
		types.push_back(data[i].GetType());
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(ColumnCount()) + " Columns]\n";
	for (idx_t i = 0; i < ColumnCount(); i++) {
		retval += "- " + data[i].ToString(size()) + "\n";
	}
	return retval;
}

string DataChunk::ToString(size_t size_to_print) const {
	string retval = "Chunk - [" + to_string(ColumnCount()) + " Columns]\n";
	for (idx_t i = 0; i < ColumnCount(); i++) {
		if (data[i].GetType() == LogicalType::FORWARD_ADJLIST || data[i].GetType() == LogicalType::BACKWARD_ADJLIST || data[i].GetType().id() == LogicalTypeId::LIST) continue;
		retval += "- " + data[i].ToString(size_to_print) + "\n";
	}
	return retval;
}

/*void DataChunk::Serialize(Serializer &serializer) {
	// write the count
	serializer.Write<sel_t>(size());
	serializer.Write<idx_t>(ColumnCount());
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		// write the types
		data[col_idx].GetType().Serialize(serializer);
	}
	// write the data
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		data[col_idx].Serialize(size(), serializer);
	}
}

void DataChunk::Deserialize(Deserializer &source) {
	auto rows = source.Read<sel_t>();
	idx_t column_count = source.Read<idx_t>();

	vector<LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		types.push_back(LogicalType::Deserialize(source));
	}
	Initialize(types);
	// now load the column data
	SetCardinality(rows);
	for (idx_t i = 0; i < column_count; i++) {
		data[i].Deserialize(rows, source);
	}
	Verify();
}*/

void DataChunk::Slice(const SelectionVector &sel_vector, idx_t count_p) {
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < ColumnCount(); c++) {
		data[c].Slice(sel_vector, count_p, merge_cache);
	}
}

void DataChunk::Slice(DataChunk &other, const SelectionVector &sel, idx_t count_p, idx_t col_offset) {
	D_ASSERT(other.ColumnCount() <= col_offset + ColumnCount());
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < other.ColumnCount(); c++) {
		if (other.data[c].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			// already a dictionary! merge the dictionaries
			data[col_offset + c].Reference(other.data[c]);
			data[col_offset + c].Slice(sel, count_p, merge_cache);
		} else {
			data[col_offset + c].Slice(other.data[c], sel, count_p);
		}
	}
}

unique_ptr<VectorData[]> DataChunk::Orrify() {
	auto orrified_data = unique_ptr<VectorData[]>(new VectorData[ColumnCount()]);
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		data[col_idx].Orrify(size(), orrified_data[col_idx]);
	}
	return orrified_data;
}

void DataChunk::Hash(Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::HASH);
	VectorOperations::Hash(data[0], result, size());
	for (idx_t i = 1; i < ColumnCount(); i++) {
		VectorOperations::CombineHash(result, data[i], size());
	}
}

void DataChunk::Verify() {
#ifdef DEBUG
	D_ASSERT(size() <= capacity);
	// verify that all vectors in this chunk have the chunk selection vector
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Verify(size());
	}
#endif
}

/*void DataChunk::Print() {
	Printer::Print(ToString());
}*/

} // namespace duckdb
