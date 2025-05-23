//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/selection_vector.hpp"
#include "common/types/string_heap.hpp"
#include "common/types/string_type.hpp"

namespace duckdb {

class BufferHandle;
class VectorBuffer;
class Vector;
class ChunkCollection;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER,     // standard buffer, holds a single array of data
	DICTIONARY_BUFFER,   // dictionary buffer, holds a selection vector
	VECTOR_CHILD_BUFFER, // vector child buffer: holds another vector
	STRING_BUFFER,       // string buffer, holds a string heap
	STRUCT_BUFFER,       // struct buffer, holds a ordered mapping from name to child vector
	LIST_BUFFER,         // list buffer, holds a single flatvector child
	MANAGED_BUFFER,      // managed buffer, holds a buffer managed by the buffermanager
	OPAQUE_BUFFER,       // opaque buffer, can be created for example by the parquet reader
	ROWSTORE_BUFFER      // s62 row-major buffer, holds a row-major store
};

enum class VectorAuxiliaryDataType : uint8_t {
	ARROW_AUXILIARY // Holds Arrow Chunks that this vector depends on
};

struct VectorAuxiliaryData {
	explicit VectorAuxiliaryData(VectorAuxiliaryDataType type_p)
	    : type(type_p) {

	      };
	VectorAuxiliaryDataType type;

	virtual ~VectorAuxiliaryData() {
	}
};

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	explicit VectorBuffer(VectorBufferType type) : buffer_type(type) {
	}
	explicit VectorBuffer(idx_t data_size) : buffer_type(VectorBufferType::STANDARD_BUFFER) {
		if (data_size > 0) {
			data = unique_ptr<data_t[]>(new data_t[data_size]);
		}
	}
	explicit VectorBuffer(unique_ptr<data_t[]> data_p)
	    : buffer_type(VectorBufferType::STANDARD_BUFFER), data(move(data_p)) {
	}
	virtual ~VectorBuffer() {
	}
	VectorBuffer() {
	}

public:
	data_ptr_t GetData() {
		return data.get();
	}

	void SetData(unique_ptr<data_t[]> new_data) {
		data = move(new_data);
	}

	VectorAuxiliaryData *GetAuxiliaryData() {
		return aux_data.get();
	}

	void SetAuxiliaryData(unique_ptr<VectorAuxiliaryData> aux_data_p) {
		aux_data = move(aux_data_p);
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(PhysicalType type, idx_t capacity = STANDARD_VECTOR_SIZE);
	static buffer_ptr<VectorBuffer> CreateConstantVector(PhysicalType type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(const LogicalType &logical_type);
	static buffer_ptr<VectorBuffer> CreateStandardVector(const LogicalType &logical_type,
	                                                     idx_t capacity = STANDARD_VECTOR_SIZE);

	inline VectorBufferType GetBufferType() const {
		return buffer_type;
	}

	inline VectorAuxiliaryDataType GetAuxiliaryDataType() const {
		return aux_data->type;
	}

protected:
	VectorBufferType buffer_type;
	unique_ptr<VectorAuxiliaryData> aux_data;
	unique_ptr<data_t[]> data;
};

//! The DictionaryBuffer holds a selection vector
class DictionaryBuffer : public VectorBuffer {
public:
	explicit DictionaryBuffer(const SelectionVector &sel)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel) {
	}
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(move(data)) {
	}
	explicit DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(count) {
	}

public:
	const SelectionVector &GetSelVector() const {
		return sel_vector;
	}
	SelectionVector &GetSelVector() {
		return sel_vector;
	}
	void SetSelVector(const SelectionVector &vector) {
		this->sel_vector.Initialize(vector);
	}

private:
	SelectionVector sel_vector;
};

class VectorStringBuffer : public VectorBuffer {
public:
	VectorStringBuffer();

public:
	string_t AddString(const char *data, idx_t len) {
		return heap.AddString(data, len);
	}
	string_t AddString(string_t data) {
		return heap.AddString(data);
	}
	string_t AddBlob(string_t data) {
		return heap.AddBlob(data.GetDataUnsafe(), data.GetSize());
	}
	string_t EmptyString(idx_t len) {
		return heap.EmptyString(len);
	}

	void AddHeapReference(buffer_ptr<VectorBuffer> heap) {
		references.push_back(move(heap));
	}

private:
	//! The string heap of this buffer
	StringHeap heap;
	// References to additional vector buffers referenced by this string buffer
	vector<buffer_ptr<VectorBuffer>> references;
};

class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	VectorStructBuffer(const LogicalType &struct_type, idx_t capacity = STANDARD_VECTOR_SIZE);
	~VectorStructBuffer() override;

public:
	const vector<unique_ptr<Vector>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<Vector>> &GetChildren() {
		return children;
	}

private:
	//! child vectors used for nested data
	vector<unique_ptr<Vector>> children;
};

class VectorListBuffer : public VectorBuffer {
public:
	VectorListBuffer(unique_ptr<Vector> vector, idx_t initial_capacity = STANDARD_VECTOR_SIZE);
	VectorListBuffer(const LogicalType &list_type, idx_t initial_capacity = STANDARD_VECTOR_SIZE);
	~VectorListBuffer() override;

public:
	Vector &GetChild() {
		return *child;
	}
	void Reserve(idx_t to_reserve);

	void Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset = 0);
	void Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size, idx_t source_offset = 0);

	void PushBack(const Value &insert);

	idx_t capacity = 0;
	idx_t size = 0;

private:
	//! child vectors used for nested data
	unique_ptr<Vector> child;
};

class VectorRowStoreBuffer : public VectorBuffer {
public:
	VectorRowStoreBuffer();

public:
	void Reserve(idx_t to_reserve);
	char *GetRowData();

private:
	unique_ptr<data_t[]> row_data;
};

//! The ManagedVectorBuffer holds a buffer handle
/*class ManagedVectorBuffer : public VectorBuffer {
public:
	explicit ManagedVectorBuffer(unique_ptr<BufferHandle> handle);
	~ManagedVectorBuffer() override;

private:
	unique_ptr<BufferHandle> handle;
};*/

} // namespace duckdb
