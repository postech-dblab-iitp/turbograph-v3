#include "common/types/vector.hpp"
#include "common/types/vector_buffer.hpp"
//#include "common/types/chunk_collection.hpp"
//#include "storage/buffer/buffer_handle.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include "common/assert.hpp"
#include "icecream.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type, idx_t capacity) {
	return make_buffer<VectorBuffer>(capacity * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(const LogicalType &type) {
	return VectorBuffer::CreateConstantVector(type.InternalType());
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(const LogicalType &type, idx_t capacity) {
	return VectorBuffer::CreateStandardVector(type.InternalType(), capacity);
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer(const LogicalType &type, idx_t capacity)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_type : child_types) {
		auto vector = make_unique<Vector>(child_type.second, capacity);
		children.push_back(move(vector));
	}
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer(unique_ptr<Vector> vector, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), capacity(initial_capacity), child(move(vector)) {
}

VectorListBuffer::VectorListBuffer(const LogicalType &list_type, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER) {
	// FIXME: directly construct vector of correct size
	child = make_unique<Vector>(ListType::GetChildType(list_type));
	capacity = STANDARD_VECTOR_SIZE;
	Reserve(initial_capacity);
}

void VectorListBuffer::Reserve(idx_t to_reserve) {
	if (to_reserve > capacity) {
		idx_t new_capacity = (to_reserve + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE;
		D_ASSERT(new_capacity >= to_reserve);
		D_ASSERT(new_capacity % STANDARD_VECTOR_SIZE == 0);
		child->Resize(capacity, new_capacity);
		capacity = new_capacity;
	}
}

void VectorListBuffer::Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size,
                              idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, sel, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(const Value &insert) {
	if (size + 1 > capacity) {
		child->Resize(capacity, capacity * 2);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

VectorListBuffer::~VectorListBuffer() {
}

VectorRowStoreBuffer::VectorRowStoreBuffer() : VectorBuffer(VectorBufferType::ROWSTORE_BUFFER) {
}

void VectorRowStoreBuffer::Reserve(idx_t to_reserve) {
	row_data = make_unique<data_t[]>(to_reserve);
}

char *VectorRowStoreBuffer::GetRowData() {
	return (char *)row_data.get();
}

/*ManagedVectorBuffer::ManagedVectorBuffer(unique_ptr<BufferHandle> handle)
    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), handle(move(handle)) {
}

ManagedVectorBuffer::~ManagedVectorBuffer() {
}*/

} // namespace duckdb
