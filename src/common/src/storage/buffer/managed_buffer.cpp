#include "storage/buffer/managed_buffer.hpp"

#include "common/allocator.hpp"
#include "common/exception.hpp"
// #include "main/config.hpp"

namespace duckdb {

ManagedBuffer::ManagedBuffer(DatabaseInstance &db, idx_t size, bool can_destroy, block_id_t id)
    // : FileBuffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER, size), db(db), can_destroy(can_destroy), id(id) {
	: FileBuffer(DEFAULT_ALLOCATOR, FileBufferType::MANAGED_BUFFER, size), db(db), can_destroy(can_destroy), id(id) {
	D_ASSERT(id >= MAXIMUM_BLOCK);
	D_ASSERT(size >= Storage::BLOCK_SIZE);
}

} // namespace duckdb
