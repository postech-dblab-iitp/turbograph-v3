#include "storage/block.hpp"
#include "common/assert.hpp"

namespace s62 {

Block::Block(Allocator &allocator, block_id_t id)
    : FileBuffer(allocator, FileBufferType::BLOCK, Storage::BLOCK_ALLOC_SIZE), id(id) {
}

Block::Block(FileBuffer &source, block_id_t id) : FileBuffer(source, FileBufferType::BLOCK), id(id) {
	D_ASSERT(GetMallocedSize() == Storage::BLOCK_ALLOC_SIZE);
	D_ASSERT(size == Storage::BLOCK_SIZE);
}

} // namespace s62
