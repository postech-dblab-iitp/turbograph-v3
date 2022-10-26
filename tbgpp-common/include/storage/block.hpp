//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/storage_info.hpp"
#include "common/file_buffer.hpp"

namespace duckdb {

class Block : public FileBuffer {
public:
	Block(Allocator &allocator, block_id_t id);
	Block(FileBuffer &source, block_id_t id);

	block_id_t id;
};

struct BlockPointer {
	block_id_t block_id;
	uint32_t offset;
};

} // namespace duckdb
