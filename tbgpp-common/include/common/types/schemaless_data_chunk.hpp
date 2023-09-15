//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/winapi.hpp"
#include "common/types/vector.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {
class VectorCache;

class SchemalessDataChunk : public DataChunk {
public:
	//! Creates an empty DataChunk
	DUCKDB_API SchemalessDataChunk();
	DUCKDB_API ~SchemalessDataChunk();

	void SetValidColumns(vector<bool> &valid_column_) {
		valid_column_count = 0;
		valid_column.resize(valid_column_.size());
		for (idx_t i = 0; i < valid_column_.size(); i++) {
			valid_column[i] = valid_column_[i];
			valid_column_count++;
		}
	}

public:	

private:
	vector<bool> valid_column;
	idx_t valid_column_count;

};
} // namespace duckdb
