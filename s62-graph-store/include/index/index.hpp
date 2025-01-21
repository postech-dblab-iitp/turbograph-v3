//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/mutex.hpp"
#include "common/unordered_set.hpp"
#include "common/enums/index_type.hpp"
#include "common/types/data_chunk.hpp"

namespace s62 {

class ClientContext;
class Transaction;

struct IndexLock;

enum IndexConstraintType : uint8_t {
	NONE = 0,    // index is an index don't built to any constraint
	UNIQUE = 1,  // index is an index built to enforce a UNIQUE constraint
	PRIMARY = 2, // index is an index built to enforce a PRIMARY KEY constraint
	FOREIGN = 3  // index is an index built to enforce a FOREIGN KEY constraint
};

struct IndexLock {
	unique_lock<mutex> index_lock;
};

//! The index is an abstract base class that serves as the basis for indexes
class Index {
public:
	Index(IndexType type, const vector<column_t> &column_ids, IndexConstraintType constraint_type);
	virtual ~Index() = default;

	//! The type of the index
	IndexType type;
	//! Column identifiers to extract from the base table
	vector<column_t> column_ids;
	//! unordered_set of column_ids used by the index
	unordered_set<column_t> column_id_set;
	//! The physical types stored in the index
	vector<PhysicalType> types;
	//! The logical types of the expressions
	vector<LogicalType> logical_types;
	// ! constraint type
	IndexConstraintType constraint_type;

public:

	vector<column_t> GetColumnIds();

	//! Insert data into the index. Does not lock the index.
	virtual bool Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) = 0;

	//! Returns unique flag
	bool IsUnique() {
		return (constraint_type == IndexConstraintType::UNIQUE || constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns primary flag
	bool IsPrimary() {
		return (constraint_type == IndexConstraintType::PRIMARY);
	}
	//! Returns foreign flag
	bool IsForeign() {
		return (constraint_type == IndexConstraintType::FOREIGN);
	}

protected:
	void ExecuteExpressions(DataChunk &input, DataChunk &result);

	//! Lock used for updating the index
	mutex lock;
};

} // namespace s62
