#pragma once

#include "catalog/standard_entry.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "common/boost_typedefs.hpp"

namespace s62 {

class Index;

//! An index catalog entry
class IndexCatalogEntry : public StandardEntry {
public:
	IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info, const void_allocator &void_alloc);
	~IndexCatalogEntry() override;

	IndexType index_type;
	Index *index;
	idx_t pid; // oid of the partition to which this index belongs
	idx_t psid; // oid of the segment to which this index belongs
	int64_t_vector index_key_columns;
	idx_t adj_col_idx;

public:
	string ToSQL() override;
	idx_t GetPartitionID();
	idx_t GetPropertySchemaID();
	int64_t_vector *GetIndexKeyColumns();
	IndexType GetIndexType();
	idx_t GetAdjColIdx();
};

} // namespace s62
