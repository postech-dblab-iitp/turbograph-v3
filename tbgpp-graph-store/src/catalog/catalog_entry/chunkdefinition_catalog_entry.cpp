#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/enums/graph_component_type.hpp"
#include "common/types/vector.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

ChunkDefinitionCatalogEntry::ChunkDefinitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::EXTENT_ENTRY, schema, catalog, info->chunkdefinition) {
	this->temporary = info->temporary;
	this->data_type = info->type;
}

void ChunkDefinitionCatalogEntry::CreateMinMaxArray(Vector &column, size_t input_size) {
	idx_t num_entries_in_column = input_size;
	idx_t num_entries_in_array = (num_entries_in_column + MIN_MAX_ARRAY_SIZE - 1) / MIN_MAX_ARRAY_SIZE;
	min_max_array.resize(num_entries_in_array);

	for (idx_t i = 0; i < num_entries_in_array; i++) {
		idx_t start_offset = i * MIN_MAX_ARRAY_SIZE;
		idx_t end_offset = (i == num_entries_in_array - 1) ? 
							num_entries_in_column : (i + 1) * MIN_MAX_ARRAY_SIZE;
		Value min_val = Value::MinimumValue(data_type);
		Value max_val = Value::MaximumValue(data_type);
		for (idx_t j = start_offset; j < end_offset; j++) {
			Value val = column.GetValue(j);
			if (min_val > val) min_val = val;
			if (max_val < val) max_val = val;
		}
		min_max_array[i].min = min_val.GetValue<idx_t>();
		min_max_array[i].max = max_val.GetValue<idx_t>();
	}
	is_min_max_array_exist = true;
}

unique_ptr<CatalogEntry> ChunkDefinitionCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreateChunkDefinitionInfo>(schema->name, name, data_type);
	//return make_unique<ChunkDefinitionCatalogEntry>(catalog, schema, create_info.get());
}

} // namespace duckdb
