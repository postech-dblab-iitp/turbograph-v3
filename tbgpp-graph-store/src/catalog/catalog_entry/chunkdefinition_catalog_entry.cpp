#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/enums/graph_component_type.hpp"
#include "common/types/vector.hpp"

#include <memory>
#include <algorithm>
#include <iostream>

namespace s62 {

ChunkDefinitionCatalogEntry::ChunkDefinitionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::CHUNKDEFINITION_ENTRY, schema, catalog, info->chunkdefinition, void_alloc), min_max_array(void_alloc) {
	this->temporary = info->temporary;
	this->data_type_id = info->l_type.id();
}

void ChunkDefinitionCatalogEntry::CreateMinMaxArray(Vector &column, size_t input_size) {
	D_ASSERT(LogicalType(data_type_id).IsNumeric());

	idx_t num_entries_in_array = (num_entries_in_column + MIN_MAX_ARRAY_SIZE - 1) / MIN_MAX_ARRAY_SIZE;
	min_max_array.resize(num_entries_in_array);

	for (idx_t i = 0; i < num_entries_in_array; i++) {
		bool has_nonnull_value = false;
		idx_t start_offset = i * MIN_MAX_ARRAY_SIZE;
		idx_t end_offset = (i == num_entries_in_array - 1) ? 
							num_entries_in_column : (i + 1) * MIN_MAX_ARRAY_SIZE;
		Value min_val = Value::MaximumValue(LogicalTypeId::BIGINT);
		Value max_val = Value::MinimumValue(LogicalTypeId::BIGINT);
		for (idx_t j = start_offset; j < end_offset; j++) {
			Value val = column.GetValue(j);
			if (val.IsNull()) continue;
			Value bigint_val = val.CastAs(LogicalType::BIGINT);
			if (min_val > bigint_val) min_val = bigint_val;
			if (max_val < bigint_val) max_val = bigint_val;
			has_nonnull_value = true;
		}
		// This does automatic type conversion
		if (has_nonnull_value) {
			min_max_array[i].min = min_val.GetValue<int64_t>();
			min_max_array[i].max = max_val.GetValue<int64_t>();
		}
	}
	is_min_max_array_exist = true;
}

vector<minmax_t> ChunkDefinitionCatalogEntry::GetMinMaxArray() {
	// TODO do not copy this.. return pointer
	vector<minmax_t> minmax;
	for (auto it : min_max_array){
		minmax.push_back(it);
	}
	return minmax;
}

unique_ptr<CatalogEntry> ChunkDefinitionCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
	//auto create_info = make_unique<CreateChunkDefinitionInfo>(schema->name, name, data_type);
	//return make_unique<ChunkDefinitionCatalogEntry>(catalog, schema, create_info.get());
}

} // namespace s62
