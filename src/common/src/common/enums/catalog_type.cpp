#include "common/enums/catalog_type.hpp"

#include "common/exception.hpp"

namespace duckdb {

// LCOV_EXCL_START
string CatalogTypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::COLLATION_ENTRY:
		return "Collation";
	case CatalogType::TYPE_ENTRY:
		return "Type";
	case CatalogType::TABLE_ENTRY:
		return "Table";
	case CatalogType::SCHEMA_ENTRY:
		return "Schema";
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return "Table Function";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "Scalar Function";
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		return "Aggregate Function";
	case CatalogType::COPY_FUNCTION_ENTRY:
		return "Copy Function";
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
		return "Pragma Function";
	case CatalogType::MACRO_ENTRY:
		return "Macro Function";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "Table Macro Function";
	case CatalogType::VIEW_ENTRY:
		return "View";
	case CatalogType::INDEX_ENTRY:
		return "Index";
	case CatalogType::PREPARED_STATEMENT:
		return "Prepared Statement";
	case CatalogType::SEQUENCE_ENTRY:
		return "Sequence";
	case CatalogType::GRAPH_ENTRY:
		return "Graph";
	case CatalogType::PARTITION_ENTRY:
		return "Partition";
	case CatalogType::PROPERTY_SCHEMA_ENTRY:
		return "Property Schema";
	case CatalogType::EXTENT_ENTRY:
		return "Extent";
	case CatalogType::CHUNKDEFINITION_ENTRY:
		return "Chunk Definition";
	case CatalogType::INVALID:
	case CatalogType::DELETED_ENTRY:
	case CatalogType::UPDATED_ENTRY:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

} // namespace duckdb
