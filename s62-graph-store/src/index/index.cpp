#include "index/index.hpp"

namespace s62 {

Index::Index(IndexType type, const vector<column_t> &column_ids_p, IndexConstraintType constraint_type_p)
    : type(type), column_ids(column_ids_p), constraint_type(constraint_type_p) {
	// temporary
	if (column_ids.size() == 1) {
		types.push_back(PhysicalType::UINT64);
		logical_types.push_back(LogicalType::UBIGINT);
	} else if (column_ids.size() == 2) {
		types.push_back(PhysicalType::INT128);
		logical_types.push_back(LogicalType::HUGEINT);
	} else throw InvalidInputException("");
	for (auto column_id : column_ids) {
		column_id_set.insert(column_id);
	}
}

vector<column_t> Index::GetColumnIds() {
	vector<column_t> return_column_ids;
	for (size_t i = 0; i < column_ids.size(); i++) return_column_ids.push_back(column_ids[i]);
	return return_column_ids;
}

} // namespace s62
