#include "parser/constraints/unique_constraint.hpp"

#include "common/field_writer.hpp"
#include "common/limits.hpp"
#include "parser/keyword_helper.hpp"

namespace duckdb {

UniqueConstraint::UniqueConstraint(uint64_t index, bool is_primary_key)
    : Constraint(ConstraintType::UNIQUE), index(index), is_primary_key(is_primary_key) {
}
UniqueConstraint::UniqueConstraint(vector<string> columns, bool is_primary_key)
    : Constraint(ConstraintType::UNIQUE), index(DConstants::INVALID_INDEX), columns(move(columns)),
      is_primary_key(is_primary_key) {
}

string UniqueConstraint::ToString() const {
	string base = is_primary_key ? "PRIMARY KEY(" : "UNIQUE(";
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			base += ", ";
		}
		base += KeywordHelper::WriteOptionallyQuoted(columns[i]);
	}
	return base + ")";
}

unique_ptr<Constraint> UniqueConstraint::Copy() const {
	if (index == DConstants::INVALID_INDEX) {
		return make_unique<UniqueConstraint>(columns, is_primary_key);
	} else {
		auto result = make_unique<UniqueConstraint>(index, is_primary_key);
		result->columns = columns;
		return move(result);
	}
}

void UniqueConstraint::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(is_primary_key);
	writer.WriteField<uint64_t>(index);
	D_ASSERT(columns.size() <= NumericLimits<uint32_t>::Maximum());
	writer.WriteList<string>(columns);
}

unique_ptr<Constraint> UniqueConstraint::Deserialize(FieldReader &source) {
	auto is_primary_key = source.ReadRequired<bool>();
	auto index = source.ReadRequired<uint64_t>();
	auto columns = source.ReadRequiredList<string>();

	if (index != DConstants::INVALID_INDEX) {
		// single column parsed constraint
		auto result = make_unique<UniqueConstraint>(index, is_primary_key);
		result->columns = move(columns);
		return move(result);
	} else {
		// column list parsed constraint
		return make_unique<UniqueConstraint>(move(columns), is_primary_key);
	}
}

} // namespace duckdb
