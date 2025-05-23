#include "storage/statistics/validity_statistics.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/field_writer.hpp"
#include "common/exception.hpp"

namespace duckdb {

ValidityStatistics::ValidityStatistics(bool has_null, bool has_no_null)
    : BaseStatistics(LogicalType(LogicalTypeId::VALIDITY)), has_null(has_null), has_no_null(has_no_null) {
}

unique_ptr<BaseStatistics> ValidityStatistics::Combine(const unique_ptr<BaseStatistics> &lstats,
                                                       const unique_ptr<BaseStatistics> &rstats) {
	if (!lstats && !rstats) {
		return nullptr;
	} else if (!lstats) {
		return rstats->Copy();
	} else if (!rstats) {
		return lstats->Copy();
	} else {
		auto &l = (ValidityStatistics &)*lstats;
		auto &r = (ValidityStatistics &)*rstats;
		return make_unique<ValidityStatistics>(l.has_null || r.has_null, l.has_no_null || r.has_no_null);
	}
}

bool ValidityStatistics::IsConstant() const {
	if (!has_null) {
		return true;
	}
	if (!has_no_null) {
		return true;
	}
	return false;
}

void ValidityStatistics::Merge(const BaseStatistics &other_p) {
	auto &other = (ValidityStatistics &)other_p;
	has_null = has_null || other.has_null;
	has_no_null = has_no_null || other.has_no_null;
}

unique_ptr<BaseStatistics> ValidityStatistics::Copy() const {
	return make_unique<ValidityStatistics>(has_null, has_no_null);
}

void ValidityStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(has_null);
	writer.WriteField<bool>(has_no_null);
}

unique_ptr<BaseStatistics> ValidityStatistics::Deserialize(FieldReader &reader) {
	bool has_null = reader.ReadRequired<bool>();
	bool has_no_null = reader.ReadRequired<bool>();
	return make_unique<ValidityStatistics>(has_null, has_no_null);
}

void ValidityStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	if (has_null && has_no_null) {
		// nothing to verify
		return;
	}
	VectorData vdata;
	vector.Orrify(count, vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		bool row_is_valid = vdata.validity.RowIsValid(index);
		if (row_is_valid && !has_no_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as having only NULL values, but vector contains valid values: %s",
			    vector.ToString(count));
		}
		if (!row_is_valid && !has_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
			    vector.ToString(count));
		}
	}
}

string ValidityStatistics::ToString() const {
	return has_null ? "[Has Null: true]" : "[Has Null: false]";
}

} // namespace duckdb
