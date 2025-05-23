#include "storage/statistics/numeric_statistics.hpp"
#include "common/types/vector.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/field_writer.hpp"

namespace duckdb {

template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value) {
}

template <>
void NumericStatistics::Update<list_entry_t>(SegmentStatistics &stats, list_entry_t new_value) {
}

NumericStatistics::NumericStatistics(LogicalType type_p) : BaseStatistics(move(type_p)) {
	min = Value::MaximumValue(type);
	max = Value::MinimumValue(type);
	validity_stats = make_unique<ValidityStatistics>(false);
}

NumericStatistics::NumericStatistics(LogicalType type_p, Value min_p, Value max_p)
    : BaseStatistics(move(type_p)), min(move(min_p)), max(move(max_p)) {
}

void NumericStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const NumericStatistics &)other_p;
	if (other.min.IsNull() || min.IsNull()) {
		min = Value(type);
	} else if (other.min < min) {
		min = other.min;
	}
	if (other.max.IsNull() || max.IsNull()) {
		max = Value(type);
	} else if (other.max > max) {
		max = other.max;
	}
}

FilterPropagateResult NumericStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	if (constant.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (min.IsNull() || max.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (constant == min && constant == max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (constant >= min && constant <= max) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
		if (constant < min || constant > max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min == max && min == constant) {
			// corner case of a cluster with one numeric equal to the target constant
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// X >= C
		// this can be true only if max(X) >= C
		// if min(X) >= C, then this is always true
		if (min >= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (max >= constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_GREATERTHAN:
		// X > C
		// this can be true only if max(X) > C
		// if min(X) > C, then this is always true
		if (min > constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (max > constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// X <= C
		// this can be true only if min(X) <= C
		// if max(X) <= C, then this is always true
		if (max <= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min <= constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
		// X < C
		// this can be true only if min(X) < C
		// if max(X) < C, then this is always true
		if (max < constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min < constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type in zonemap check not implemented");
	}
}

unique_ptr<BaseStatistics> NumericStatistics::Copy() const {
	auto stats = make_unique<NumericStatistics>(type, min, max);
	if (validity_stats) {
		stats->validity_stats = validity_stats->Copy();
	}
	return move(stats);
}

bool NumericStatistics::IsConstant() const {
	return max <= min;
}

void NumericStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(min);
	writer.WriteSerializable(max);
}

// unique_ptr<BaseStatistics> NumericStatistics::Deserialize(FieldReader &reader, LogicalType type) {
// 	auto min = reader.ReadRequiredSerializable<Value, Value>();
// 	auto max = reader.ReadRequiredSerializable<Value, Value>();
// 	return make_unique_base<BaseStatistics, NumericStatistics>(move(type), min, max);
// }

string NumericStatistics::ToString() const {
	return StringUtil::Format("[Min: %s, Max: %s]%s", min.ToString(), max.ToString(),
	                          validity_stats ? validity_stats->ToString() : "");
}

template <class T>
void NumericStatistics::TemplatedVerify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	VectorData vdata;
	vector.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		if (!min.IsNull() && LessThan::Operation(data[index], min.GetValueUnsafe<T>())) { // LCOV_EXCL_START
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		} // LCOV_EXCL_STOP
		if (!max.IsNull() && GreaterThan::Operation(data[index], max.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
	}
}

void NumericStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		break;
	case PhysicalType::INT8:
		TemplatedVerify<int8_t>(vector, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedVerify<int16_t>(vector, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedVerify<int32_t>(vector, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedVerify<int64_t>(vector, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedVerify<uint8_t>(vector, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedVerify<uint16_t>(vector, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedVerify<uint32_t>(vector, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedVerify<uint64_t>(vector, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedVerify<hugeint_t>(vector, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedVerify<float>(vector, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedVerify<double>(vector, sel, count);
		break;
	default:
		throw InternalException("Unsupported type %s for numeric statistics verify", type.ToString());
	}
}

} // namespace duckdb
