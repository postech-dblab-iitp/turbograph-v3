#include "storage/statistics/struct_statistics.hpp"
#include "common/types/vector.hpp"
#include "common/field_writer.hpp"

namespace duckdb {

StructStatistics::StructStatistics(LogicalType type_p) : BaseStatistics(move(type_p)) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);

	auto &child_types = StructType::GetChildTypes(type);
	child_stats.resize(child_types.size());
	for (idx_t i = 0; i < child_types.size(); i++) {
		child_stats[i] = BaseStatistics::CreateEmpty(child_types[i].second);
	}
	validity_stats = make_unique<ValidityStatistics>(false);
}

void StructStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);

	auto &other = (const StructStatistics &)other_p;
	D_ASSERT(other.child_stats.size() == child_stats.size());
	for (idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i] && other.child_stats[i]) {
			child_stats[i]->Merge(*other.child_stats[i]);
		} else {
			child_stats[i].reset();
		}
	}
}

// LCOV_EXCL_START
FilterPropagateResult StructStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	throw InternalException("Struct zonemaps are not supported yet");
}
// LCOV_EXCL_STOP

unique_ptr<BaseStatistics> StructStatistics::Copy() const {
	auto copy = make_unique<StructStatistics>(type);
	if (validity_stats) {
		copy->validity_stats = validity_stats->Copy();
	}
	for (idx_t i = 0; i < child_stats.size(); i++) {
		copy->child_stats[i] = child_stats[i] ? child_stats[i]->Copy() : nullptr;
	}
	return move(copy);
}

void StructStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteField<uint32_t>(child_stats.size());
	auto &serializer = writer.GetSerializer();
	for (idx_t i = 0; i < child_stats.size(); i++) {
		serializer.Write<bool>(child_stats[i] ? true : false);
		if (child_stats[i]) {
			child_stats[i]->Serialize(serializer);
		}
	}
}

unique_ptr<BaseStatistics> StructStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto result = make_unique<StructStatistics>(move(type));
	auto &child_types = StructType::GetChildTypes(result->type);

	auto child_type_count = reader.ReadRequired<uint32_t>();
	if (child_types.size() != child_type_count) {
		throw InternalException("Struct stats deserialization failure: child count does not match type count!");
	}
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto has_child = source.Read<bool>();
		if (has_child) {
			result->child_stats[i] = BaseStatistics::Deserialize(source, child_types[i].second);
		} else {
			result->child_stats[i].reset();
		}
	}
	return move(result);
}

string StructStatistics::ToString() const {
	string result;
	result += " {";
	auto &child_types = StructType::GetChildTypes(type);
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += child_types[i].first + ": " + (child_stats[i] ? child_stats[i]->ToString() : "No Stats");
	}
	result += "}";
	result += validity_stats ? validity_stats->ToString() : "";
	return result;
}

void StructStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		if (child_stats[i]) {
			child_stats[i]->Verify(*child_entries[i], sel, count);
		}
	}
}

} // namespace duckdb
