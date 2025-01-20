#include "storage/statistics/segment_statistics.hpp"
#include "storage/statistics/numeric_statistics.hpp"
#include "storage/statistics/string_statistics.hpp"
#include "common/exception.hpp"
#include "planner/table_filter.hpp"

namespace s62 {

SegmentStatistics::SegmentStatistics(LogicalType type) : type(move(type)) {
	Reset();
}

SegmentStatistics::SegmentStatistics(LogicalType type, unique_ptr<BaseStatistics> stats)
    : type(move(type)), statistics(move(stats)) {
	if (!statistics) {
		Reset();
	}
}

void SegmentStatistics::Reset() {
	statistics = BaseStatistics::CreateEmpty(type);
	statistics->validity_stats = make_unique<ValidityStatistics>(false);
}

} // namespace s62
