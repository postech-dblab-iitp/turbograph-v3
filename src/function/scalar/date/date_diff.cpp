#include "function/scalar/date_functions.hpp"
#include "common/enums/date_part_specifier.hpp"
#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/interval.hpp"
#include "common/types/time.hpp"
#include "common/types/timestamp.hpp"
#include "common/vector_operations/ternary_executor.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/string_util.hpp"
#include "storage/statistics/numeric_statistics.hpp"

namespace duckdb {

// This function is an implementation of the "period-crossing" date difference function from T-SQL
// https://docs.microsoft.com/en-us/sql/t-sql/functions/datediff-transact-sql?view=sql-server-ver15
struct DateDiff {
	struct YearOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) - Date::ExtractYear(startdate);
		}
	};

	struct MonthOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			int32_t start_year, start_month, start_day;
			Date::Convert(startdate, start_year, start_month, start_day);
			int32_t end_year, end_month, end_day;
			Date::Convert(enddate, end_year, end_month, end_day);

			return (end_year * 12 + end_month - 1) - (start_year * 12 + start_month - 1);
		}
	};

	struct DayOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::EpochDays(enddate) - Date::EpochDays(startdate);
		}
	};

	struct DecadeOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) / 10 - Date::ExtractYear(startdate) / 10;
		}
	};

	struct CenturyOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) / 100 - Date::ExtractYear(startdate) / 100;
		}
	};

	struct MilleniumOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) / 1000 - Date::ExtractYear(startdate) / 1000;
		}
	};

	struct QuarterOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			int32_t start_year, start_month, start_day;
			Date::Convert(startdate, start_year, start_month, start_day);
			int32_t end_year, end_month, end_day;
			Date::Convert(enddate, end_year, end_month, end_day);

			return (end_year * 12 + end_month - 1) / Interval::MONTHS_PER_QUARTER -
			       (start_year * 12 + start_month - 1) / Interval::MONTHS_PER_QUARTER;
		}
	};

	struct WeekOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) / Interval::SECS_PER_WEEK - Date::Epoch(startdate) / Interval::SECS_PER_WEEK;
		}
	};

	struct ISOYearOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractISOYearNumber(enddate) - Date::ExtractISOYearNumber(startdate);
		}
	};

	struct MicrosecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::EpochNanoseconds(enddate) / Interval::NANOS_PER_MICRO -
			       Date::EpochNanoseconds(startdate) / Interval::NANOS_PER_MICRO;
		}
	};

	struct MillisecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::EpochNanoseconds(enddate) / Interval::NANOS_PER_MSEC -
			       Date::EpochNanoseconds(startdate) / Interval::NANOS_PER_MSEC;
		}
	};

	struct SecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) - Date::Epoch(startdate);
		}
	};

	struct MinutesOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) / Interval::SECS_PER_MINUTE -
			       Date::Epoch(startdate) / Interval::SECS_PER_MINUTE;
		}
	};

	struct HoursOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) / Interval::SECS_PER_HOUR - Date::Epoch(startdate) / Interval::SECS_PER_HOUR;
		}
	};
};

// TIMESTAMP specialisations
template <>
int64_t DateDiff::YearOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return YearOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate), Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::MonthOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return MonthOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                         Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::DayOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return DayOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate), Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::DecadeOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return DecadeOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                          Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::CenturyOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return CenturyOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                           Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::MilleniumOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return MilleniumOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                             Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::QuarterOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return QuarterOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                           Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::WeekOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return WeekOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate), Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::ISOYearOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return ISOYearOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                           Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::MicrosecondsOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochMicroSeconds(enddate) - Timestamp::GetEpochMicroSeconds(startdate);
}

template <>
int64_t DateDiff::MillisecondsOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochMs(enddate) - Timestamp::GetEpochMs(startdate);
}

template <>
int64_t DateDiff::SecondsOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochSeconds(enddate) - Timestamp::GetEpochSeconds(startdate);
}

template <>
int64_t DateDiff::MinutesOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochSeconds(enddate) / Interval::SECS_PER_MINUTE -
	       Timestamp::GetEpochSeconds(startdate) / Interval::SECS_PER_MINUTE;
}

template <>
int64_t DateDiff::HoursOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochSeconds(enddate) / Interval::SECS_PER_HOUR -
	       Timestamp::GetEpochSeconds(startdate) / Interval::SECS_PER_HOUR;
}

// TIME specialisations
template <>
int64_t DateDiff::YearOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DateDiff::MonthOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DateDiff::DayOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DateDiff::DecadeOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DateDiff::CenturyOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DateDiff::MilleniumOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DateDiff::QuarterOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DateDiff::WeekOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DateDiff::ISOYearOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"isoyear\" not recognized");
}

template <>
int64_t DateDiff::MicrosecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros - startdate.micros;
}

template <>
int64_t DateDiff::MillisecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_MSEC - startdate.micros / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DateDiff::SecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_SEC - startdate.micros / Interval::MICROS_PER_SEC;
}

template <>
int64_t DateDiff::MinutesOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_MINUTE - startdate.micros / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DateDiff::HoursOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_HOUR - startdate.micros / Interval::MICROS_PER_HOUR;
}

template <typename TA, typename TB, typename TR>
static int64_t DifferenceDates(DatePartSpecifier type, TA startdate, TB enddate) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return DateDiff::YearOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MONTH:
		return DateDiff::MonthOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
		return DateDiff::DayOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DECADE:
		return DateDiff::DecadeOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::CENTURY:
		return DateDiff::CenturyOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLENNIUM:
		return DateDiff::MilleniumOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::QUARTER:
		return DateDiff::QuarterOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return DateDiff::WeekOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::ISOYEAR:
		return DateDiff::ISOYearOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MICROSECONDS:
		return DateDiff::MicrosecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLISECONDS:
		return DateDiff::MillisecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return DateDiff::SecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MINUTE:
		return DateDiff::MinutesOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::HOUR:
		return DateDiff::HoursOperator::template Operation<TA, TB, TR>(startdate, enddate);
	default:
		throw NotImplementedException("Specifier type not implemented for DATEDIFF");
	}
}

struct DateDiffTernaryOperator {
	template <typename TS, typename TA, typename TB, typename TR>
	static inline TR Operation(TS part, TA startdate, TB enddate) {
		return DifferenceDates<TA, TB, TR>(GetDatePartSpecifier(part.GetString()), startdate, enddate);
	}
};

template <typename TA, typename TB, typename TR>
static void DateDiffBinaryExecutor(DatePartSpecifier type, Vector &left, Vector &right, Vector &result, idx_t count) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::YearOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MONTH:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::MonthOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::DayOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DECADE:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::DecadeOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::CenturyOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLENNIUM:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::MilleniumOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::QuarterOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::WeekOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::ISOYEAR:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::ISOYearOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::MicrosecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::MillisecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::SecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::MinutesOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::HOUR:
		BinaryExecutor::ExecuteStandard<TA, TB, TR, DateDiff::HoursOperator>(left, right, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATEDIFF");
	}
}

template <typename T>
static void DateDiffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	auto &part_arg = args.data[0];
	auto &startdate_arg = args.data[1];
	auto &enddate_arg = args.data[2];

	if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Common case of constant part.
		if (ConstantVector::IsNull(part_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			const auto type = GetDatePartSpecifier(ConstantVector::GetData<string_t>(part_arg)->GetString());
			DateDiffBinaryExecutor<T, T, int64_t>(type, startdate_arg, enddate_arg, result, args.size());
		}
	} else {
		TernaryExecutor::Execute<string_t, T, T, int64_t>(part_arg, startdate_arg, enddate_arg, result, args.size(),
		                                                  DateDiffTernaryOperator::Operation<string_t, T, T, int64_t>);
	}
}

void DateDiffFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet date_diff("date_diff");
	date_diff.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
	                                     LogicalType::BIGINT, DateDiffFunction<date_t>));
	date_diff.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                     LogicalType::BIGINT, DateDiffFunction<timestamp_t>));
	date_diff.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME, LogicalType::TIME},
	                                     LogicalType::BIGINT, DateDiffFunction<dtime_t>));
	set.AddFunction(date_diff);

	date_diff.name = "datediff";
	set.AddFunction(date_diff);
}

} // namespace duckdb
