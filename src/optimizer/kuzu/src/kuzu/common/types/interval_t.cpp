#include "kuzu/common/types/interval_t.h"

#include "kuzu/common/exception.h"
#include "kuzu/common/types/cast_helpers.h"
#include "kuzu/common/types/timestamp_t.h"
#include "kuzu/common/utils.h"

namespace kuzu {
namespace common {

bool interval_t::operator>(const interval_t& rhs) const {
    return Interval::GreaterThan(*this, rhs);
}

interval_t interval_t::operator+(const interval_t& rhs) const {
    interval_t result{};
    result.months = months + rhs.months;
    result.days = days + rhs.days;
    result.micros = micros + rhs.micros;
    return result;
}

timestamp_t interval_t::operator+(const timestamp_t& rhs) const {
    return rhs + *this;
}

date_t interval_t::operator+(const date_t& rhs) const {
    return rhs + *this;
}

interval_t interval_t::operator-(const interval_t& rhs) const {
    interval_t result{};
    result.months = months - rhs.months;
    result.days = days - rhs.days;
    result.micros = micros - rhs.micros;
    return result;
}

interval_t interval_t::operator/(const uint64_t& rhs) const {
    interval_t result{};
    int32_t monthsRemainder = months % rhs;
    int32_t daysRemainder = (days + monthsRemainder * Interval::DAYS_PER_MONTH) % rhs;
    result.months = months / rhs;
    result.days = (days + monthsRemainder * Interval::DAYS_PER_MONTH) / rhs;
    result.micros = (micros + daysRemainder * Interval::MICROS_PER_DAY) / rhs;
    return result;
}

void Interval::addition(interval_t& result, uint64_t number, string specifierStr) {
    StringUtils::toLower(specifierStr);
    if (specifierStr == "year" || specifierStr == "years" || specifierStr == "y") {
        result.months += number * MONTHS_PER_YEAR;
    } else if (specifierStr == "month" || specifierStr == "months" || specifierStr == "mon") {
        result.months += number;
    } else if (specifierStr == "day" || specifierStr == "days" || specifierStr == "d") {
        result.days += number;
    } else if (specifierStr == "hour" || specifierStr == "hours" || specifierStr == "h") {
        result.micros += number * MICROS_PER_HOUR;
    } else if (specifierStr == "minute" || specifierStr == "minutes" || specifierStr == "m") {
        result.micros += number * MICROS_PER_MINUTE;
    } else if (specifierStr == "second" || specifierStr == "seconds" || specifierStr == "s") {
        result.micros += number * MICROS_PER_SEC;
    } else if (specifierStr == "millisecond" || specifierStr == "milliseconds" ||
               specifierStr == "ms" || specifierStr == "msec") {
        result.micros += number * MICROS_PER_MSEC;
    } else if (specifierStr == "microsecond" || specifierStr == "microseconds" ||
               specifierStr == "us") {
        result.micros += number;
    } else {
        throw ConversionException("Unrecognized interval specifier string: " + specifierStr + ".");
    }
}

void Interval::parseIntervalField(string buf, uint64_t& pos, uint64_t len, interval_t& result) {
    uint64_t number;
    uint64_t offset = 0;
    // parse digits
    number = stoi(buf.c_str() + pos, reinterpret_cast<size_t*>(&offset));
    pos += offset;
    // skip spaces
    while (pos < len && isspace(buf[pos])) {
        pos++;
    }
    if (pos == len) {
        throw ConversionException("Error occurred during parsing interval. Field name is missing.");
    }
    // Parse intervalPartSpecifier (eg. hours, dates, minutes)
    uint64_t spacePos = string(buf).find(' ', pos);
    if (spacePos == string::npos) {
        spacePos = len;
    }
    string specifierStr = buf.substr(pos, spacePos - pos);
    pos = spacePos;
    addition(result, number, specifierStr);
}

interval_t Interval::FromCString(const char* ku_str, uint64_t len) {
    string str = string(ku_str, len);
    interval_t result;
    uint64_t pos = 0;
    result.days = 0;
    result.micros = 0;
    result.months = 0;

    if (str[pos] == '@') {
        pos++;
    }

    while (pos < len) {
        if (isdigit(str[pos])) {
            parseIntervalField(str, pos, len, result);
        } else if (!isspace(str[pos])) {
            throw ConversionException(
                "Error occurred during parsing interval. Given: \"" + str + "\".");
        }
        pos++;
    }
    return result;
}

string Interval::toString(interval_t interval) {
    char buffer[70];
    uint64_t length = IntervalToStringCast::Format(interval, buffer);
    return string(buffer, length);
}

// helper function of interval comparison
void Interval::NormalizeIntervalEntries(
    interval_t input, int64_t& months, int64_t& days, int64_t& micros) {
    int64_t extra_months_d = input.days / Interval::DAYS_PER_MONTH;
    int64_t extra_months_micros = input.micros / Interval::MICROS_PER_MONTH;
    input.days -= extra_months_d * Interval::DAYS_PER_MONTH;
    input.micros -= extra_months_micros * Interval::MICROS_PER_MONTH;

    int64_t extra_days_micros = input.micros / Interval::MICROS_PER_DAY;
    input.micros -= extra_days_micros * Interval::MICROS_PER_DAY;

    months = input.months + extra_months_d + extra_months_micros;
    days = input.days + extra_days_micros;
    micros = input.micros;
}

bool Interval::GreaterThan(const interval_t& left, const interval_t& right) {
    int64_t lMonths, lDays, lMicros;
    int64_t rMonths, rDays, rMicros;
    NormalizeIntervalEntries(left, lMonths, lDays, lMicros);
    NormalizeIntervalEntries(right, rMonths, rDays, rMicros);
    if (lMonths > rMonths) {
        return true;
    } else if (lMonths < rMonths) {
        return false;
    }
    if (lDays > rDays) {
        return true;
    } else if (lDays < rDays) {
        return false;
    }
    return lMicros > rMicros;
}

void Interval::TryGetDatePartSpecifier(string specifier, DatePartSpecifier& result) {
    StringUtils::toLower(specifier);
    if (specifier == "year" || specifier == "y" || specifier == "years") {
        result = DatePartSpecifier::YEAR;
    } else if (specifier == "month" || specifier == "mon" || specifier == "months" ||
               specifier == "mons") {
        result = DatePartSpecifier::MONTH;
    } else if (specifier == "day" || specifier == "days" || specifier == "d" ||
               specifier == "dayofmonth") {
        result = DatePartSpecifier::DAY;
    } else if (specifier == "decade" || specifier == "decades") {
        result = DatePartSpecifier::DECADE;
    } else if (specifier == "century" || specifier == "centuries") {
        result = DatePartSpecifier::CENTURY;
    } else if (specifier == "millennium" || specifier == "millennia" || specifier == "millenium") {
        result = DatePartSpecifier::MILLENNIUM;
    } else if (specifier == "quarter" || specifier == "quarters") {
        // quarter of the year (1-4)
        result = DatePartSpecifier::QUARTER;
    } else if (specifier == "microseconds" || specifier == "microsecond") {
        result = DatePartSpecifier::MICROSECOND;
    } else if (specifier == "milliseconds" || specifier == "millisecond" || specifier == "ms" ||
               specifier == "msec" || specifier == "msecs") {
        result = DatePartSpecifier::MILLISECOND;
    } else if (specifier == "second" || specifier == "seconds" || specifier == "s") {
        result = DatePartSpecifier::SECOND;
    } else if (specifier == "minute" || specifier == "minutes" || specifier == "m") {
        result = DatePartSpecifier::MINUTE;
    } else if (specifier == "hour" || specifier == "hours" || specifier == "h") {
        result = DatePartSpecifier::HOUR;
    } else {
        throw Exception("Invalid partSpecifier specifier: " + specifier);
    }
}

int32_t Interval::getIntervalPart(DatePartSpecifier specifier, interval_t& interval) {
    switch (specifier) {
    case DatePartSpecifier::YEAR:
        return interval.months / Interval::MONTHS_PER_YEAR;
    case DatePartSpecifier::MONTH:
        return interval.months % Interval::MONTHS_PER_YEAR;
    case DatePartSpecifier::DAY:
        return interval.days;
    case DatePartSpecifier::DECADE:
        return interval.months / Interval::MONTHS_PER_DECADE;
    case DatePartSpecifier::CENTURY:
        return interval.months / Interval::MONTHS_PER_CENTURY;
    case DatePartSpecifier::MILLENNIUM:
        return interval.months / Interval::MONTHS_PER_MILLENIUM;
    case DatePartSpecifier::QUARTER:
        return getIntervalPart(DatePartSpecifier::MONTH, interval) / Interval::MONTHS_PER_QUARTER +
               1;
    case DatePartSpecifier::MICROSECOND:
        return interval.micros % Interval::MICROS_PER_MINUTE;
    case DatePartSpecifier::MILLISECOND:
        return getIntervalPart(DatePartSpecifier::MICROSECOND, interval) /
               Interval::MICROS_PER_MSEC;
    case DatePartSpecifier::SECOND:
        return getIntervalPart(DatePartSpecifier::MICROSECOND, interval) / Interval::MICROS_PER_SEC;
    case DatePartSpecifier::MINUTE:
        return (interval.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
    case DatePartSpecifier::HOUR:
        return interval.micros / Interval::MICROS_PER_HOUR;
    }
}

} // namespace common
} // namespace kuzu
