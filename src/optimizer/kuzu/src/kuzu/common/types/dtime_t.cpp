#include "kuzu/common/types/dtime_t.h"

#include "kuzu/common/exception.h"
#include "kuzu/common/types/cast_helpers.h"
#include "kuzu/common/types/date_t.h"
#include "kuzu/common/utils.h"

namespace kuzu {
namespace common {

static_assert(sizeof(dtime_t) == sizeof(int64_t), "dtime_t was padded");

// string format is hh:mm:ss[.mmmmmm] (ISO 8601) (m represent microseconds)
// microseconds is optional, timezone is currently not supported
bool Time::TryConvertTime(const char* buf, uint64_t len, uint64_t& pos, dtime_t& result) {
    int32_t hour = -1, min = -1, sec = -1, micros = -1;
    pos = 0;

    if (len == 0) {
        return false;
    }

    int sep;

    // skip leading spaces
    while (pos < len && isspace(buf[pos])) {
        pos++;
    }

    if (pos >= len) {
        return false;
    }

    if (!isdigit(buf[pos])) {
        return false;
    }

    if (!Date::ParseDoubleDigit(buf, len, pos, hour)) {
        return false;
    }

    if (pos >= len) {
        return false;
    }

    // fetch the separator
    sep = buf[pos++];
    if (sep != ':') {
        // invalid separator
        return false;
    }

    if (!Date::ParseDoubleDigit(buf, len, pos, min)) {
        return false;
    }

    if (pos >= len) {
        return false;
    }

    if (buf[pos++] != sep) {
        return false;
    }

    if (!Date::ParseDoubleDigit(buf, len, pos, sec)) {
        return false;
    }

    micros = 0;
    if (pos < len && buf[pos] == '.') {
        pos++;
        // we expect some microseconds
        int32_t mult = 100000;
        for (; pos < len && isdigit(buf[pos]); pos++, mult /= 10) {
            if (mult > 0) {
                micros += (buf[pos] - '0') * mult;
            }
        }
    }

    if (!IsValid(hour, min, sec, micros)) {
        return false;
    }

    result = Time::FromTime(hour, min, sec, micros);
    return true;
}

dtime_t Time::FromCString(const char* buf, uint64_t len) {
    dtime_t result;
    uint64_t pos;
    if (!Time::TryConvertTime(buf, len, pos, result)) {
        throw ConversionException(StringUtils::string_format(
            "Error occurred during parsing time. Given: \"" + string(buf, len) +
            "\". Expected format: (hh:mm:ss[.zzzzzz])."));
    }
    return result;
}

string Time::toString(dtime_t time) {
    int32_t time_units[4];
    Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

    char micro_buffer[6];
    auto length = TimeToStringCast::Length(time_units, micro_buffer);
    auto buffer = unique_ptr<char[]>(new char[length]);
    TimeToStringCast::Format(buffer.get(), length, time_units, micro_buffer);
    return string(buffer.get(), length);
}

bool Time::IsValid(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
    if (hour > 23 || hour < 0 || minute > 59 || minute < 0 || second > 59 || second < 0 ||
        microseconds > 999999 || microseconds < 0) {
        return false;
    }
    return true;
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
    if (!Time::IsValid(hour, minute, second, microseconds)) {
        throw ConversionException(StringUtils::string_format(
            "Time field value out of range: %d:%d:%d[.%d].", hour, minute, second, microseconds));
    }
    int64_t result;
    result = hour;                                             // hours
    result = result * Interval::MINS_PER_HOUR + minute;        // hours -> minutes
    result = result * Interval::SECS_PER_MINUTE + second;      // minutes -> seconds
    result = result * Interval::MICROS_PER_SEC + microseconds; // seconds -> microseconds
    return dtime_t(result);
}

void Time::Convert(dtime_t dtime, int32_t& hour, int32_t& min, int32_t& sec, int32_t& micros) {
    int64_t time = dtime.micros;
    hour = int32_t(time / Interval::MICROS_PER_HOUR);
    time -= int64_t(hour) * Interval::MICROS_PER_HOUR;
    min = int32_t(time / Interval::MICROS_PER_MINUTE);
    time -= int64_t(min) * Interval::MICROS_PER_MINUTE;
    sec = int32_t(time / Interval::MICROS_PER_SEC);
    time -= int64_t(sec) * Interval::MICROS_PER_SEC;
    micros = int32_t(time);
}

} // namespace common
} // namespace kuzu
