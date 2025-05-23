#include "common/types/decimal.hpp"
#include "common/types/cast_helpers.hpp"

namespace duckdb {

template <class SIGNED, class UNSIGNED>
string TemplatedDecimalToString(SIGNED value, uint8_t scale) {
	auto len = DecimalToString::DecimalLength<SIGNED, UNSIGNED>(value, scale);
	auto data = unique_ptr<char[]>(new char[len + 1]);
	DecimalToString::FormatDecimal<SIGNED, UNSIGNED>(value, scale, data.get(), len);
	return string(data.get(), len);
}

string Decimal::ToString(int16_t value, uint8_t scale) {
	return TemplatedDecimalToString<int16_t, uint16_t>(value, scale);
}

string Decimal::ToString(int32_t value, uint8_t scale) {
	return TemplatedDecimalToString<int32_t, uint32_t>(value, scale);
}

string Decimal::ToString(int64_t value, uint8_t scale) {
	return TemplatedDecimalToString<int64_t, uint64_t>(value, scale);
}

string Decimal::ToString(hugeint_t value, uint8_t scale) {
	auto len = HugeintToStringCast::DecimalLength(value, scale);
	auto data = unique_ptr<char[]>(new char[len + 1]);
	HugeintToStringCast::FormatDecimal(value, scale, data.get(), len);
	return string(data.get(), len);
}

} // namespace duckdb
