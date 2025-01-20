#include "function/scalar/string_functions.hpp"
#include "third_party/utf8proc/utf8proc.hpp"
#include "third_party/utf8proc/utf8proc_wrapper.hpp"

namespace s62 {

struct AsciiOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		auto str = input.GetDataUnsafe();
		if (duckdb::Utf8Proc::Analyze(str, input.GetSize()) == duckdb::UnicodeType::ASCII) {
			return str[0];
		}
		int utf8_bytes = 4;
		return duckdb::Utf8Proc::UTF8ToCodepoint(str, utf8_bytes);
	}
};

void ASCII::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction ascii("ascii", {LogicalType::VARCHAR}, LogicalType::INTEGER,
	                     ScalarFunction::UnaryFunction<string_t, int32_t, AsciiOperator>);
	set.AddFunction(ascii);
}

} // namespace s62
