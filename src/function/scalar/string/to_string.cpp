#include "function/scalar/string_functions.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/vector_operations/unary_executor.hpp"
#include "common/operator/convert_to_string.hpp"

namespace duckdb {
struct ToStringOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		return ConvertToString::Operation<TA>(input);
	}
};

void ToStringFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction to_string("string", 
                             {LogicalType::INTEGER},
                             LogicalType::VARCHAR,
                             ScalarFunction::UnaryFunction<int32_t, string_t, ToStringOperator>);
	set.AddFunction(to_string);
}

} // namespace duckdb