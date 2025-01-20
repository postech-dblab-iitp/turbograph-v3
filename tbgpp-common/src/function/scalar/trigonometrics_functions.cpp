#include "function/scalar/trigonometric_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/exception.hpp"

namespace s62 {

void BuiltinFunctions::RegisterTrigonometricsFunctions() {
	Register<SinFun>();
	Register<CosFun>();
	Register<TanFun>();
	Register<AsinFun>();
	Register<AcosFun>();
	Register<AtanFun>();
	Register<CotFun>();
	Register<Atan2Fun>();
}

} // namespace s62
