#include "function/scalar/nested_functions.hpp"

namespace s62 {

void BuiltinFunctions::RegisterNestedFunctions() {
	// Register<ArraySliceFun>();
	// Register<StructPackFun>();
	// Register<StructExtractFun>();
	// Register<ListConcatFun>();
	Register<ListContainsFun>();
	Register<ListPositionFun>();
	// Register<ListAggregateFun>();
	Register<ListValueFun>();
	// Register<ListApplyFun>();
	// Register<ListFilterFun>();
	// Register<ListExtractFun>();
	// Register<ListRangeFun>();
	// Register<ListFlattenFun>();
	// Register<MapFun>();
	// Register<MapExtractFun>();
	// Register<CardinalityFun>();
}

} // namespace s62
