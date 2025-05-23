#include "function/function.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "common/types/hash.hpp"
#include "common/limits.hpp"
#include "common/string_util.hpp"
#include "function/aggregate_function.hpp"
#include "function/cast_rules.hpp"
#include "function/scalar/string_functions.hpp"
#include "function/scalar_function.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"
#include "parser/parsed_data/create_collation_info.hpp"
// #include "parser/parsed_data/create_copy_function_info.hpp"
// #include "parser/parsed_data/create_pragma_function_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_table_function_info.hpp"
// #include "parser/parsed_data/pragma_info.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
// #include "planner/expression_binder.hpp"
#include <iostream>

namespace duckdb {

FunctionData::~FunctionData() {
}

unique_ptr<FunctionData> FunctionData::Copy() {
	throw InternalException("Unimplemented copy for FunctionData");
}

bool FunctionData::Equals(FunctionData &other) {
	return true;
}

bool FunctionData::Equals(FunctionData *left, FunctionData *right) {
	if (left == right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

Function::Function(string name_p) : name(move(name_p)) {
}
Function::~Function() {
}

SimpleFunction::SimpleFunction(string name_p, vector<LogicalType> arguments_p, LogicalType varargs_p)
    : Function(move(name_p)), arguments(move(arguments_p)), varargs(move(varargs_p)) {
}

SimpleFunction::~SimpleFunction() {
}

string SimpleFunction::ToString() {
	return Function::CallToString(name, arguments);
}

bool SimpleFunction::HasVarArgs() const {
	return varargs.id() != LogicalTypeId::INVALID;
}

SimpleNamedParameterFunction::SimpleNamedParameterFunction(string name_p, vector<LogicalType> arguments_p,
                                                           LogicalType varargs_p)
    : SimpleFunction(move(name_p), move(arguments_p), move(varargs_p)) {
}

SimpleNamedParameterFunction::~SimpleNamedParameterFunction() {
}

string SimpleNamedParameterFunction::ToString() {
	return Function::CallToString(name, arguments, named_parameters);
}

bool SimpleNamedParameterFunction::HasNamedParameters() {
	return !named_parameters.empty();
}

BaseScalarFunction::BaseScalarFunction(string name_p, vector<LogicalType> arguments_p, LogicalType return_type_p,
                                       bool has_side_effects, LogicalType varargs_p, bool propagates_null_values_p)
    : SimpleFunction(move(name_p), move(arguments_p), move(varargs_p)), return_type(move(return_type_p)),
      has_side_effects(has_side_effects), propagates_null_values(propagates_null_values_p) {
}

BaseScalarFunction::~BaseScalarFunction() {
}

string BaseScalarFunction::ToString() {
	return Function::CallToString(name, arguments, return_type);
}

// add your initializer for new functions here
void BuiltinFunctions::Initialize() {

	// TODO make necessary ones available
	// RegisterSQLiteFunctions();
	// RegisterReadFunctions();
	// RegisterTableFunctions();
	// RegisterArrowFunctions();

	RegisterAlgebraicAggregates();
	RegisterDistributiveAggregates();
	RegisterNestedAggregates();
	// RegisterHolisticAggregates();
	RegisterRegressiveAggregates();

	RegisterDateFunctions();
	// RegisterEnumFunctions();
	// RegisterGenericFunctions();
	RegisterMathFunctions();
	RegisterOperators();
	// RegisterSequenceFunctions();
	RegisterStringFunctions();
	RegisterNestedFunctions();
	// RegisterTrigonometricsFunctions();

	// RegisterPragmaFunctions();

	// initialize collations
	// AddCollation("nocase", LowerFun::GetFunction(), true);
	// AddCollation("noaccent", StripAccentsFun::GetFunction());
	// AddCollation("nfc", NFCNormalizeFun::GetFunction());
}

BuiltinFunctions::BuiltinFunctions(ClientContext &context, Catalog &catalog, bool is_catalog_already_exist) : context(context), catalog(catalog),
	is_catalog_already_exist(is_catalog_already_exist) {
}

// void BuiltinFunctions::AddCollation(string name, ScalarFunction function, bool combinable,
//                                     bool not_required_for_equality) {
// 	CreateCollationInfo info(move(name), move(function), combinable, not_required_for_equality);
// 	catalog.CreateCollation(context, &info);
// }

void BuiltinFunctions::AddFunction(AggregateFunctionSet set) {
	unique_ptr<AggregateFunctionSet> set_ptr = make_unique<AggregateFunctionSet>(set.name);
	set_ptr->functions = move(set.functions);
	CreateAggregateFunctionInfo info(move(set_ptr));
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(AggregateFunction function) {
	unique_ptr<AggregateFunctionSet> set_ptr = make_unique<AggregateFunctionSet>(function.name);
	set_ptr->AddFunction(move(function));
	CreateAggregateFunctionInfo info(move(set_ptr));
	catalog.CreateFunction(context, &info);
}

// void BuiltinFunctions::AddFunction(PragmaFunction function) {
// 	CreatePragmaFunctionInfo info(move(function));
// 	catalog.CreatePragmaFunction(context, &info);
// }

// void BuiltinFunctions::AddFunction(const string &name, vector<PragmaFunction> functions) {
// 	CreatePragmaFunctionInfo info(name, move(functions));
// 	catalog.CreatePragmaFunction(context, &info);
// }

void BuiltinFunctions::AddFunction(ScalarFunction function) {
	unique_ptr<ScalarFunctionSet> set_ptr = make_unique<ScalarFunctionSet>(function.name);
	set_ptr->AddFunction(move(function));
	CreateScalarFunctionInfo info(move(set_ptr));
	catalog.CreateFunction(context, &info);
}

void BuiltinFunctions::AddFunction(const vector<string> &names, ScalarFunction function) { // NOLINT: false positive
	for (auto &name : names) {
		function.name = name;
		AddFunction(function);
	}
}

void BuiltinFunctions::AddFunction(ScalarFunctionSet set) {
	unique_ptr<ScalarFunctionSet> set_ptr = make_unique<ScalarFunctionSet>(set.name);
	set_ptr->functions = move(set.functions);
	CreateScalarFunctionInfo info(move(set_ptr));
	catalog.CreateFunction(context, &info);
}

// void BuiltinFunctions::AddFunction(TableFunction function) {
// 	CreateTableFunctionInfo info(move(function));
// 	// catalog.CreateTableFunction(context, &info);
// }

// void BuiltinFunctions::AddFunction(TableFunctionSet set) {
// 	CreateTableFunctionInfo info(move(set));
// 	// catalog.CreateTableFunction(context, &info);
// }

// void BuiltinFunctions::AddFunction(CopyFunction function) {
// 	CreateCopyFunctionInfo info(move(function));
// 	// catalog.CreateCopyFunction(context, &info);
// }

hash_t BaseScalarFunction::Hash() const {
	hash_t hash = return_type.Hash();
	for (auto &arg : arguments) {
		duckdb::CombineHash(hash, arg.Hash());
	}
	return hash;
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments) {
	string result = name + "(";
	result += StringUtil::Join(arguments, arguments.size(), ", ",
	                           [](const LogicalType &argument) { return argument.ToString(); });
	return result + ")";
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments,
                              const LogicalType &return_type) {
	string result = CallToString(name, arguments);
	result += " -> " + return_type.ToString();
	return result;
}

string Function::CallToString(const string &name, const vector<LogicalType> &arguments,
                              const named_parameter_type_map_t &named_parameters) {
	vector<string> input_arguments;
	input_arguments.reserve(arguments.size() + named_parameters.size());
	for (auto &arg : arguments) {
		input_arguments.push_back(arg.ToString());
	}
	for (auto &kv : named_parameters) {
		input_arguments.push_back(StringUtil::Format("%s : %s", kv.first, kv.second.ToString()));
	}
	return StringUtil::Format("%s(%s)", name, StringUtil::Join(input_arguments, ", "));
}

static int64_t BindVarArgsFunctionCost(SimpleFunction &func, vector<LogicalType> &arguments) {
	if (arguments.size() < func.arguments.size()) {
		// not enough arguments to fulfill the non-vararg part of the function
		return -1;
	}
	int64_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		LogicalType arg_type = i < func.arguments.size() ? func.arguments[i] : func.varargs;
		if (arguments[i] == arg_type) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastRules::ImplicitCast(arguments[i], arg_type);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += cast_cost;
		} else {
			// we can't implicitly cast: throw an error
			return -1;
		}
	}
	return cost;
}

static int64_t BindFunctionCost(SimpleFunction &func, vector<LogicalType> &arguments) {
	if (func.HasVarArgs()) {
		// special case varargs function
		return BindVarArgsFunctionCost(func, arguments);
	}
	if (func.arguments.size() != arguments.size()) {
		// invalid argument count: check the next function
		return -1;
	}
	int64_t cost = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i].id() == func.arguments[i].id()) {
			// arguments match: do nothing
			continue;
		}
		int64_t cast_cost = CastRules::ImplicitCast(arguments[i], func.arguments[i]);
		if (cast_cost >= 0) {
			// we can implicitly cast, add the cost to the total cost
			cost += cast_cost;
		} else {
			// we can't implicitly cast: throw an error
			return -1;
		}
	}
	return cost;
}

template <class T>
static idx_t BindFunctionFromArguments(const string &name, vector<T> &functions, vector<LogicalType> &arguments,
                                       string &error) {
	idx_t best_function = DConstants::INVALID_INDEX;
	int64_t lowest_cost = NumericLimits<int64_t>::Maximum();
	vector<idx_t> conflicting_functions;
	for (idx_t f_idx = 0; f_idx < functions.size(); f_idx++) {
		auto &func = functions[f_idx];
		// check the arguments of the function
		int64_t cost = BindFunctionCost(func, arguments);
		if (cost < 0) {
			// auto casting was not possible
			continue;
		}
		if (cost == lowest_cost) {
			conflicting_functions.push_back(f_idx);
			continue;
		}
		if (cost > lowest_cost) {
			continue;
		}
		conflicting_functions.clear();
		lowest_cost = cost;
		best_function = f_idx;
	}
	if (!conflicting_functions.empty()) {
		// there are multiple possible function definitions
		// throw an exception explaining which overloads are there
		conflicting_functions.push_back(best_function);
		string call_str = Function::CallToString(name, arguments);
		string candidate_str = "";
		for (auto &conf : conflicting_functions) {
			auto &f = functions[conf];
			candidate_str += "\t" + f.ToString() + "\n";
		}
		error =
		    StringUtil::Format("Could not choose a best candidate function for the function call \"%s\". In order to "
		                       "select one, please add explicit type casts.\n\tCandidate functions:\n%s",
		                       call_str, candidate_str);
		return DConstants::INVALID_INDEX;
	}
	if (best_function == DConstants::INVALID_INDEX) {
		// no matching function was found, throw an error
		string call_str = Function::CallToString(name, arguments);
		string candidate_str = "";
		for (auto &f : functions) {
			candidate_str += "\t" + f.ToString() + "\n";
		}
		error = StringUtil::Format("No function matches the given name and argument types '%s'. You might need to add "
		                           "explicit type casts.\n\tCandidate functions:\n%s",
		                           call_str, candidate_str);
		return DConstants::INVALID_INDEX;
	}
	return best_function;
}

idx_t Function::BindFunction(const string &name, vector<ScalarFunction> &functions, vector<LogicalType> &arguments,
                             string &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

idx_t Function::BindFunction(const string &name, vector<AggregateFunction> &functions, vector<LogicalType> &arguments,
                             string &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

idx_t Function::BindFunction(const string &name, vector<TableFunction> &functions, vector<LogicalType> &arguments,
                             string &error) {
	return BindFunctionFromArguments(name, functions, arguments, error);
}

// idx_t Function::BindFunction(const string &name, vector<PragmaFunction> &functions, PragmaInfo &info, string &error) {
// 	vector<LogicalType> types;
// 	for (auto &value : info.parameters) {
// 		types.push_back(value.type());
// 	}
// 	idx_t entry = BindFunctionFromArguments(name, functions, types, error);
// 	if (entry == DConstants::INVALID_INDEX) {
// 		throw BinderException(error);
// 	}
// 	auto &candidate_function = functions[entry];
// 	// cast the input parameters
// 	for (idx_t i = 0; i < info.parameters.size(); i++) {
// 		auto target_type =
// 		    i < candidate_function.arguments.size() ? candidate_function.arguments[i] : candidate_function.varargs;
// 		info.parameters[i] = info.parameters[i].CastAs(target_type);
// 	}
// 	return entry;
// }

vector<LogicalType> GetLogicalTypesFromExpressions(vector<unique_ptr<Expression>> &arguments) {
	vector<LogicalType> types;
	types.reserve(arguments.size());
	for (auto &argument : arguments) {
		types.push_back(argument->return_type);
	}
	return types;
}

idx_t Function::BindFunction(const string &name, vector<ScalarFunction> &functions,
                             vector<unique_ptr<Expression>> &arguments, string &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return Function::BindFunction(name, functions, types, error);
}

idx_t Function::BindFunction(const string &name, vector<AggregateFunction> &functions,
                             vector<unique_ptr<Expression>> &arguments, string &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return Function::BindFunction(name, functions, types, error);
}

idx_t Function::BindFunction(const string &name, vector<TableFunction> &functions,
                             vector<unique_ptr<Expression>> &arguments, string &error) {
	auto types = GetLogicalTypesFromExpressions(arguments);
	return Function::BindFunction(name, functions, types, error);
}

enum class LogicalTypeComparisonResult { IDENTICAL_TYPE, TARGET_IS_ANY, DIFFERENT_TYPES };

LogicalTypeComparisonResult RequiresCast(const LogicalType &source_type, const LogicalType &target_type) {
	if (target_type.id() == LogicalTypeId::ANY) {
		return LogicalTypeComparisonResult::TARGET_IS_ANY;
	}
	if (source_type == target_type) {
		return LogicalTypeComparisonResult::IDENTICAL_TYPE;
	}
	if (source_type.id() == LogicalTypeId::LIST && target_type.id() == LogicalTypeId::LIST) {
		if (source_type.AuxInfo() && target_type.AuxInfo()) {
			return RequiresCast(ListType::GetChildType(source_type), ListType::GetChildType(target_type));
		} else {
			return RequiresCast(LogicalType::ID, LogicalType::ID);
		}
	}
	return LogicalTypeComparisonResult::DIFFERENT_TYPES;
}

void BaseScalarFunction::CastToFunctionArguments(vector<unique_ptr<Expression>> &children) {
	// D_ASSERT(false && "nope!");
	for (idx_t i = 0; i < children.size(); i++) {
		auto target_type = i < this->arguments.size() ? this->arguments[i] : this->varargs;
		target_type.Verify();
		// check if the type of child matches the type of function argument
		// if not we need to add a cast
		auto cast_result = RequiresCast(children[i]->return_type, target_type);
		// except for one special case: if the function accepts ANY argument
		// in that case we don't add a cast
		if (cast_result == LogicalTypeComparisonResult::TARGET_IS_ANY) {
			if (children[i]->return_type.id() == LogicalTypeId::UNKNOWN) {
				// UNLESS the child is a prepared statement parameter
				// in that case we default the prepared statement parameter to VARCHAR
				D_ASSERT(false); // TODO s62 disabled for type exchange
				// children[i]->return_type =
				//     ExpressionBinder::ExchangeType(target_type, LogicalTypeId::ANY, LogicalType::VARCHAR);
			}
		} else if (cast_result == LogicalTypeComparisonResult::DIFFERENT_TYPES) {
			children[i] = BoundCastExpression::AddCastToType(move(children[i]), target_type);
		}
	}
}

// unique_ptr<BoundFunctionExpression> ScalarFunction::BindScalarFunction(ClientContext &context, const string &schema,
//                                                                        const string &name,
//                                                                        vector<unique_ptr<Expression>> children,
//                                                                        string &error, bool is_operator) {
// 	// bind the function
// 	auto function = Catalog::GetCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, schema, name);
// 	D_ASSERT(function && function->type == CatalogType::SCALAR_FUNCTION_ENTRY);
// 	return ScalarFunction::BindScalarFunction(context, (ScalarFunctionCatalogEntry &)*function, move(children), error,
// 	                                          is_operator);
// }

// unique_ptr<BoundFunctionExpression> ScalarFunction::BindScalarFunction(ClientContext &context,
//                                                                        ScalarFunctionCatalogEntry &func,
//                                                                        vector<unique_ptr<Expression>> children,
//                                                                        string &error, bool is_operator) {
// 	// bind the function
// 	idx_t best_function = Function::BindFunction(func.name, func.functions, children, error);
// 	if (best_function == DConstants::INVALID_INDEX) {
// 		return nullptr;
// 	}
// 	// found a matching function!
// 	auto &bound_function = func.functions[best_function];
// 	return ScalarFunction::BindScalarFunction(context, bound_function, move(children), is_operator);
// }

unique_ptr<BoundFunctionExpression> ScalarFunction::BindScalarFunction(ClientContext &context,
                                                                       ScalarFunction bound_function,
                                                                       vector<unique_ptr<Expression>> children,
                                                                       bool is_operator) {
	unique_ptr<FunctionData> bind_info;
	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
	}
	// check if we need to add casts to the children
	bound_function.CastToFunctionArguments(children);

	// now create the function
	auto return_type = bound_function.return_type;
	return make_unique<BoundFunctionExpression>(move(return_type), move(bound_function), move(children),
	                                            move(bind_info), is_operator);
}

unique_ptr<BoundAggregateExpression>
AggregateFunction::BindAggregateFunction(ClientContext &context, AggregateFunction bound_function,
                                         vector<unique_ptr<Expression>> children, unique_ptr<Expression> filter,
                                         bool is_distinct, unique_ptr<BoundOrderModifier> order_bys) {
	unique_ptr<FunctionData> bind_info;
	if (bound_function.bind) {
		bind_info = bound_function.bind(context, bound_function, children);
		// we may have lost some arguments in the bind
		children.resize(MinValue(bound_function.arguments.size(), children.size()));
	}

	// check if we need to add casts to the children
	bound_function.CastToFunctionArguments(children);

	// Special case: for ORDER BY aggregates, we wrap the aggregate function in a SortedAggregateFunction
	// The children are the sort clauses and the binding contains the ordering data.
	if (order_bys && !order_bys->orders.empty()) {
		bind_info = BindSortedAggregate(bound_function, children, move(bind_info), move(order_bys));
	}

	return make_unique<BoundAggregateExpression>(move(bound_function), move(children), move(filter), move(bind_info),
	                                             is_distinct);
}

} // namespace duckdb
