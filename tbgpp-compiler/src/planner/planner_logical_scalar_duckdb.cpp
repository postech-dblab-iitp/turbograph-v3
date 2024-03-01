#include "kuzu/common/expression_type.h"
#include "planner.hpp"

#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_case_expression.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

namespace s62 {

unique_ptr<duckdb::Expression> Planner::lExprScalarExpressionDuckDB(
    kuzu::binder::Expression *expression)
{
    auto expr_type = expression->expressionType;
    if (isExpressionBoolConnection(expr_type)) {
        return lExprScalarBoolOpDuckDB(expression);
    }
    else if (isExpressionComparison(expr_type)) {
        return lExprScalarComparisonExprDuckDB(expression);
    }
    else if (isExpressionProperty(expr_type)) {
        return lExprScalarPropertyExprDuckDB(expression);
    }
    else if (isExpressionLiteral(expr_type)) {
        return lExprScalarLiteralExprDuckDB(expression);
    }
    else if (isExpressionAggregate(expr_type)) {
        return lExprScalarAggFuncExprDuckDB(expression);
    }
    else if (isExpressionScalarFunc(expr_type)) {
        return lExprScalarFuncExprDuckDB(expression);
    }
    else if (isExpressionCaseElse(expr_type)) {
        return lExprScalarCaseElseExprDuckDB(expression);
    }
    else if (isExpressionSubquery(expr_type)) {
        return lExprScalarExistentialSubqueryExprDuckDB(expression);
    }
    else if (isExpressionFunction(expr_type)) {
        return lExprScalarFuncExprDuckDB(expression);
    }
    else if (isExpressionParameter(expr_type)) {
        return lExprScalarParamExprDuckDB(expression);
    }
    else {
        D_ASSERT(false);
    }
}

unique_ptr<duckdb::Expression> Planner::lExprScalarBoolOpDuckDB(
    kuzu::binder::Expression *expression)
{
    ScalarFunctionExpression *bool_expr =
        (ScalarFunctionExpression *)expression;
    auto children = bool_expr->getChildren();
    D_ASSERT(false);
    return nullptr;
}

unique_ptr<duckdb::Expression> Planner::lExprScalarComparisonExprDuckDB(
    kuzu::binder::Expression *expression)
{
    D_ASSERT(false);
    return nullptr;
}

unique_ptr<duckdb::Expression> Planner::lExprScalarPropertyExprDuckDB(
    kuzu::binder::Expression *expression)
{
    // TODO: this will not get proper type if the property_expr is from query execution result, not a table.
    PropertyExpression *prop_expr = (PropertyExpression *)expression;
    DataType type = prop_expr->getDataType();
    duckdb::LogicalTypeId duckdb_type_id = (duckdb::LogicalTypeId)type.typeID;
    duckdb::LogicalType duckdb_type;

    if (duckdb_type_id == duckdb::LogicalTypeId::DECIMAL) {
        // TODO: very temporal code
        duckdb_type =  duckdb::LogicalType::DECIMAL(12, 2);
    }
    else {
        duckdb_type = duckdb::LogicalType(duckdb_type_id);
    }
    return make_unique<duckdb::BoundReferenceExpression>(
        duckdb_type, 0 /* child_idx TODO: give proper value */);
}

unique_ptr<duckdb::Expression> Planner::lExprScalarLiteralExprDuckDB(
    kuzu::binder::Expression *expression)
{
    // TODO: use the actual value
    LiteralExpression *lit_expr = (LiteralExpression *)expression;
    DataType type = lit_expr->literal.get()->dataType;
    duckdb::LogicalTypeId duckdb_type_id = (duckdb::LogicalTypeId)type.typeID;
    duckdb::LogicalType duckdb_type;

    if (duckdb_type_id == duckdb::LogicalTypeId::DECIMAL) {
        // TODO: very temporal code
        duckdb_type =  duckdb::LogicalType::DECIMAL(12, 2);
    }
    else {
        duckdb_type = duckdb::LogicalType(duckdb_type_id);
    }
    return make_unique<duckdb::BoundConstantExpression>(
        duckdb::Value(duckdb_type));
}

unique_ptr<duckdb::Expression> Planner::lExprScalarAggFuncExprDuckDB(
    kuzu::binder::Expression *expression)
{
    D_ASSERT(false);
    return nullptr;
}

unique_ptr<duckdb::Expression> Planner::lExprScalarFuncExprDuckDB(
    kuzu::binder::Expression *expression)
{
	ScalarFunctionExpression *scalarfunc_expr = (ScalarFunctionExpression *)expression;
	kuzu::binder::expression_vector children = scalarfunc_expr->getChildren();
	std::string func_name = (scalarfunc_expr)->getRawFuncName();
	if (lIsCastingFunction(func_name)) { return lExprScalarCastExprDuckDB(expression); }
	std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);

    vector<duckdb::LogicalType> duckdb_child_types;
    for (auto i = 0; i < children.size(); i++) {
        auto duckdb_child = lExprScalarExpressionDuckDB(children[i].get());
        duckdb_child_types.push_back(duckdb_child->return_type);
    }

	duckdb::idx_t func_mdid_id = context->db->GetCatalogWrapper().GetScalarFuncMdId(*context, func_name, duckdb_child_types);
    duckdb::ScalarFunctionCatalogEntry *func_catalog_entry;
	duckdb::idx_t function_idx;
	context->db->GetCatalogWrapper().GetScalarFuncAndIdx(*context, func_mdid_id, func_catalog_entry, function_idx);
	auto function = func_catalog_entry->functions.get()->functions[function_idx];
	vector<unique_ptr<duckdb::Expression>> duckdb_childs;
	for (auto i = 0; i < children.size(); i++) {
		duckdb_childs.push_back(move(lExprScalarExpressionDuckDB(children[i].get())));
	}
    unique_ptr<duckdb::FunctionData> bind_info;
	if (function.bind) {
		bind_info = function.bind(*context, function, duckdb_childs);
		duckdb_childs.resize(std::min(function.arguments.size(), duckdb_childs.size()));
	}

    return make_unique<duckdb::BoundFunctionExpression>(
        function.return_type, function, std::move(duckdb_childs), std::move(bind_info));
}

unique_ptr<duckdb::Expression> Planner::lExprScalarCaseElseExprDuckDB(
    kuzu::binder::Expression *expression)
{
    D_ASSERT(false);
    return nullptr;
}

unique_ptr<duckdb::Expression>
Planner::lExprScalarExistentialSubqueryExprDuckDB(
    kuzu::binder::Expression *expression)
{
    D_ASSERT(false);
    return nullptr;
}

unique_ptr<duckdb::Expression> Planner::lExprScalarCastExprDuckDB(
    kuzu::binder::Expression *expression)
{
    D_ASSERT(false);
    return nullptr;
}

unique_ptr<duckdb::Expression> Planner::lExprScalarParamExprDuckDB(
    kuzu::binder::Expression *expression)
{
    D_ASSERT(false);
    return nullptr;
}

}  // namespace s62