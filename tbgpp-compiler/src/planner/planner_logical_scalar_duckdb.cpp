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
    else if (isExpressionShortestPath(expr_type)) {
        return lExprScalarShortestPathExprDuckDB(expression);
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

    duckdb::ExpressionType op_type;
    switch (bool_expr->expressionType) {
        case kuzu::common::ExpressionType::NOT:
            op_type = duckdb::ExpressionType::OPERATOR_NOT;
            break;
        case kuzu::common::ExpressionType::AND:
            op_type = duckdb::ExpressionType::CONJUNCTION_AND;
            break;
        case kuzu::common::ExpressionType::OR:
            op_type = duckdb::ExpressionType::CONJUNCTION_OR;
            break;
        default:
            D_ASSERT(false);
    }

    if (op_type == duckdb::ExpressionType::OPERATOR_NOT) {
        auto bound_operator = make_unique<duckdb::BoundOperatorExpression>(
            op_type, duckdb::LogicalType::BOOLEAN);
        bound_operator->children.push_back(
            std::move(lExprScalarExpressionDuckDB(children[0].get())));
        return bound_operator;
    }
    else {
        auto conjunction =
            make_unique<duckdb::BoundConjunctionExpression>(op_type);
        for (auto i = 0; i < children.size(); i++) {
            conjunction->children.push_back(
                std::move(lExprScalarExpressionDuckDB(children[i].get())));
        }
        return conjunction;
    }
}

unique_ptr<duckdb::Expression> Planner::lExprScalarComparisonExprDuckDB(
    kuzu::binder::Expression *expression)
{
    ScalarFunctionExpression *comp_expr =
        (ScalarFunctionExpression *)expression;
    auto children = comp_expr->getChildren();
    unique_ptr<duckdb::Expression> lhs =
        lExprScalarExpressionDuckDB(children[0].get());
    unique_ptr<duckdb::Expression> rhs =
        lExprScalarExpressionDuckDB(children[1].get());

    duckdb::ExpressionType cmp_type;
    switch (comp_expr->expressionType) {
        case kuzu::common::ExpressionType::EQUALS:
            cmp_type = duckdb::ExpressionType::COMPARE_EQUAL;
            break;
        case kuzu::common::ExpressionType::NOT_EQUALS:
            cmp_type = duckdb::ExpressionType::COMPARE_NOTEQUAL;
            break;
        case kuzu::common::ExpressionType::GREATER_THAN:
            cmp_type = duckdb::ExpressionType::COMPARE_GREATERTHAN;
            break;
        case kuzu::common::ExpressionType::GREATER_THAN_EQUALS:
            cmp_type = duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
            break;
        case kuzu::common::ExpressionType::LESS_THAN:
            cmp_type = duckdb::ExpressionType::COMPARE_LESSTHAN;
            break;
        case kuzu::common::ExpressionType::LESS_THAN_EQUALS:
            cmp_type = duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
            break;
        default:
            D_ASSERT(false);
    }

    return make_unique<duckdb::BoundComparisonExpression>(
        cmp_type,
        std::move(lhs),  // lhs
        std::move(rhs)   // rhs
    );
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
        duckdb_type = duckdb::LogicalType::DECIMAL(12, 2);
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
        duckdb_type = duckdb::LogicalType::DECIMAL(12, 2);
    }
    else if (duckdb_type_id == duckdb::LogicalTypeId::LIST) {
        duckdb_type = pConvertKuzuTypeToLogicalType(lit_expr->literal->dataType);
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
    AggregateFunctionExpression *aggfunc_expr =
        (AggregateFunctionExpression *)expression;
    kuzu::binder::expression_vector children = aggfunc_expr->getChildren();

    std::string func_name = (aggfunc_expr)->getRawFuncName();
    std::transform(func_name.begin(), func_name.end(), func_name.begin(),
                   ::tolower);

    vector<duckdb::LogicalType> duckdb_child_types;
    for (auto i = 0; i < children.size(); i++) {
        auto duckdb_child = lExprScalarExpressionDuckDB(children[i].get());
        duckdb_child_types.push_back(duckdb_child->return_type);
    }

    duckdb::idx_t func_mdid_id =
        context->db->GetCatalogWrapper().GetAggFuncMdId(*context, func_name,
                                                        duckdb_child_types);
    duckdb::AggregateFunctionCatalogEntry *aggfunc_catalog_entry;
    duckdb::idx_t function_idx;
    context->db->GetCatalogWrapper().GetAggFuncAndIdx(
        *context, func_mdid_id, aggfunc_catalog_entry, function_idx);

    auto function =
        aggfunc_catalog_entry->functions.get()->functions[function_idx];
    vector<unique_ptr<duckdb::Expression>> duckdb_childs;
    for (auto i = 0; i < children.size(); i++) {
        duckdb_childs.push_back(
            move(lExprScalarExpressionDuckDB(children[i].get())));
    }
    unique_ptr<duckdb::FunctionData> bind_info;
    if (function.bind) {
        bind_info = function.bind(*context, function, duckdb_childs);
        duckdb_childs.resize(
            std::min(function.arguments.size(), duckdb_childs.size()));
    }

    return make_unique<duckdb::BoundAggregateExpression>(
        std::move(function), std::move(duckdb_childs), nullptr,
        std::move(bind_info),
        aggfunc_expr->isDistinct());  // get function.return_type
}

unique_ptr<duckdb::Expression> Planner::lExprScalarFuncExprDuckDB(
    kuzu::binder::Expression *expression)
{
    ScalarFunctionExpression *scalarfunc_expr =
        (ScalarFunctionExpression *)expression;
    kuzu::binder::expression_vector children = scalarfunc_expr->getChildren();
    std::string func_name = (scalarfunc_expr)->getRawFuncName();
    if (lIsCastingFunction(func_name)) {
        return lExprScalarCastExprDuckDB(expression);
    }
    std::transform(func_name.begin(), func_name.end(), func_name.begin(),
                   ::tolower);

    vector<duckdb::LogicalType> duckdb_child_types;
    for (auto i = 0; i < children.size(); i++) {
        auto duckdb_child = lExprScalarExpressionDuckDB(children[i].get());
        duckdb_child_types.push_back(duckdb_child->return_type);
    }

    duckdb::idx_t func_mdid_id =
        context->db->GetCatalogWrapper().GetScalarFuncMdId(*context, func_name,
                                                           duckdb_child_types);
    duckdb::ScalarFunctionCatalogEntry *func_catalog_entry;
    duckdb::idx_t function_idx;
    context->db->GetCatalogWrapper().GetScalarFuncAndIdx(
        *context, func_mdid_id, func_catalog_entry, function_idx);
    auto function =
        func_catalog_entry->functions.get()->functions[function_idx];
    vector<unique_ptr<duckdb::Expression>> duckdb_childs;
    for (auto i = 0; i < children.size(); i++) {
        duckdb_childs.push_back(
            move(lExprScalarExpressionDuckDB(children[i].get())));
    }
    unique_ptr<duckdb::FunctionData> bind_info;
    if (function.bind) {
        bind_info = function.bind(*context, function, duckdb_childs);
        duckdb_childs.resize(
            std::min(function.arguments.size(), duckdb_childs.size()));
    }

    return make_unique<duckdb::BoundFunctionExpression>(
        function.return_type, function, std::move(duckdb_childs),
        std::move(bind_info));
}

unique_ptr<duckdb::Expression> Planner::lExprScalarCaseElseExprDuckDB(
    kuzu::binder::Expression *expression)
{
    CaseExpression *case_expr = (CaseExpression *)expression;

    unique_ptr<duckdb::Expression> e_when;
    unique_ptr<duckdb::Expression> e_then;
    unique_ptr<duckdb::Expression> e_else;

    size_t numCaseAlts = case_expr->getNumCaseAlternatives();
    for (size_t idx = 0; idx < numCaseAlts; idx++) {
        auto casealt_expr = case_expr->getCaseAlternative(idx);
        auto when_expr = casealt_expr->whenExpression;
        auto then_expr = casealt_expr->thenExpression;
        e_when = lExprScalarExpressionDuckDB(when_expr.get());
        e_then = lExprScalarExpressionDuckDB(then_expr.get());
    }

    auto else_expr = case_expr->getElseExpression();
    e_else = lExprScalarExpressionDuckDB(else_expr.get());
    return make_unique<duckdb::BoundCaseExpression>(move(e_when), move(e_then),
                                                    move(e_else));
}

unique_ptr<duckdb::Expression>
Planner::lExprScalarExistentialSubqueryExprDuckDB(
    kuzu::binder::Expression *expression)
{
    // D_ASSERT(false);
    // return nullptr;
    return make_unique<duckdb::BoundConstantExpression>(
        duckdb::Value(duckdb::LogicalType::BOOLEAN));
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


unique_ptr<duckdb::Expression> Planner::lExprScalarShortestPathExprDuckDB(
    kuzu::binder::Expression *expression)
{
    PathExpression *path_expr = (PathExpression *)expression;
    DataType type = path_expr->getDataType();
    duckdb::LogicalTypeId duckdb_type_id = (duckdb::LogicalTypeId)type.typeID;
    D_ASSERT(duckdb_type_id == duckdb::LogicalTypeId::PATH);
    // duckdb::LogicalType duckdb_type = duckdb::LogicalType(duckdb_type_id);
    duckdb::LogicalType duckdb_type = duckdb::LogicalType::PATH(duckdb::LogicalType::UBIGINT);
    
    return make_unique<duckdb::BoundReferenceExpression>(
        duckdb_type, 0 /* child_idx TODO: give proper value */);
}

}  // namespace s62