#include "parser/transformer.hpp"
#include "common/operator/cast_operators.hpp"
#include "parser/query/graph_pattern/node_pattern.hpp"
#include "parser/query/graph_pattern/pattern_element.hpp"
#include "parser/expression/list.hpp"

namespace duckdb {

std::unique_ptr<ParsedExpression> Transformer::transformExpression(
    CypherParser::OC_ExpressionContext& ctx) {
    return transformOrExpression(*ctx.oC_OrExpression());
}

std::unique_ptr<ParsedExpression> Transformer::transformOrExpression(CypherParser::OC_OrExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& xorExpression : ctx.oC_XorExpression()) {
        auto next = transformXorExpression(*xorExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            expression = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(expression), std::move(next));
        }
    }
    return expression;
}


std::unique_ptr<ParsedExpression> Transformer::transformXorExpression(CypherParser::OC_XorExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& andExpression : ctx.oC_AndExpression()) {
        auto next = transformAndExpression(*andExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            throw ParserException("XOR is not supported.");
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformAndExpression(CypherParser::OC_AndExpressionContext& ctx) {
    std::unique_ptr<ParsedExpression> expression;
    for (auto& notExpression : ctx.oC_NotExpression()) {
        auto next = transformNotExpression(*notExpression);
        if (!expression) {
            expression = std::move(next);
        } else {
            expression = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expression), std::move(next));
        }
    }
    return expression;
}

std::unique_ptr<ParsedExpression> Transformer::transformNotExpression(CypherParser::OC_NotExpressionContext& ctx) {
    auto result = transformComparisonExpression(*ctx.oC_ComparisonExpression());
    if (!ctx.NOT().empty()) {
        for ([[maybe_unused]] auto& _ : ctx.NOT()) {
            result = make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(result));
        }
    }
    return result;
}

std::unique_ptr<ParsedExpression> Transformer::transformComparisonExpression(CypherParser::OC_ComparisonExpressionContext& ctx) {
    if (ctx.kU_BitwiseOrOperatorExpression().size() == 1) {
        return transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    }

    auto left = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(0));
    auto right = transformBitwiseOrOperatorExpression(*ctx.kU_BitwiseOrOperatorExpression(1));
    auto comparisonOperator = ctx.kU_ComparisonOperator()[0]->getText();

    if (comparisonOperator == "=") {
        return make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(left), std::move(right));
    } else if (comparisonOperator == "<>") {
        return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(left), std::move(right));
    } else if (comparisonOperator == ">") {
        return make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(left), std::move(right));
    } else if (comparisonOperator == ">=") {
        return make_unique<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(left), std::move(right));
    } else if (comparisonOperator == "<") {
        return make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, std::move(left), std::move(right));
    } else {
        return make_unique<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(left), std::move(right));
    }
}

unique_ptr<ParsedExpression> Transformer::transformBitwiseOrOperatorExpression(
    CypherParser::KU_BitwiseOrOperatorExpressionContext &ctx) {

    unique_ptr<ParsedExpression> expression;
    for (idx_t i = 0; i < ctx.kU_BitwiseAndOperatorExpression().size(); ++i) {
        auto next = transformBitwiseAndOperatorExpression(*ctx.kU_BitwiseAndOperatorExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            vector<unique_ptr<ParsedExpression>> children;
            children.push_back(move(expression));
            children.push_back(move(next));
            expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "|", move(children));
        }
    }
    return expression;
}

// Transform a Bitwise AND (&)
unique_ptr<ParsedExpression> Transformer::transformBitwiseAndOperatorExpression(
    CypherParser::KU_BitwiseAndOperatorExpressionContext &ctx) {

    unique_ptr<ParsedExpression> expression;
    for (idx_t i = 0; i < ctx.kU_BitShiftOperatorExpression().size(); ++i) {
        auto next = transformBitShiftOperatorExpression(*ctx.kU_BitShiftOperatorExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            vector<unique_ptr<ParsedExpression>> children;
            children.push_back(move(expression));
            children.push_back(move(next));
            expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "&", move(children));
        }
    }
    return expression;
}

// Transform a Bitwise Shift (<<, >>)
unique_ptr<ParsedExpression> Transformer::transformBitShiftOperatorExpression(
    CypherParser::KU_BitShiftOperatorExpressionContext &ctx) {

    unique_ptr<ParsedExpression> expression;
    for (idx_t i = 0; i < ctx.oC_AddOrSubtractExpression().size(); ++i) {
        auto next = transformAddOrSubtractExpression(*ctx.oC_AddOrSubtractExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            string bitShiftOperator = ctx.kU_BitShiftOperator(i - 1)->getText();
            vector<unique_ptr<ParsedExpression>> children;
            children.push_back(move(expression));
            children.push_back(move(next));

            if (bitShiftOperator == "<<") {
                expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "<<", move(children));
            } else {
                expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, ">>", move(children));
            }
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformAddOrSubtractExpression(
    CypherParser::OC_AddOrSubtractExpressionContext &ctx) {

    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_MultiplyDivideModuloExpression().size(); ++i) {
        auto next = transformMultiplyDivideModuloExpression(*ctx.oC_MultiplyDivideModuloExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            string op = ctx.kU_AddOrSubtractOperator(i - 1)->getText();
            vector<unique_ptr<ParsedExpression>> children;
            children.push_back(move(expression));
            children.push_back(move(next));
            expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, op, move(children));
        }
    }
    return expression;
}


unique_ptr<ParsedExpression> Transformer::transformMultiplyDivideModuloExpression(
    CypherParser::OC_MultiplyDivideModuloExpressionContext &ctx) {

    unique_ptr<ParsedExpression> expression;
    for (auto i = 0ul; i < ctx.oC_PowerOfExpression().size(); ++i) {
        auto next = transformPowerOfExpression(*ctx.oC_PowerOfExpression(i));
        if (!expression) {
            expression = move(next);
        } else {
            string op = ctx.kU_MultiplyDivideModuloOperator(i - 1)->getText();
            vector<unique_ptr<ParsedExpression>> children;
            children.push_back(move(expression));
            children.push_back(move(next));
            expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, op, move(children));
        }
    }
    return expression;
}


unique_ptr<ParsedExpression> Transformer::transformPowerOfExpression(
    CypherParser::OC_PowerOfExpressionContext &ctx) {

    unique_ptr<ParsedExpression> expression;
    for (auto &unaryExpr : ctx.oC_UnaryAddSubtractOrFactorialExpression()) {
        auto next = transformUnaryAddSubtractOrFactorialExpression(*unaryExpr);
        if (!expression) {
            expression = move(next);
        } else {
            vector<unique_ptr<ParsedExpression>> children;
            children.push_back(move(expression));
            children.push_back(move(next));
            expression = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "pow", move(children));
        }
    }
    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformUnaryAddSubtractOrFactorialExpression(
    CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext &ctx) {

    auto result = transformStringListNullOperatorExpression(*ctx.oC_StringListNullOperatorExpression());
    if (ctx.FACTORIAL()) {
        vector<unique_ptr<ParsedExpression>> children;
        children.push_back(move(result));
        result = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "factorial", move(children));
    }
    if (!ctx.MINUS().empty()) {
        throw ParserException("Unary minus is not supported.");
    }
    return result;
}

unique_ptr<ParsedExpression> Transformer::transformStringListNullOperatorExpression(
    CypherParser::OC_StringListNullOperatorExpressionContext &ctx) {

    auto property_expr = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());

    if (ctx.oC_NullOperatorExpression()) {
        return transformNullOperatorExpression(*ctx.oC_NullOperatorExpression(), move(property_expr));
    }
    if (!ctx.oC_ListOperatorExpression().empty()) {
        auto result = transformListOperatorExpression(*ctx.oC_ListOperatorExpression(0), move(property_expr));
        for (size_t i = 1; i < ctx.oC_ListOperatorExpression().size(); ++i) {
            result = transformListOperatorExpression(*ctx.oC_ListOperatorExpression(i), move(result));
        }
        return result;
    }
    if (ctx.oC_StringOperatorExpression()) {
        return transformStringOperatorExpression(*ctx.oC_StringOperatorExpression(), move(property_expr));
    }
    return property_expr;
}

unique_ptr<ParsedExpression> Transformer::transformStringOperatorExpression(
    CypherParser::OC_StringOperatorExpressionContext &ctx,
    unique_ptr<ParsedExpression> property_expr) {

    auto right = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
    vector<unique_ptr<ParsedExpression>> children;
    children.push_back(move(property_expr));
    children.push_back(move(right));

    if (ctx.STARTS()) {
        return make_unique<FunctionExpression>(DEFAULT_SCHEMA, "startswith", move(children));
    } else if (ctx.ENDS()) {
        return make_unique<FunctionExpression>(DEFAULT_SCHEMA, "endswith", move(children));
    } else if (ctx.CONTAINS()) {
        return make_unique<FunctionExpression>(DEFAULT_SCHEMA, "contains", move(children));
    } else {
        return make_unique<FunctionExpression>(DEFAULT_SCHEMA, "regexp_matches", move(children));
    }
}

unique_ptr<ParsedExpression> Transformer::transformListOperatorExpression(
    CypherParser::OC_ListOperatorExpressionContext &ctx, unique_ptr<ParsedExpression> child) {

    if (ctx.IN()) {  // `x IN y`
        auto right = transformPropertyOrLabelsExpression(*ctx.oC_PropertyOrLabelsExpression());
        vector<unique_ptr<ParsedExpression>> children;
        children.push_back(move(child));
        children.push_back(move(right));
        return make_unique<FunctionExpression>(DEFAULT_SCHEMA, "list_contains", move(children));
    }

    if (ctx.COLON()) {  // `x[:]`
        throw ParserException("List slice is not supported.");  
    }

    throw ParserException("List extract is not supported.");
}

unique_ptr<ParsedExpression> Transformer::transformNullOperatorExpression(
    CypherParser::OC_NullOperatorExpressionContext &ctx,
    unique_ptr<ParsedExpression> property_expression) {

    ExpressionType expr_type = ctx.NOT() ? ExpressionType::OPERATOR_IS_NOT_NULL : ExpressionType::OPERATOR_IS_NULL;
    return make_unique<OperatorExpression>(expr_type, move(property_expression));
}

unique_ptr<ParsedExpression> Transformer::transformPropertyOrLabelsExpression(
    CypherParser::OC_PropertyOrLabelsExpressionContext &ctx) {

    auto atom = transformAtom(*ctx.oC_Atom());

    if (!ctx.oC_PropertyLookup().empty()) {
        auto lookUpCtx = ctx.oC_PropertyLookup(0);
        auto result = createPropertyExpression(*lookUpCtx, move(atom));
        for (auto i = 1u; i < ctx.oC_PropertyLookup().size(); ++i) {
            lookUpCtx = ctx.oC_PropertyLookup(i);
            result = createPropertyExpression(*lookUpCtx, move(result));
        }
        return result;
    }

    return atom;
}

unique_ptr<ParsedExpression> Transformer::transformAtom(CypherParser::OC_AtomContext &ctx) {
    if (ctx.oC_Literal()) {
        return transformLiteral(*ctx.oC_Literal());
    } else if (ctx.oC_Parameter()) {
        return transformParameterExpression(*ctx.oC_Parameter());
    } else if (ctx.oC_CaseExpression()) {
        return transformCaseExpression(*ctx.oC_CaseExpression());
    } else if (ctx.oC_ParenthesizedExpression()) {
        return transformParenthesizedExpression(*ctx.oC_ParenthesizedExpression());
    } else if (ctx.oC_FunctionInvocation()) {
        return transformFunctionInvocation(*ctx.oC_FunctionInvocation());
    } else if (ctx.oC_PathPatterns()) {
        return transformPathPattern(*ctx.oC_PathPatterns());
    } else if (ctx.oC_ExistCountSubquery()) {
        return transformExistCountSubquery(*ctx.oC_ExistCountSubquery());
    } else if (ctx.oC_Quantifier()) {
        return transformOcQuantifier(*ctx.oC_Quantifier());
    } else {
        return make_unique<VariableExpression>(transformVariable(*ctx.oC_Variable()));
    }
}

unique_ptr<ParsedExpression> Transformer::transformLiteral(CypherParser::OC_LiteralContext &ctx) {
    if (ctx.oC_NumberLiteral()) {
        return transformNumberLiteral(*ctx.oC_NumberLiteral());
    } else if (ctx.oC_BooleanLiteral()) {
        return transformBooleanLiteral(*ctx.oC_BooleanLiteral());
    } else if (ctx.StringLiteral()) {
        return make_unique<ConstantExpression>(Value(ctx.StringLiteral()->getText()));
    } else if (ctx.NULL_()) {
        return make_unique<ConstantExpression>(Value(nullptr));
    } else if (ctx.kU_StructLiteral()) {
        return transformStructLiteral(*ctx.kU_StructLiteral());
    } else {
        return transformListLiteral(*ctx.oC_ListLiteral());
    }
}

unique_ptr<ParsedExpression> Transformer::transformBooleanLiteral(CypherParser::OC_BooleanLiteralContext &ctx) {
    if (ctx.TRUE()) {
        return make_unique<ConstantExpression>(Value::BOOLEAN(true));
    } else if (ctx.FALSE()) {
        return make_unique<ConstantExpression>(Value::BOOLEAN(false));
    }
}

unique_ptr<ParsedExpression> Transformer::transformListLiteral(
    CypherParser::OC_ListLiteralContext &ctx) {
    throw ParserException("List literal is not supported.");
    return nullptr;
}

unique_ptr<ParsedExpression> Transformer::transformStructLiteral(
    CypherParser::KU_StructLiteralContext &ctx) {
    throw ParserException("Struct listral is not supported.");
    return nullptr;
}

unique_ptr<ParsedExpression> Transformer::transformParameterExpression(
    CypherParser::OC_ParameterContext &ctx) {

    auto parameter_name = ctx.oC_SymbolicName() ? ctx.oC_SymbolicName()->getText()
                                                : ctx.DecimalInteger()->getText();
    return make_unique<NamedParameterExpression>(parameter_name);
}

unique_ptr<ParsedExpression> Transformer::transformParenthesizedExpression(
    CypherParser::OC_ParenthesizedExpressionContext &ctx) {
    return transformExpression(*ctx.oC_Expression());
}

unique_ptr<ParsedExpression> Transformer::transformFunctionInvocation(
    CypherParser::OC_FunctionInvocationContext &ctx) {

    // Collect function parameters first
    vector<unique_ptr<ParsedExpression>> children;
    for (auto &function_parameter : ctx.kU_FunctionParameter()) {
        auto parsed_function_parameter = transformFunctionParameterExpression(*function_parameter);
        if (function_parameter->oC_SymbolicName()) {
            throw ParserException("Optional parameter is not supported.");
        }
        children.push_back(move(parsed_function_parameter));
    }

    // Handle special case: COUNT(*)
    if (ctx.STAR()) {
        return make_unique<FunctionExpression>(DEFAULT_SCHEMA, "count_star", move(children));
    }

    // Determine function name
    string function_name;
    if (ctx.COUNT()) {
        function_name = "count";
    } else if (ctx.CAST()) {
        throw ParserException("CAST is not supported.");
    } else {
        function_name = transformFunctionName(*ctx.oC_FunctionName());
    }

    // Construct FunctionExpression with children
    return make_unique<FunctionExpression>(DEFAULT_SCHEMA, function_name, move(children));
}

string Transformer::transformFunctionName(CypherParser::OC_FunctionNameContext &ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

vector<string> Transformer::transformLambdaVariables(CypherParser::KU_LambdaVarsContext &ctx) {
    vector<string> lambda_variables;
    lambda_variables.reserve(ctx.oC_SymbolicName().size());

    for (auto &var : ctx.oC_SymbolicName()) {
        lambda_variables.push_back(transformSymbolicName(*var));
    }

    return lambda_variables;
}

unique_ptr<ParsedExpression> Transformer::transformLambdaParameter(
    CypherParser::KU_LambdaParameterContext &ctx) {

    auto vars = transformLambdaVariables(*ctx.kU_LambdaVars());
    auto lambda_operation = transformExpression(*ctx.oC_Expression());

    throw ParserException("Lambda parameter is not supported.");
    return nullptr;
    // return make_unique<LambdaExpression>(move(vars), move(lambda_operation));
}

unique_ptr<ParsedExpression> Transformer::transformFunctionParameterExpression(
    CypherParser::KU_FunctionParameterContext &ctx) {

    if (ctx.kU_LambdaParameter()) {
        return transformLambdaParameter(*ctx.kU_LambdaParameter());
    }

    auto expression = transformExpression(*ctx.oC_Expression());
    if (ctx.oC_SymbolicName()) {
        expression->alias = transformSymbolicName(*ctx.oC_SymbolicName());
    }

    return expression;
}

unique_ptr<ParsedExpression> Transformer::transformPathPattern(CypherParser::OC_PathPatternsContext &ctx) {
    throw ParserException("Path pattern is not supported.");
    return nullptr;
    
    // auto subquery_expr = make_unique<SubqueryExpression>(SubqueryType::EXISTS);
    
    // auto pattern_element = PatternElement(transformNodePattern(*ctx.oC_NodePattern()));
    // for (auto &chain : ctx.oC_PatternElementChain()) {
    //     pattern_element.addPatternElementChain(transformPatternElementChain(*chain));
    // }
    
    // subquery_expr->subquery = move(pattern_element);
    // return subquery_expr;
}

unique_ptr<ParsedExpression> Transformer::transformExistCountSubquery(CypherParser::OC_ExistCountSubqueryContext &ctx) {
    throw ParserException("Exist count subquery is not supported.");
    return nullptr;

    // auto subquery_type = ctx.EXISTS() ? SubqueryType::EXISTS : SubqueryType::SCALAR;
    // auto subquery_expr = make_unique<SubqueryExpression>(subquery_type);

    // subquery_expr->subquery = transformPattern(*ctx.oC_Pattern());
    // if (ctx.oC_Where()) {
    //     subquery_expr->where_clause = transformWhere(*ctx.oC_Where());
    // }
    // return subquery_expr;
}

unique_ptr<ParsedExpression> Transformer::transformOcQuantifier(CypherParser::OC_QuantifierContext &ctx) {
    throw ParserException("Quantifier is not supported.");
    return nullptr;

    // auto variable = transformVariable(*ctx.oC_FilterExpression()->oC_IdInColl()->oC_Variable());
    // auto where_expr = transformWhere(*ctx.oC_FilterExpression()->oC_Where());
    
    // auto lambda_expr = make_unique<LambdaExpression>(vector<string>{variable}, move(where_expr));
    // auto list_expr = transformExpression(*ctx.oC_FilterExpression()->oC_IdInColl()->oC_Expression());

    // string quantifier_name;
    // if (ctx.ALL()) {
    //     quantifier_name = "ALL";
    // } else if (ctx.ANY()) {
    //     quantifier_name = "ANY";
    // } else if (ctx.NONE()) {
    //     quantifier_name = "NONE";
    // } else if (ctx.SINGLE()) {
    //     quantifier_name = "SINGLE";
    // }

    // return make_unique<FunctionExpression>(quantifier_name, move(list_expr), move(lambda_expr));
}

unique_ptr<ParsedExpression> Transformer::createPropertyExpression(CypherParser::OC_PropertyLookupContext &ctx, unique_ptr<ParsedExpression> child) {
    auto key = ctx.STAR() ? "*" : transformPropertyKeyName(*ctx.oC_PropertyKeyName());
    return make_unique<PropertyExpression>(key, move(child));
}

unique_ptr<ParsedExpression> Transformer::transformCaseExpression(CypherParser::OC_CaseExpressionContext &ctx) {
    auto case_expr = make_unique<CaseExpression>();

    unique_ptr<ParsedExpression> optional_case_expr = nullptr;
    unique_ptr<ParsedExpression> else_expr = nullptr;

    if (ctx.ELSE()) {
        if (ctx.oC_Expression().size() == 1) {
            else_expr = transformExpression(*ctx.oC_Expression(0));
        }
        else {
            D_ASSERT(ctx.oC_Expression().size() == 2);
            optional_case_expr = transformExpression(*ctx.oC_Expression(0));
            else_expr = transformExpression(*ctx.oC_Expression(1));
        }
    }
    else {
        if (ctx.oC_Expression().size() == 1) {
            optional_case_expr = transformExpression(*ctx.oC_Expression(0));
        }
    }

    // Transform CASE WHEN conditions
    for (auto &case_alternative : ctx.oC_CaseAlternative()) {
        CaseCheck case_check;
        case_check.when_expr = transformExpression(*case_alternative->oC_Expression(0));
        case_check.then_expr = transformExpression(*case_alternative->oC_Expression(1));
        case_expr->case_checks.push_back(move(case_check));
    }

    case_expr->optional_case_expr = move(optional_case_expr);
    case_expr->else_expr = move(else_expr);

    return case_expr;
}

unique_ptr<ParsedExpression> Transformer::transformNumberLiteral(CypherParser::OC_NumberLiteralContext &ctx) {
    if (ctx.oC_IntegerLiteral()) {
        return transformIntegerLiteral(*ctx.oC_IntegerLiteral());
    } else {
        D_ASSERT(ctx.oC_DoubleLiteral());
        return transformDoubleLiteral(*ctx.oC_DoubleLiteral());
    }
}

unique_ptr<ParsedExpression> Transformer::transformProperty(CypherParser::OC_PropertyExpressionContext &ctx) {
    auto child = transformAtom(*ctx.oC_Atom());
    return createPropertyExpression(*ctx.oC_PropertyLookup(), move(child));
}

std::string Transformer::transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx) {
    return transformSchemaName(*ctx.oC_SchemaName());
}

unique_ptr<ParsedExpression> Transformer::transformIntegerLiteral(CypherParser::OC_IntegerLiteralContext &ctx) {
    string text = ctx.DecimalInteger()->getText();
    string_t text_str(text);
    int64_t int_value;
    
    // Try to cast to BIGINT first
    if (TryCast::Operation<string_t, int64_t>(text_str, int_value)) {
        return make_unique<ConstantExpression>(Value::BIGINT(int_value));
    }
    
    // If BIGINT conversion fails, attempt to cast to HUGEINT
    hugeint_t hugeint_value;
    if (TryCast::Operation<string_t, hugeint_t>(text_str, hugeint_value)) {
        return make_unique<ConstantExpression>(Value::HUGEINT(hugeint_value));
    }
    
    throw NotImplementedException("Integer literal conversion failed: " + text);
}

unique_ptr<ParsedExpression> Transformer::transformDoubleLiteral(CypherParser::OC_DoubleLiteralContext &ctx) {
    string text = ctx.ExponentDecimalReal() ? ctx.ExponentDecimalReal()->getText() : ctx.RegularDecimalReal()->getText();
    string_t text_str(text);
    
    double dbl_value;
    if (!TryCast::Operation<string_t, double>(text_str, dbl_value)) {
        throw NotImplementedException("Double literal conversion failed: " + text);
    }

    return make_unique<ConstantExpression>(Value::DOUBLE(dbl_value));
}

}