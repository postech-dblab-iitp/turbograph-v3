#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cypher_parser.h"
#pragma GCC diagnostic pop

#include "parser/cypher_statement.hpp"
#include "parser/tokens.hpp"
#include "parser/parsed_expression.hpp"

namespace duckdb {

class Transformer {

public:
    explicit Transformer(CypherParser::Ku_StatementsContext& root) : root{root} {}

    std::vector<std::shared_ptr<CypherStatement>> transform();

private:
    std::unique_ptr<CypherStatement> transformStatement(CypherParser::OC_StatementContext& ctx);
    
    // Transform where.
    std::unique_ptr<ParsedExpression> transformWhere(CypherParser::OC_WhereContext& ctx);

    // Transform common.
    std::string transformVariable(CypherParser::OC_VariableContext& ctx);
    std::string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);
    std::string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);
    std::string transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral);

    // Transform query statement.
    std::unique_ptr<CypherStatement> transformQuery(CypherParser::OC_QueryContext& ctx);
    std::unique_ptr<CypherStatement> transformRegularQuery(CypherParser::OC_RegularQueryContext& ctx);
    std::unique_ptr<SingleQuery> transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);
    std::unique_ptr<SingleQuery>  transformSinglePartQuery(CypherParser::OC_SinglePartQueryContext& ctx);
    std::unique_ptr<QueryPart> transformQueryPart(CypherParser::KU_QueryPartContext& ctx);

    // Transform reading.
    std::unique_ptr<ReadingClause> transformReadingClause(
        CypherParser::OC_ReadingClauseContext& ctx);
    std::unique_ptr<ReadingClause> transformMatch(CypherParser::OC_MatchContext& ctx);

    // Transform graph pattern.
    std::vector<PatternElement> transformPattern(CypherParser::OC_PatternContext& ctx);
    PatternElement transformPatternPart(CypherParser::OC_PatternPartContext& ctx);
    PatternElement transformAnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext& ctx);
    PatternElement transformPatternElement(CypherParser::OC_PatternElementContext& ctx);
    std::unique_ptr<NodePattern> transformNodePattern(CypherParser::OC_NodePatternContext& ctx);
    std::unique_ptr<PatternElementChain> transformPatternElementChain(
        CypherParser::OC_PatternElementChainContext& ctx);
    std::unique_ptr<RelPattern> transformRelationshipPattern(CypherParser::OC_RelationshipPatternContext& ctx);
    std::vector<ParsedPropertyKeyValue> transformProperties(CypherParser::KU_PropertiesContext& ctx);
    std::vector<std::string> transformRelTypes(CypherParser::OC_RelationshipTypesContext& ctx);
    std::vector<NodeLabel> transformNodeLabels(CypherParser::OC_NodeLabelsContext& ctx);
    NodeLabel transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx);
    NodeLabel transformLabelName(CypherParser::OC_LabelNameContext& ctx);
    EdgeType transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx);

    // Transform projection.
    std::unique_ptr<WithClause> transformWith(CypherParser::OC_WithContext& ctx);
    std::unique_ptr<ReturnClause> transformReturn(CypherParser::OC_ReturnContext& ctx);
    std::unique_ptr<ProjectionBody> transformProjectionBody(CypherParser::OC_ProjectionBodyContext& ctx);
    std::vector<std::unique_ptr<ParsedExpression>> transformProjectionItems(
        CypherParser::OC_ProjectionItemsContext& ctx);
    std::unique_ptr<ParsedExpression> transformProjectionItem(
        CypherParser::OC_ProjectionItemContext& ctx);

    // Transform Expression.
    std::unique_ptr<ParsedExpression> transformExpression(CypherParser::OC_ExpressionContext& ctx); 
    std::unique_ptr<ParsedExpression> transformOrExpression(
        CypherParser::OC_OrExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformXorExpression(
        CypherParser::OC_XorExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformAndExpression(
        CypherParser::OC_AndExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformNotExpression(
        CypherParser::OC_NotExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformComparisonExpression(
        CypherParser::OC_ComparisonExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformBitwiseOrOperatorExpression(
        CypherParser::KU_BitwiseOrOperatorExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformBitwiseAndOperatorExpression(
        CypherParser::KU_BitwiseAndOperatorExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformBitShiftOperatorExpression(
        CypherParser::KU_BitShiftOperatorExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformAddOrSubtractExpression(
        CypherParser::OC_AddOrSubtractExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformMultiplyDivideModuloExpression(
        CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformPowerOfExpression(
        CypherParser::OC_PowerOfExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformUnaryAddSubtractOrFactorialExpression(
        CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformStringListNullOperatorExpression(
        CypherParser::OC_StringListNullOperatorExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformStringOperatorExpression(
        CypherParser::OC_StringOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> propertyExpression);
    std::unique_ptr<ParsedExpression> transformListOperatorExpression(
        CypherParser::OC_ListOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> childExpression);
    std::unique_ptr<ParsedExpression> transformNullOperatorExpression(
        CypherParser::OC_NullOperatorExpressionContext& ctx,
        std::unique_ptr<ParsedExpression> propertyExpression);
    std::unique_ptr<ParsedExpression> transformPropertyOrLabelsExpression(
        CypherParser::OC_PropertyOrLabelsExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformAtom(CypherParser::OC_AtomContext& ctx);
    std::unique_ptr<ParsedExpression> transformLiteral(CypherParser::OC_LiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformBooleanLiteral(
        CypherParser::OC_BooleanLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformListLiteral(
        CypherParser::OC_ListLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformStructLiteral(
        CypherParser::KU_StructLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformParameterExpression(
        CypherParser::OC_ParameterContext& ctx);
    std::unique_ptr<ParsedExpression> transformParenthesizedExpression(
        CypherParser::OC_ParenthesizedExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformFunctionInvocation(
        CypherParser::OC_FunctionInvocationContext& ctx);
    std::string transformFunctionName(CypherParser::OC_FunctionNameContext& ctx);
    std::vector<std::string> transformLambdaVariables(CypherParser::KU_LambdaVarsContext& ctx);
    std::unique_ptr<ParsedExpression> transformLambdaParameter(
        CypherParser::KU_LambdaParameterContext& ctx);
    std::unique_ptr<ParsedExpression> transformFunctionParameterExpression(
        CypherParser::KU_FunctionParameterContext& ctx);
    std::unique_ptr<ParsedExpression> transformPathPattern(
        CypherParser::OC_PathPatternsContext& ctx);
    std::unique_ptr<ParsedExpression> transformExistCountSubquery(
        CypherParser::OC_ExistCountSubqueryContext& ctx);
    std::unique_ptr<ParsedExpression> transformOcQuantifier(
        CypherParser::OC_QuantifierContext& ctx);
    std::unique_ptr<ParsedExpression> createPropertyExpression(
        CypherParser::OC_PropertyLookupContext& ctx, std::unique_ptr<ParsedExpression> child);
    std::unique_ptr<ParsedExpression> transformCaseExpression(
        CypherParser::OC_CaseExpressionContext& ctx);
    std::unique_ptr<ParsedExpression> transformNumberLiteral(
        CypherParser::OC_NumberLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformProperty(
        CypherParser::OC_PropertyExpressionContext& ctx);
    std::string transformPropertyKeyName(CypherParser::OC_PropertyKeyNameContext& ctx);
    std::unique_ptr<ParsedExpression> transformIntegerLiteral(
        CypherParser::OC_IntegerLiteralContext& ctx);
    std::unique_ptr<ParsedExpression> transformDoubleLiteral(
        CypherParser::OC_DoubleLiteralContext& ctx);

private:
    CypherParser::Ku_StatementsContext& root;
};

}