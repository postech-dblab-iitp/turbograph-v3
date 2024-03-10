
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"
#include "CypherParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by CypherParser.
 */
class  CypherListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterOC_Cypher(CypherParser::OC_CypherContext *ctx) = 0;
  virtual void exitOC_Cypher(CypherParser::OC_CypherContext *ctx) = 0;

  virtual void enterKU_CopyCSV(CypherParser::KU_CopyCSVContext *ctx) = 0;
  virtual void exitKU_CopyCSV(CypherParser::KU_CopyCSVContext *ctx) = 0;

  virtual void enterKU_ParsingOptions(CypherParser::KU_ParsingOptionsContext *ctx) = 0;
  virtual void exitKU_ParsingOptions(CypherParser::KU_ParsingOptionsContext *ctx) = 0;

  virtual void enterKU_ParsingOption(CypherParser::KU_ParsingOptionContext *ctx) = 0;
  virtual void exitKU_ParsingOption(CypherParser::KU_ParsingOptionContext *ctx) = 0;

  virtual void enterKU_DDL(CypherParser::KU_DDLContext *ctx) = 0;
  virtual void exitKU_DDL(CypherParser::KU_DDLContext *ctx) = 0;

  virtual void enterKU_CreateNode(CypherParser::KU_CreateNodeContext *ctx) = 0;
  virtual void exitKU_CreateNode(CypherParser::KU_CreateNodeContext *ctx) = 0;

  virtual void enterKU_CreateRel(CypherParser::KU_CreateRelContext *ctx) = 0;
  virtual void exitKU_CreateRel(CypherParser::KU_CreateRelContext *ctx) = 0;

  virtual void enterKU_DropTable(CypherParser::KU_DropTableContext *ctx) = 0;
  virtual void exitKU_DropTable(CypherParser::KU_DropTableContext *ctx) = 0;

  virtual void enterKU_RelConnections(CypherParser::KU_RelConnectionsContext *ctx) = 0;
  virtual void exitKU_RelConnections(CypherParser::KU_RelConnectionsContext *ctx) = 0;

  virtual void enterKU_RelConnection(CypherParser::KU_RelConnectionContext *ctx) = 0;
  virtual void exitKU_RelConnection(CypherParser::KU_RelConnectionContext *ctx) = 0;

  virtual void enterKU_NodeLabels(CypherParser::KU_NodeLabelsContext *ctx) = 0;
  virtual void exitKU_NodeLabels(CypherParser::KU_NodeLabelsContext *ctx) = 0;

  virtual void enterKU_PropertyDefinitions(CypherParser::KU_PropertyDefinitionsContext *ctx) = 0;
  virtual void exitKU_PropertyDefinitions(CypherParser::KU_PropertyDefinitionsContext *ctx) = 0;

  virtual void enterKU_PropertyDefinition(CypherParser::KU_PropertyDefinitionContext *ctx) = 0;
  virtual void exitKU_PropertyDefinition(CypherParser::KU_PropertyDefinitionContext *ctx) = 0;

  virtual void enterKU_CreateNodeConstraint(CypherParser::KU_CreateNodeConstraintContext *ctx) = 0;
  virtual void exitKU_CreateNodeConstraint(CypherParser::KU_CreateNodeConstraintContext *ctx) = 0;

  virtual void enterKU_DataType(CypherParser::KU_DataTypeContext *ctx) = 0;
  virtual void exitKU_DataType(CypherParser::KU_DataTypeContext *ctx) = 0;

  virtual void enterKU_ListIdentifiers(CypherParser::KU_ListIdentifiersContext *ctx) = 0;
  virtual void exitKU_ListIdentifiers(CypherParser::KU_ListIdentifiersContext *ctx) = 0;

  virtual void enterKU_ListIdentifier(CypherParser::KU_ListIdentifierContext *ctx) = 0;
  virtual void exitKU_ListIdentifier(CypherParser::KU_ListIdentifierContext *ctx) = 0;

  virtual void enterOC_AnyCypherOption(CypherParser::OC_AnyCypherOptionContext *ctx) = 0;
  virtual void exitOC_AnyCypherOption(CypherParser::OC_AnyCypherOptionContext *ctx) = 0;

  virtual void enterOC_Explain(CypherParser::OC_ExplainContext *ctx) = 0;
  virtual void exitOC_Explain(CypherParser::OC_ExplainContext *ctx) = 0;

  virtual void enterOC_Profile(CypherParser::OC_ProfileContext *ctx) = 0;
  virtual void exitOC_Profile(CypherParser::OC_ProfileContext *ctx) = 0;

  virtual void enterOC_Statement(CypherParser::OC_StatementContext *ctx) = 0;
  virtual void exitOC_Statement(CypherParser::OC_StatementContext *ctx) = 0;

  virtual void enterOC_Query(CypherParser::OC_QueryContext *ctx) = 0;
  virtual void exitOC_Query(CypherParser::OC_QueryContext *ctx) = 0;

  virtual void enterOC_RegularQuery(CypherParser::OC_RegularQueryContext *ctx) = 0;
  virtual void exitOC_RegularQuery(CypherParser::OC_RegularQueryContext *ctx) = 0;

  virtual void enterOC_Union(CypherParser::OC_UnionContext *ctx) = 0;
  virtual void exitOC_Union(CypherParser::OC_UnionContext *ctx) = 0;

  virtual void enterOC_SingleQuery(CypherParser::OC_SingleQueryContext *ctx) = 0;
  virtual void exitOC_SingleQuery(CypherParser::OC_SingleQueryContext *ctx) = 0;

  virtual void enterOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *ctx) = 0;
  virtual void exitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext *ctx) = 0;

  virtual void enterOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *ctx) = 0;
  virtual void exitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext *ctx) = 0;

  virtual void enterKU_QueryPart(CypherParser::KU_QueryPartContext *ctx) = 0;
  virtual void exitKU_QueryPart(CypherParser::KU_QueryPartContext *ctx) = 0;

  virtual void enterOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *ctx) = 0;
  virtual void exitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext *ctx) = 0;

  virtual void enterOC_ReadingClause(CypherParser::OC_ReadingClauseContext *ctx) = 0;
  virtual void exitOC_ReadingClause(CypherParser::OC_ReadingClauseContext *ctx) = 0;

  virtual void enterOC_Match(CypherParser::OC_MatchContext *ctx) = 0;
  virtual void exitOC_Match(CypherParser::OC_MatchContext *ctx) = 0;

  virtual void enterOC_Unwind(CypherParser::OC_UnwindContext *ctx) = 0;
  virtual void exitOC_Unwind(CypherParser::OC_UnwindContext *ctx) = 0;

  virtual void enterOC_Create(CypherParser::OC_CreateContext *ctx) = 0;
  virtual void exitOC_Create(CypherParser::OC_CreateContext *ctx) = 0;

  virtual void enterOC_Set(CypherParser::OC_SetContext *ctx) = 0;
  virtual void exitOC_Set(CypherParser::OC_SetContext *ctx) = 0;

  virtual void enterOC_SetItem(CypherParser::OC_SetItemContext *ctx) = 0;
  virtual void exitOC_SetItem(CypherParser::OC_SetItemContext *ctx) = 0;

  virtual void enterOC_Delete(CypherParser::OC_DeleteContext *ctx) = 0;
  virtual void exitOC_Delete(CypherParser::OC_DeleteContext *ctx) = 0;

  virtual void enterOC_With(CypherParser::OC_WithContext *ctx) = 0;
  virtual void exitOC_With(CypherParser::OC_WithContext *ctx) = 0;

  virtual void enterOC_Return(CypherParser::OC_ReturnContext *ctx) = 0;
  virtual void exitOC_Return(CypherParser::OC_ReturnContext *ctx) = 0;

  virtual void enterOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *ctx) = 0;
  virtual void exitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext *ctx) = 0;

  virtual void enterOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *ctx) = 0;
  virtual void exitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext *ctx) = 0;

  virtual void enterOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *ctx) = 0;
  virtual void exitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext *ctx) = 0;

  virtual void enterOC_Order(CypherParser::OC_OrderContext *ctx) = 0;
  virtual void exitOC_Order(CypherParser::OC_OrderContext *ctx) = 0;

  virtual void enterOC_Skip(CypherParser::OC_SkipContext *ctx) = 0;
  virtual void exitOC_Skip(CypherParser::OC_SkipContext *ctx) = 0;

  virtual void enterOC_Limit(CypherParser::OC_LimitContext *ctx) = 0;
  virtual void exitOC_Limit(CypherParser::OC_LimitContext *ctx) = 0;

  virtual void enterOC_SortItem(CypherParser::OC_SortItemContext *ctx) = 0;
  virtual void exitOC_SortItem(CypherParser::OC_SortItemContext *ctx) = 0;

  virtual void enterOC_Where(CypherParser::OC_WhereContext *ctx) = 0;
  virtual void exitOC_Where(CypherParser::OC_WhereContext *ctx) = 0;

  virtual void enterOC_Pattern(CypherParser::OC_PatternContext *ctx) = 0;
  virtual void exitOC_Pattern(CypherParser::OC_PatternContext *ctx) = 0;

  virtual void enterOC_PatternPart(CypherParser::OC_PatternPartContext *ctx) = 0;
  virtual void exitOC_PatternPart(CypherParser::OC_PatternPartContext *ctx) = 0;

  virtual void enterOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *ctx) = 0;
  virtual void exitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext *ctx) = 0;

  virtual void enterOC_PatternElement(CypherParser::OC_PatternElementContext *ctx) = 0;
  virtual void exitOC_PatternElement(CypherParser::OC_PatternElementContext *ctx) = 0;

  virtual void enterOC_NodePattern(CypherParser::OC_NodePatternContext *ctx) = 0;
  virtual void exitOC_NodePattern(CypherParser::OC_NodePatternContext *ctx) = 0;

  virtual void enterOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *ctx) = 0;
  virtual void exitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext *ctx) = 0;

  virtual void enterOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *ctx) = 0;
  virtual void exitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext *ctx) = 0;

  virtual void enterOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *ctx) = 0;
  virtual void exitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext *ctx) = 0;

  virtual void enterKU_Properties(CypherParser::KU_PropertiesContext *ctx) = 0;
  virtual void exitKU_Properties(CypherParser::KU_PropertiesContext *ctx) = 0;

  virtual void enterOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *ctx) = 0;
  virtual void exitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext *ctx) = 0;

  virtual void enterOC_NodeLabels(CypherParser::OC_NodeLabelsContext *ctx) = 0;
  virtual void exitOC_NodeLabels(CypherParser::OC_NodeLabelsContext *ctx) = 0;

  virtual void enterOC_NodeLabel(CypherParser::OC_NodeLabelContext *ctx) = 0;
  virtual void exitOC_NodeLabel(CypherParser::OC_NodeLabelContext *ctx) = 0;

  virtual void enterOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *ctx) = 0;
  virtual void exitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext *ctx) = 0;

  virtual void enterOC_RangeStartLiteral(CypherParser::OC_RangeStartLiteralContext *ctx) = 0;
  virtual void exitOC_RangeStartLiteral(CypherParser::OC_RangeStartLiteralContext *ctx) = 0;

  virtual void enterOC_RangeEndLiteral(CypherParser::OC_RangeEndLiteralContext *ctx) = 0;
  virtual void exitOC_RangeEndLiteral(CypherParser::OC_RangeEndLiteralContext *ctx) = 0;

  virtual void enterOC_LabelName(CypherParser::OC_LabelNameContext *ctx) = 0;
  virtual void exitOC_LabelName(CypherParser::OC_LabelNameContext *ctx) = 0;

  virtual void enterOC_RelTypeName(CypherParser::OC_RelTypeNameContext *ctx) = 0;
  virtual void exitOC_RelTypeName(CypherParser::OC_RelTypeNameContext *ctx) = 0;

  virtual void enterOC_Expression(CypherParser::OC_ExpressionContext *ctx) = 0;
  virtual void exitOC_Expression(CypherParser::OC_ExpressionContext *ctx) = 0;

  virtual void enterOC_OrExpression(CypherParser::OC_OrExpressionContext *ctx) = 0;
  virtual void exitOC_OrExpression(CypherParser::OC_OrExpressionContext *ctx) = 0;

  virtual void enterOC_XorExpression(CypherParser::OC_XorExpressionContext *ctx) = 0;
  virtual void exitOC_XorExpression(CypherParser::OC_XorExpressionContext *ctx) = 0;

  virtual void enterOC_AndExpression(CypherParser::OC_AndExpressionContext *ctx) = 0;
  virtual void exitOC_AndExpression(CypherParser::OC_AndExpressionContext *ctx) = 0;

  virtual void enterOC_NotExpression(CypherParser::OC_NotExpressionContext *ctx) = 0;
  virtual void exitOC_NotExpression(CypherParser::OC_NotExpressionContext *ctx) = 0;

  virtual void enterOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *ctx) = 0;
  virtual void exitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext *ctx) = 0;

  virtual void enterKU_ComparisonOperator(CypherParser::KU_ComparisonOperatorContext *ctx) = 0;
  virtual void exitKU_ComparisonOperator(CypherParser::KU_ComparisonOperatorContext *ctx) = 0;

  virtual void enterKU_BitwiseOrOperatorExpression(CypherParser::KU_BitwiseOrOperatorExpressionContext *ctx) = 0;
  virtual void exitKU_BitwiseOrOperatorExpression(CypherParser::KU_BitwiseOrOperatorExpressionContext *ctx) = 0;

  virtual void enterKU_BitwiseAndOperatorExpression(CypherParser::KU_BitwiseAndOperatorExpressionContext *ctx) = 0;
  virtual void exitKU_BitwiseAndOperatorExpression(CypherParser::KU_BitwiseAndOperatorExpressionContext *ctx) = 0;

  virtual void enterKU_BitShiftOperatorExpression(CypherParser::KU_BitShiftOperatorExpressionContext *ctx) = 0;
  virtual void exitKU_BitShiftOperatorExpression(CypherParser::KU_BitShiftOperatorExpressionContext *ctx) = 0;

  virtual void enterKU_BitShiftOperator(CypherParser::KU_BitShiftOperatorContext *ctx) = 0;
  virtual void exitKU_BitShiftOperator(CypherParser::KU_BitShiftOperatorContext *ctx) = 0;

  virtual void enterOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *ctx) = 0;
  virtual void exitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext *ctx) = 0;

  virtual void enterKU_AddOrSubtractOperator(CypherParser::KU_AddOrSubtractOperatorContext *ctx) = 0;
  virtual void exitKU_AddOrSubtractOperator(CypherParser::KU_AddOrSubtractOperatorContext *ctx) = 0;

  virtual void enterOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *ctx) = 0;
  virtual void exitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext *ctx) = 0;

  virtual void enterKU_MultiplyDivideModuloOperator(CypherParser::KU_MultiplyDivideModuloOperatorContext *ctx) = 0;
  virtual void exitKU_MultiplyDivideModuloOperator(CypherParser::KU_MultiplyDivideModuloOperatorContext *ctx) = 0;

  virtual void enterOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *ctx) = 0;
  virtual void exitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext *ctx) = 0;

  virtual void enterOC_UnaryAddSubtractOrFactorialExpression(CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext *ctx) = 0;
  virtual void exitOC_UnaryAddSubtractOrFactorialExpression(CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext *ctx) = 0;

  virtual void enterOC_StringListNullOperatorExpression(CypherParser::OC_StringListNullOperatorExpressionContext *ctx) = 0;
  virtual void exitOC_StringListNullOperatorExpression(CypherParser::OC_StringListNullOperatorExpressionContext *ctx) = 0;

  virtual void enterOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *ctx) = 0;
  virtual void exitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext *ctx) = 0;

  virtual void enterKU_ListPropertyOrLabelsExpression(CypherParser::KU_ListPropertyOrLabelsExpressionContext *ctx) = 0;
  virtual void exitKU_ListPropertyOrLabelsExpression(CypherParser::KU_ListPropertyOrLabelsExpressionContext *ctx) = 0;

  virtual void enterKU_ListExtractOperatorExpression(CypherParser::KU_ListExtractOperatorExpressionContext *ctx) = 0;
  virtual void exitKU_ListExtractOperatorExpression(CypherParser::KU_ListExtractOperatorExpressionContext *ctx) = 0;

  virtual void enterKU_ListSliceOperatorExpression(CypherParser::KU_ListSliceOperatorExpressionContext *ctx) = 0;
  virtual void exitKU_ListSliceOperatorExpression(CypherParser::KU_ListSliceOperatorExpressionContext *ctx) = 0;

  virtual void enterOC_StringOperatorExpression(CypherParser::OC_StringOperatorExpressionContext *ctx) = 0;
  virtual void exitOC_StringOperatorExpression(CypherParser::OC_StringOperatorExpressionContext *ctx) = 0;

  virtual void enterOC_NullOperatorExpression(CypherParser::OC_NullOperatorExpressionContext *ctx) = 0;
  virtual void exitOC_NullOperatorExpression(CypherParser::OC_NullOperatorExpressionContext *ctx) = 0;

  virtual void enterOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext *ctx) = 0;
  virtual void exitOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext *ctx) = 0;

  virtual void enterOC_Atom(CypherParser::OC_AtomContext *ctx) = 0;
  virtual void exitOC_Atom(CypherParser::OC_AtomContext *ctx) = 0;

  virtual void enterOC_Literal(CypherParser::OC_LiteralContext *ctx) = 0;
  virtual void exitOC_Literal(CypherParser::OC_LiteralContext *ctx) = 0;

  virtual void enterOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *ctx) = 0;
  virtual void exitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext *ctx) = 0;

  virtual void enterOC_ListLiteral(CypherParser::OC_ListLiteralContext *ctx) = 0;
  virtual void exitOC_ListLiteral(CypherParser::OC_ListLiteralContext *ctx) = 0;

  virtual void enterOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *ctx) = 0;
  virtual void exitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext *ctx) = 0;

  virtual void enterOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *ctx) = 0;
  virtual void exitOC_RelationshipsPattern(CypherParser::OC_RelationshipsPatternContext *ctx) = 0;

  virtual void enterOC_FilterExpression(CypherParser::OC_FilterExpressionContext *ctx) = 0;
  virtual void exitOC_FilterExpression(CypherParser::OC_FilterExpressionContext *ctx) = 0;

  virtual void enterOC_IdInColl(CypherParser::OC_IdInCollContext *ctx) = 0;
  virtual void exitOC_IdInColl(CypherParser::OC_IdInCollContext *ctx) = 0;

  virtual void enterOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *ctx) = 0;
  virtual void exitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext *ctx) = 0;

  virtual void enterOC_FunctionName(CypherParser::OC_FunctionNameContext *ctx) = 0;
  virtual void exitOC_FunctionName(CypherParser::OC_FunctionNameContext *ctx) = 0;

  virtual void enterOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *ctx) = 0;
  virtual void exitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext *ctx) = 0;

  virtual void enterOC_ListComprehension(CypherParser::OC_ListComprehensionContext *ctx) = 0;
  virtual void exitOC_ListComprehension(CypherParser::OC_ListComprehensionContext *ctx) = 0;

  virtual void enterOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *ctx) = 0;
  virtual void exitOC_PatternComprehension(CypherParser::OC_PatternComprehensionContext *ctx) = 0;

  virtual void enterOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *ctx) = 0;
  virtual void exitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext *ctx) = 0;

  virtual void enterOC_CaseExpression(CypherParser::OC_CaseExpressionContext *ctx) = 0;
  virtual void exitOC_CaseExpression(CypherParser::OC_CaseExpressionContext *ctx) = 0;

  virtual void enterOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *ctx) = 0;
  virtual void exitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext *ctx) = 0;

  virtual void enterOC_Variable(CypherParser::OC_VariableContext *ctx) = 0;
  virtual void exitOC_Variable(CypherParser::OC_VariableContext *ctx) = 0;

  virtual void enterOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *ctx) = 0;
  virtual void exitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext *ctx) = 0;

  virtual void enterOC_Parameter(CypherParser::OC_ParameterContext *ctx) = 0;
  virtual void exitOC_Parameter(CypherParser::OC_ParameterContext *ctx) = 0;

  virtual void enterOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *ctx) = 0;
  virtual void exitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext *ctx) = 0;

  virtual void enterOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *ctx) = 0;
  virtual void exitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext *ctx) = 0;

  virtual void enterOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *ctx) = 0;
  virtual void exitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext *ctx) = 0;

  virtual void enterOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *ctx) = 0;
  virtual void exitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext *ctx) = 0;

  virtual void enterOC_SchemaName(CypherParser::OC_SchemaNameContext *ctx) = 0;
  virtual void exitOC_SchemaName(CypherParser::OC_SchemaNameContext *ctx) = 0;

  virtual void enterOC_SymbolicName(CypherParser::OC_SymbolicNameContext *ctx) = 0;
  virtual void exitOC_SymbolicName(CypherParser::OC_SymbolicNameContext *ctx) = 0;

  virtual void enterOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *ctx) = 0;
  virtual void exitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext *ctx) = 0;

  virtual void enterOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *ctx) = 0;
  virtual void exitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext *ctx) = 0;

  virtual void enterOC_Dash(CypherParser::OC_DashContext *ctx) = 0;
  virtual void exitOC_Dash(CypherParser::OC_DashContext *ctx) = 0;


};

