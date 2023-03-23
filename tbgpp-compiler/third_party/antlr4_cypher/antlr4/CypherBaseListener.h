
// Generated from Cypher.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"
#include "CypherListener.h"


/**
 * This class provides an empty implementation of CypherListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  CypherBaseListener : public CypherListener {
public:

  virtual void enterOC_Cypher(CypherParser::OC_CypherContext * /*ctx*/) override { }
  virtual void exitOC_Cypher(CypherParser::OC_CypherContext * /*ctx*/) override { }

  virtual void enterKU_CopyCSV(CypherParser::KU_CopyCSVContext * /*ctx*/) override { }
  virtual void exitKU_CopyCSV(CypherParser::KU_CopyCSVContext * /*ctx*/) override { }

  virtual void enterKU_ParsingOptions(CypherParser::KU_ParsingOptionsContext * /*ctx*/) override { }
  virtual void exitKU_ParsingOptions(CypherParser::KU_ParsingOptionsContext * /*ctx*/) override { }

  virtual void enterKU_ParsingOption(CypherParser::KU_ParsingOptionContext * /*ctx*/) override { }
  virtual void exitKU_ParsingOption(CypherParser::KU_ParsingOptionContext * /*ctx*/) override { }

  virtual void enterKU_DDL(CypherParser::KU_DDLContext * /*ctx*/) override { }
  virtual void exitKU_DDL(CypherParser::KU_DDLContext * /*ctx*/) override { }

  virtual void enterKU_CreateNode(CypherParser::KU_CreateNodeContext * /*ctx*/) override { }
  virtual void exitKU_CreateNode(CypherParser::KU_CreateNodeContext * /*ctx*/) override { }

  virtual void enterKU_CreateRel(CypherParser::KU_CreateRelContext * /*ctx*/) override { }
  virtual void exitKU_CreateRel(CypherParser::KU_CreateRelContext * /*ctx*/) override { }

  virtual void enterKU_DropTable(CypherParser::KU_DropTableContext * /*ctx*/) override { }
  virtual void exitKU_DropTable(CypherParser::KU_DropTableContext * /*ctx*/) override { }

  virtual void enterKU_RelConnections(CypherParser::KU_RelConnectionsContext * /*ctx*/) override { }
  virtual void exitKU_RelConnections(CypherParser::KU_RelConnectionsContext * /*ctx*/) override { }

  virtual void enterKU_RelConnection(CypherParser::KU_RelConnectionContext * /*ctx*/) override { }
  virtual void exitKU_RelConnection(CypherParser::KU_RelConnectionContext * /*ctx*/) override { }

  virtual void enterKU_NodeLabels(CypherParser::KU_NodeLabelsContext * /*ctx*/) override { }
  virtual void exitKU_NodeLabels(CypherParser::KU_NodeLabelsContext * /*ctx*/) override { }

  virtual void enterKU_PropertyDefinitions(CypherParser::KU_PropertyDefinitionsContext * /*ctx*/) override { }
  virtual void exitKU_PropertyDefinitions(CypherParser::KU_PropertyDefinitionsContext * /*ctx*/) override { }

  virtual void enterKU_PropertyDefinition(CypherParser::KU_PropertyDefinitionContext * /*ctx*/) override { }
  virtual void exitKU_PropertyDefinition(CypherParser::KU_PropertyDefinitionContext * /*ctx*/) override { }

  virtual void enterKU_CreateNodeConstraint(CypherParser::KU_CreateNodeConstraintContext * /*ctx*/) override { }
  virtual void exitKU_CreateNodeConstraint(CypherParser::KU_CreateNodeConstraintContext * /*ctx*/) override { }

  virtual void enterKU_DataType(CypherParser::KU_DataTypeContext * /*ctx*/) override { }
  virtual void exitKU_DataType(CypherParser::KU_DataTypeContext * /*ctx*/) override { }

  virtual void enterKU_ListIdentifiers(CypherParser::KU_ListIdentifiersContext * /*ctx*/) override { }
  virtual void exitKU_ListIdentifiers(CypherParser::KU_ListIdentifiersContext * /*ctx*/) override { }

  virtual void enterKU_ListIdentifier(CypherParser::KU_ListIdentifierContext * /*ctx*/) override { }
  virtual void exitKU_ListIdentifier(CypherParser::KU_ListIdentifierContext * /*ctx*/) override { }

  virtual void enterOC_AnyCypherOption(CypherParser::OC_AnyCypherOptionContext * /*ctx*/) override { }
  virtual void exitOC_AnyCypherOption(CypherParser::OC_AnyCypherOptionContext * /*ctx*/) override { }

  virtual void enterOC_Explain(CypherParser::OC_ExplainContext * /*ctx*/) override { }
  virtual void exitOC_Explain(CypherParser::OC_ExplainContext * /*ctx*/) override { }

  virtual void enterOC_Profile(CypherParser::OC_ProfileContext * /*ctx*/) override { }
  virtual void exitOC_Profile(CypherParser::OC_ProfileContext * /*ctx*/) override { }

  virtual void enterOC_Statement(CypherParser::OC_StatementContext * /*ctx*/) override { }
  virtual void exitOC_Statement(CypherParser::OC_StatementContext * /*ctx*/) override { }

  virtual void enterOC_Query(CypherParser::OC_QueryContext * /*ctx*/) override { }
  virtual void exitOC_Query(CypherParser::OC_QueryContext * /*ctx*/) override { }

  virtual void enterOC_RegularQuery(CypherParser::OC_RegularQueryContext * /*ctx*/) override { }
  virtual void exitOC_RegularQuery(CypherParser::OC_RegularQueryContext * /*ctx*/) override { }

  virtual void enterOC_Union(CypherParser::OC_UnionContext * /*ctx*/) override { }
  virtual void exitOC_Union(CypherParser::OC_UnionContext * /*ctx*/) override { }

  virtual void enterOC_SingleQuery(CypherParser::OC_SingleQueryContext * /*ctx*/) override { }
  virtual void exitOC_SingleQuery(CypherParser::OC_SingleQueryContext * /*ctx*/) override { }

  virtual void enterOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext * /*ctx*/) override { }
  virtual void exitOC_SinglePartQuery(CypherParser::OC_SinglePartQueryContext * /*ctx*/) override { }

  virtual void enterOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext * /*ctx*/) override { }
  virtual void exitOC_MultiPartQuery(CypherParser::OC_MultiPartQueryContext * /*ctx*/) override { }

  virtual void enterKU_QueryPart(CypherParser::KU_QueryPartContext * /*ctx*/) override { }
  virtual void exitKU_QueryPart(CypherParser::KU_QueryPartContext * /*ctx*/) override { }

  virtual void enterOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext * /*ctx*/) override { }
  virtual void exitOC_UpdatingClause(CypherParser::OC_UpdatingClauseContext * /*ctx*/) override { }

  virtual void enterOC_ReadingClause(CypherParser::OC_ReadingClauseContext * /*ctx*/) override { }
  virtual void exitOC_ReadingClause(CypherParser::OC_ReadingClauseContext * /*ctx*/) override { }

  virtual void enterOC_Match(CypherParser::OC_MatchContext * /*ctx*/) override { }
  virtual void exitOC_Match(CypherParser::OC_MatchContext * /*ctx*/) override { }

  virtual void enterOC_Unwind(CypherParser::OC_UnwindContext * /*ctx*/) override { }
  virtual void exitOC_Unwind(CypherParser::OC_UnwindContext * /*ctx*/) override { }

  virtual void enterOC_Create(CypherParser::OC_CreateContext * /*ctx*/) override { }
  virtual void exitOC_Create(CypherParser::OC_CreateContext * /*ctx*/) override { }

  virtual void enterOC_Set(CypherParser::OC_SetContext * /*ctx*/) override { }
  virtual void exitOC_Set(CypherParser::OC_SetContext * /*ctx*/) override { }

  virtual void enterOC_SetItem(CypherParser::OC_SetItemContext * /*ctx*/) override { }
  virtual void exitOC_SetItem(CypherParser::OC_SetItemContext * /*ctx*/) override { }

  virtual void enterOC_Delete(CypherParser::OC_DeleteContext * /*ctx*/) override { }
  virtual void exitOC_Delete(CypherParser::OC_DeleteContext * /*ctx*/) override { }

  virtual void enterOC_With(CypherParser::OC_WithContext * /*ctx*/) override { }
  virtual void exitOC_With(CypherParser::OC_WithContext * /*ctx*/) override { }

  virtual void enterOC_Return(CypherParser::OC_ReturnContext * /*ctx*/) override { }
  virtual void exitOC_Return(CypherParser::OC_ReturnContext * /*ctx*/) override { }

  virtual void enterOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext * /*ctx*/) override { }
  virtual void exitOC_ProjectionBody(CypherParser::OC_ProjectionBodyContext * /*ctx*/) override { }

  virtual void enterOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext * /*ctx*/) override { }
  virtual void exitOC_ProjectionItems(CypherParser::OC_ProjectionItemsContext * /*ctx*/) override { }

  virtual void enterOC_ProjectionItem(CypherParser::OC_ProjectionItemContext * /*ctx*/) override { }
  virtual void exitOC_ProjectionItem(CypherParser::OC_ProjectionItemContext * /*ctx*/) override { }

  virtual void enterOC_Order(CypherParser::OC_OrderContext * /*ctx*/) override { }
  virtual void exitOC_Order(CypherParser::OC_OrderContext * /*ctx*/) override { }

  virtual void enterOC_Skip(CypherParser::OC_SkipContext * /*ctx*/) override { }
  virtual void exitOC_Skip(CypherParser::OC_SkipContext * /*ctx*/) override { }

  virtual void enterOC_Limit(CypherParser::OC_LimitContext * /*ctx*/) override { }
  virtual void exitOC_Limit(CypherParser::OC_LimitContext * /*ctx*/) override { }

  virtual void enterOC_SortItem(CypherParser::OC_SortItemContext * /*ctx*/) override { }
  virtual void exitOC_SortItem(CypherParser::OC_SortItemContext * /*ctx*/) override { }

  virtual void enterOC_Where(CypherParser::OC_WhereContext * /*ctx*/) override { }
  virtual void exitOC_Where(CypherParser::OC_WhereContext * /*ctx*/) override { }

  virtual void enterOC_Pattern(CypherParser::OC_PatternContext * /*ctx*/) override { }
  virtual void exitOC_Pattern(CypherParser::OC_PatternContext * /*ctx*/) override { }

  virtual void enterOC_PatternPart(CypherParser::OC_PatternPartContext * /*ctx*/) override { }
  virtual void exitOC_PatternPart(CypherParser::OC_PatternPartContext * /*ctx*/) override { }

  virtual void enterOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext * /*ctx*/) override { }
  virtual void exitOC_AnonymousPatternPart(CypherParser::OC_AnonymousPatternPartContext * /*ctx*/) override { }

  virtual void enterOC_PatternElement(CypherParser::OC_PatternElementContext * /*ctx*/) override { }
  virtual void exitOC_PatternElement(CypherParser::OC_PatternElementContext * /*ctx*/) override { }

  virtual void enterOC_NodePattern(CypherParser::OC_NodePatternContext * /*ctx*/) override { }
  virtual void exitOC_NodePattern(CypherParser::OC_NodePatternContext * /*ctx*/) override { }

  virtual void enterOC_PatternElementChain(CypherParser::OC_PatternElementChainContext * /*ctx*/) override { }
  virtual void exitOC_PatternElementChain(CypherParser::OC_PatternElementChainContext * /*ctx*/) override { }

  virtual void enterOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext * /*ctx*/) override { }
  virtual void exitOC_RelationshipPattern(CypherParser::OC_RelationshipPatternContext * /*ctx*/) override { }

  virtual void enterOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext * /*ctx*/) override { }
  virtual void exitOC_RelationshipDetail(CypherParser::OC_RelationshipDetailContext * /*ctx*/) override { }

  virtual void enterKU_Properties(CypherParser::KU_PropertiesContext * /*ctx*/) override { }
  virtual void exitKU_Properties(CypherParser::KU_PropertiesContext * /*ctx*/) override { }

  virtual void enterOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext * /*ctx*/) override { }
  virtual void exitOC_RelationshipTypes(CypherParser::OC_RelationshipTypesContext * /*ctx*/) override { }

  virtual void enterOC_NodeLabels(CypherParser::OC_NodeLabelsContext * /*ctx*/) override { }
  virtual void exitOC_NodeLabels(CypherParser::OC_NodeLabelsContext * /*ctx*/) override { }

  virtual void enterOC_NodeLabel(CypherParser::OC_NodeLabelContext * /*ctx*/) override { }
  virtual void exitOC_NodeLabel(CypherParser::OC_NodeLabelContext * /*ctx*/) override { }

  virtual void enterOC_RangeLiteral(CypherParser::OC_RangeLiteralContext * /*ctx*/) override { }
  virtual void exitOC_RangeLiteral(CypherParser::OC_RangeLiteralContext * /*ctx*/) override { }

  virtual void enterOC_RangeStartLiteral(CypherParser::OC_RangeStartLiteralContext * /*ctx*/) override { }
  virtual void exitOC_RangeStartLiteral(CypherParser::OC_RangeStartLiteralContext * /*ctx*/) override { }

  virtual void enterOC_RangeEndLiteral(CypherParser::OC_RangeEndLiteralContext * /*ctx*/) override { }
  virtual void exitOC_RangeEndLiteral(CypherParser::OC_RangeEndLiteralContext * /*ctx*/) override { }

  virtual void enterOC_LabelName(CypherParser::OC_LabelNameContext * /*ctx*/) override { }
  virtual void exitOC_LabelName(CypherParser::OC_LabelNameContext * /*ctx*/) override { }

  virtual void enterOC_RelTypeName(CypherParser::OC_RelTypeNameContext * /*ctx*/) override { }
  virtual void exitOC_RelTypeName(CypherParser::OC_RelTypeNameContext * /*ctx*/) override { }

  virtual void enterOC_Expression(CypherParser::OC_ExpressionContext * /*ctx*/) override { }
  virtual void exitOC_Expression(CypherParser::OC_ExpressionContext * /*ctx*/) override { }

  virtual void enterOC_OrExpression(CypherParser::OC_OrExpressionContext * /*ctx*/) override { }
  virtual void exitOC_OrExpression(CypherParser::OC_OrExpressionContext * /*ctx*/) override { }

  virtual void enterOC_XorExpression(CypherParser::OC_XorExpressionContext * /*ctx*/) override { }
  virtual void exitOC_XorExpression(CypherParser::OC_XorExpressionContext * /*ctx*/) override { }

  virtual void enterOC_AndExpression(CypherParser::OC_AndExpressionContext * /*ctx*/) override { }
  virtual void exitOC_AndExpression(CypherParser::OC_AndExpressionContext * /*ctx*/) override { }

  virtual void enterOC_NotExpression(CypherParser::OC_NotExpressionContext * /*ctx*/) override { }
  virtual void exitOC_NotExpression(CypherParser::OC_NotExpressionContext * /*ctx*/) override { }

  virtual void enterOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext * /*ctx*/) override { }
  virtual void exitOC_ComparisonExpression(CypherParser::OC_ComparisonExpressionContext * /*ctx*/) override { }

  virtual void enterKU_ComparisonOperator(CypherParser::KU_ComparisonOperatorContext * /*ctx*/) override { }
  virtual void exitKU_ComparisonOperator(CypherParser::KU_ComparisonOperatorContext * /*ctx*/) override { }

  virtual void enterKU_BitwiseOrOperatorExpression(CypherParser::KU_BitwiseOrOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitKU_BitwiseOrOperatorExpression(CypherParser::KU_BitwiseOrOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterKU_BitwiseAndOperatorExpression(CypherParser::KU_BitwiseAndOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitKU_BitwiseAndOperatorExpression(CypherParser::KU_BitwiseAndOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterKU_BitShiftOperatorExpression(CypherParser::KU_BitShiftOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitKU_BitShiftOperatorExpression(CypherParser::KU_BitShiftOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterKU_BitShiftOperator(CypherParser::KU_BitShiftOperatorContext * /*ctx*/) override { }
  virtual void exitKU_BitShiftOperator(CypherParser::KU_BitShiftOperatorContext * /*ctx*/) override { }

  virtual void enterOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext * /*ctx*/) override { }
  virtual void exitOC_AddOrSubtractExpression(CypherParser::OC_AddOrSubtractExpressionContext * /*ctx*/) override { }

  virtual void enterKU_AddOrSubtractOperator(CypherParser::KU_AddOrSubtractOperatorContext * /*ctx*/) override { }
  virtual void exitKU_AddOrSubtractOperator(CypherParser::KU_AddOrSubtractOperatorContext * /*ctx*/) override { }

  virtual void enterOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext * /*ctx*/) override { }
  virtual void exitOC_MultiplyDivideModuloExpression(CypherParser::OC_MultiplyDivideModuloExpressionContext * /*ctx*/) override { }

  virtual void enterKU_MultiplyDivideModuloOperator(CypherParser::KU_MultiplyDivideModuloOperatorContext * /*ctx*/) override { }
  virtual void exitKU_MultiplyDivideModuloOperator(CypherParser::KU_MultiplyDivideModuloOperatorContext * /*ctx*/) override { }

  virtual void enterOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext * /*ctx*/) override { }
  virtual void exitOC_PowerOfExpression(CypherParser::OC_PowerOfExpressionContext * /*ctx*/) override { }

  virtual void enterOC_UnaryAddSubtractOrFactorialExpression(CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext * /*ctx*/) override { }
  virtual void exitOC_UnaryAddSubtractOrFactorialExpression(CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext * /*ctx*/) override { }

  virtual void enterOC_StringListNullOperatorExpression(CypherParser::OC_StringListNullOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitOC_StringListNullOperatorExpression(CypherParser::OC_StringListNullOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitOC_ListOperatorExpression(CypherParser::OC_ListOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterKU_ListExtractOperatorExpression(CypherParser::KU_ListExtractOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitKU_ListExtractOperatorExpression(CypherParser::KU_ListExtractOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterKU_ListSliceOperatorExpression(CypherParser::KU_ListSliceOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitKU_ListSliceOperatorExpression(CypherParser::KU_ListSliceOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterOC_StringOperatorExpression(CypherParser::OC_StringOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitOC_StringOperatorExpression(CypherParser::OC_StringOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterOC_NullOperatorExpression(CypherParser::OC_NullOperatorExpressionContext * /*ctx*/) override { }
  virtual void exitOC_NullOperatorExpression(CypherParser::OC_NullOperatorExpressionContext * /*ctx*/) override { }

  virtual void enterOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext * /*ctx*/) override { }
  virtual void exitOC_PropertyOrLabelsExpression(CypherParser::OC_PropertyOrLabelsExpressionContext * /*ctx*/) override { }

  virtual void enterOC_Atom(CypherParser::OC_AtomContext * /*ctx*/) override { }
  virtual void exitOC_Atom(CypherParser::OC_AtomContext * /*ctx*/) override { }

  virtual void enterOC_Literal(CypherParser::OC_LiteralContext * /*ctx*/) override { }
  virtual void exitOC_Literal(CypherParser::OC_LiteralContext * /*ctx*/) override { }

  virtual void enterOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitOC_BooleanLiteral(CypherParser::OC_BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterOC_ListLiteral(CypherParser::OC_ListLiteralContext * /*ctx*/) override { }
  virtual void exitOC_ListLiteral(CypherParser::OC_ListLiteralContext * /*ctx*/) override { }

  virtual void enterOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext * /*ctx*/) override { }
  virtual void exitOC_ParenthesizedExpression(CypherParser::OC_ParenthesizedExpressionContext * /*ctx*/) override { }

  virtual void enterOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext * /*ctx*/) override { }
  virtual void exitOC_FunctionInvocation(CypherParser::OC_FunctionInvocationContext * /*ctx*/) override { }

  virtual void enterOC_FunctionName(CypherParser::OC_FunctionNameContext * /*ctx*/) override { }
  virtual void exitOC_FunctionName(CypherParser::OC_FunctionNameContext * /*ctx*/) override { }

  virtual void enterOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext * /*ctx*/) override { }
  virtual void exitOC_ExistentialSubquery(CypherParser::OC_ExistentialSubqueryContext * /*ctx*/) override { }

  virtual void enterOC_PropertyLookup(CypherParser::OC_PropertyLookupContext * /*ctx*/) override { }
  virtual void exitOC_PropertyLookup(CypherParser::OC_PropertyLookupContext * /*ctx*/) override { }

  virtual void enterOC_CaseExpression(CypherParser::OC_CaseExpressionContext * /*ctx*/) override { }
  virtual void exitOC_CaseExpression(CypherParser::OC_CaseExpressionContext * /*ctx*/) override { }

  virtual void enterOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext * /*ctx*/) override { }
  virtual void exitOC_CaseAlternative(CypherParser::OC_CaseAlternativeContext * /*ctx*/) override { }

  virtual void enterOC_Variable(CypherParser::OC_VariableContext * /*ctx*/) override { }
  virtual void exitOC_Variable(CypherParser::OC_VariableContext * /*ctx*/) override { }

  virtual void enterOC_NumberLiteral(CypherParser::OC_NumberLiteralContext * /*ctx*/) override { }
  virtual void exitOC_NumberLiteral(CypherParser::OC_NumberLiteralContext * /*ctx*/) override { }

  virtual void enterOC_Parameter(CypherParser::OC_ParameterContext * /*ctx*/) override { }
  virtual void exitOC_Parameter(CypherParser::OC_ParameterContext * /*ctx*/) override { }

  virtual void enterOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext * /*ctx*/) override { }
  virtual void exitOC_PropertyExpression(CypherParser::OC_PropertyExpressionContext * /*ctx*/) override { }

  virtual void enterOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext * /*ctx*/) override { }
  virtual void exitOC_PropertyKeyName(CypherParser::OC_PropertyKeyNameContext * /*ctx*/) override { }

  virtual void enterOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext * /*ctx*/) override { }
  virtual void exitOC_IntegerLiteral(CypherParser::OC_IntegerLiteralContext * /*ctx*/) override { }

  virtual void enterOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext * /*ctx*/) override { }
  virtual void exitOC_DoubleLiteral(CypherParser::OC_DoubleLiteralContext * /*ctx*/) override { }

  virtual void enterOC_SchemaName(CypherParser::OC_SchemaNameContext * /*ctx*/) override { }
  virtual void exitOC_SchemaName(CypherParser::OC_SchemaNameContext * /*ctx*/) override { }

  virtual void enterOC_SymbolicName(CypherParser::OC_SymbolicNameContext * /*ctx*/) override { }
  virtual void exitOC_SymbolicName(CypherParser::OC_SymbolicNameContext * /*ctx*/) override { }

  virtual void enterOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext * /*ctx*/) override { }
  virtual void exitOC_LeftArrowHead(CypherParser::OC_LeftArrowHeadContext * /*ctx*/) override { }

  virtual void enterOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext * /*ctx*/) override { }
  virtual void exitOC_RightArrowHead(CypherParser::OC_RightArrowHeadContext * /*ctx*/) override { }

  virtual void enterOC_Dash(CypherParser::OC_DashContext * /*ctx*/) override { }
  virtual void exitOC_Dash(CypherParser::OC_DashContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

