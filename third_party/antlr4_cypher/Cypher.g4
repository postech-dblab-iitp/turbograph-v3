/*
* OpenCypher grammar at "https://s3.amazonaws.com/artifacts.opencypher.org/legacy/Cypher.g4"
*/
grammar Cypher;

// provide ad-hoc error messages for common syntax errors
@parser::declarations {
    virtual void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) {};
    virtual void notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token* startToken) {};
    virtual void notifyInvalidNotEqualOperator(antlr4::Token* startToken) {};
    virtual void notifyEmptyToken(antlr4::Token* startToken) {};
    virtual void notifyReturnNotAtEnd(antlr4::Token* startToken) {};
    virtual void notifyNonBinaryComparison(antlr4::Token* startToken) {};
}

oC_Cypher
    : SP ? oC_AnyCypherOption? SP? oC_Statement ( SP? ';' )? SP? EOF
        | SP ? kU_DDL ( SP? ';' )? SP? EOF
        | SP ? kU_CopyCSV ( SP? ';' )? SP? EOF ;

kU_CopyCSV
    : COPY SP oC_SchemaName SP FROM SP StringLiteral ( SP? '(' SP? kU_ParsingOptions SP? ')' )? ;

kU_ParsingOptions
    : kU_ParsingOption ( SP? ',' SP? kU_ParsingOption )* ;

kU_ParsingOption
    : oC_SymbolicName SP? '=' SP? oC_Literal;

COPY : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'P' | 'p') ( 'Y' | 'y' ) ;

FROM : ( 'F' | 'f' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'M' | 'm' );

kU_DDL
    : kU_CreateNode
        | kU_CreateRel
        | kU_DropTable;

kU_CreateNode
    : CREATE SP NODE SP TABLE SP oC_SchemaName SP? '(' SP? kU_PropertyDefinitions SP? ( ',' SP? kU_CreateNodeConstraint ) SP? ')' ;

NODE : ( 'N' | 'n' ) ( 'O' | 'o' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ;

TABLE: ( 'T' | 't' ) ( 'A' | 'a' ) ( 'B' | 'b' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

kU_CreateRel
    : CREATE SP REL SP TABLE SP oC_SchemaName SP? '(' SP? kU_RelConnections SP? ( ',' SP? kU_PropertyDefinitions SP? )? ( ',' SP? oC_SymbolicName SP? )?  ')' ;

kU_DropTable
    : DROP SP TABLE SP oC_SchemaName ;

DROP : ( 'D' | 'd' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'P' | 'p' ) ;

kU_RelConnections : kU_RelConnection ( SP? ',' SP? kU_RelConnection )* ;

kU_RelConnection: FROM SP kU_NodeLabels SP TO SP kU_NodeLabels ;

kU_NodeLabels: oC_SchemaName ( SP? '|' SP ? oC_SchemaName )* ;

kU_PropertyDefinitions : kU_PropertyDefinition ( SP? ',' SP? kU_PropertyDefinition )* ;

kU_PropertyDefinition : oC_PropertyKeyName SP kU_DataType ;

kU_CreateNodeConstraint : PRIMARY SP KEY SP? '(' SP? oC_PropertyKeyName SP? ')' ;

PRIMARY: ( 'P' | 'p' ) ( 'R' | 'r' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'R' | 'r' ) ( 'Y' | 'y' ) ;

KEY : ( 'K' | 'k' ) ( 'E' | 'e' ) ( 'Y' | 'y' ) ;

REL: ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ;

TO: ( 'T' | 't' ) ( 'O' | 'o' ) ;

kU_DataType
    : oC_SymbolicName
        | ( oC_SymbolicName kU_ListIdentifiers ) ;

kU_ListIdentifiers : kU_ListIdentifier ( kU_ListIdentifier )* ;

kU_ListIdentifier : '[' ']' ;

oC_AnyCypherOption
    : oC_Explain
        | oC_Profile ;

oC_Explain
    : EXPLAIN ;

EXPLAIN : ( 'E' | 'e' ) ( 'X' | 'x' ) ( 'P' | 'p' ) ( 'L' | 'l' ) ( 'A' | 'a' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ;

oC_Profile
    : PROFILE ;

PROFILE : ( 'P' | 'p' ) ( 'R' | 'r' ) ( 'O' | 'o' ) ( 'F' | 'f' ) ( 'I' | 'i' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ;

oC_Statement
    : oC_Query ;

oC_Query
    : oC_RegularQuery ;

oC_RegularQuery
    : oC_SingleQuery ( SP? oC_Union )*
        | (oC_Return SP? )+ oC_SingleQuery { notifyReturnNotAtEnd($ctx->start); }
        ;

oC_Union
     :  ( UNION SP ALL SP? oC_SingleQuery )
         | ( UNION SP? oC_SingleQuery ) ;

UNION : ( 'U' | 'u' ) ( 'N' | 'n' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ;

ALL : ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

oC_SingleQuery
    : oC_SinglePartQuery
        | oC_MultiPartQuery
        ;

oC_SinglePartQuery
    : ( oC_ReadingClause SP? )* oC_Return
        | ( ( oC_ReadingClause SP? )* oC_UpdatingClause ( SP? oC_UpdatingClause )* ( SP? oC_Return )? )
        | ( oC_ReadingClause SP? )* { notifyQueryNotConcludeWithReturn($ctx->start); }
        ;

oC_MultiPartQuery
    : ( kU_QueryPart SP? )+ oC_SinglePartQuery;

kU_QueryPart
    : (oC_ReadingClause SP? )* ( oC_UpdatingClause SP? )* oC_With ;

oC_UpdatingClause
    : oC_Create
        | oC_Set
        | oC_Delete
        ;

oC_ReadingClause
    : oC_Match
        | oC_Unwind
        ;

oC_Match
    : ( OPTIONAL SP )? MATCH SP? oC_Pattern (SP? oC_Where)? ;

OPTIONAL : ( 'O' | 'o' ) ( 'P' | 'p' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ;

MATCH : ( 'M' | 'm' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'C' | 'c' ) ( 'H' | 'h' ) ;

UNWIND : ( 'U' | 'u' ) ( 'N' | 'n' )( 'W' | 'w' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ;

oC_Unwind : UNWIND SP? oC_Expression SP AS SP oC_Variable ;

oC_Create
    : CREATE SP? oC_Pattern ;

CREATE : ( 'C' | 'c' ) ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

oC_Set
    : SET SP? oC_SetItem ( SP? ',' SP? oC_SetItem )* ;

SET : ( 'S' | 's' ) ( 'E' | 'e' ) ( 'T' | 't' )  ;

oC_SetItem
    : ( oC_PropertyExpression SP? '=' SP? oC_Expression ) ;

oC_Delete
    : DELETE SP? oC_Expression ( SP? ',' SP? oC_Expression )*;

DELETE : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'E' | 'e' ) ;

oC_With
    : WITH oC_ProjectionBody ( SP? oC_Where )? ;

WITH : ( 'W' | 'w' ) ( 'I' | 'i' ) ( 'T' | 't' ) ( 'H' | 'h' ) ;

oC_Return
    : RETURN oC_ProjectionBody ;

RETURN : ( 'R' | 'r' ) ( 'E' | 'e' ) ( 'T' | 't' ) ( 'U' | 'u' ) ( 'R' | 'r' ) ( 'N' | 'n' ) ;

oC_ProjectionBody
    : ( SP? DISTINCT )? SP oC_ProjectionItems (SP oC_Order )? ( SP oC_Skip )? ( SP oC_Limit )? ;

DISTINCT : ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'C' | 'c' ) ( 'T' | 't' ) ;

oC_ProjectionItems
    : ( STAR ( SP? ',' SP? oC_ProjectionItem )* )
        | ( oC_ProjectionItem ( SP? ',' SP? oC_ProjectionItem )* )
        ;

STAR : '*' ;

oC_ProjectionItem
    : ( oC_Expression SP AS SP oC_Variable )
        | oC_Expression
        ;

AS : ( 'A' | 'a' ) ( 'S' | 's' ) ;

oC_Order
    : ORDER SP BY SP oC_SortItem ( ',' SP? oC_SortItem )* ;

ORDER : ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ;

BY : ( 'B' | 'b' ) ( 'Y' | 'y' ) ;

oC_Skip
    :  L_SKIP SP oC_Expression ;

L_SKIP : ( 'S' | 's' ) ( 'K' | 'k' ) ( 'I' | 'i' ) ( 'P' | 'p' ) ;

oC_Limit
    : LIMIT SP oC_Expression ;

LIMIT : ( 'L' | 'l' ) ( 'I' | 'i' ) ( 'M' | 'm' ) ( 'I' | 'i' ) ( 'T' | 't' ) ;

oC_SortItem
    : oC_Expression ( SP? ( ASCENDING | ASC | DESCENDING | DESC ) )? ;

ASCENDING : ( 'A' | 'a' ) ( 'S' | 's' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

ASC : ( 'A' | 'a' ) ( 'S' | 's' ) ( 'C' | 'c' ) ;

DESCENDING : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'C' | 'c' ) ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'G' | 'g' ) ;

DESC : ( 'D' | 'd' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'C' | 'c' ) ;

oC_Where
    : WHERE SP oC_Expression ;

WHERE : ( 'W' | 'w' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'R' | 'r' ) ( 'E' | 'e' )  ;

oC_Pattern
    : oC_PatternPart ( SP? ',' SP? oC_PatternPart )* ;

oC_PatternPart
           :  ( oC_Variable SP? '=' SP? oC_AnonymousPatternPart )
               | oC_AnonymousPatternPart
               ;

oC_AnonymousPatternPart
            :  oC_ShortestPathPattern
                | oC_PatternElement
                ;

oC_ShortestPathPattern
                   :  ( SHORTESTPATH '(' oC_PatternElement ')' )
                       | ( ALLSHORTESTPATHS '(' oC_PatternElement ')' )
                       ;

SHORTESTPATH : ( 'S' | 's' ) ( 'H' | 'h' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'T' | 't' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'P' | 'p' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'H' | 'h' )  ;

ALLSHORTESTPATHS : ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ( 'S' | 's' ) ( 'H' | 'h' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ( 'T' | 't' ) ( 'E' | 'e' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'P' | 'p' ) ( 'A' | 'a' ) ( 'T' | 't' ) ( 'H' | 'h' ) ( 'S' | 's' )  ;

oC_PatternElement
    : ( oC_NodePattern ( SP? oC_PatternElementChain )* )
        | ( '(' oC_PatternElement ')' )
        ;

oC_NodePattern
    : '(' SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( kU_Properties SP? )? ')'
        | SP? ( oC_Variable SP? )? ( oC_NodeLabels SP? )? ( kU_Properties SP? )? { notifyNodePatternWithoutParentheses($oC_Variable.text, $oC_Variable.start); }
        ;

oC_PatternElementChain
    : oC_RelationshipPattern SP? oC_NodePattern ;

oC_RelationshipPattern
    : ( oC_LeftArrowHead SP? oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )
        | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash SP? oC_RightArrowHead )
        | ( oC_Dash SP? oC_RelationshipDetail? SP? oC_Dash )
        ;

oC_RelationshipDetail
    : '[' SP? ( oC_Variable SP? )? ( oC_RelationshipTypes SP? )? ( oC_RangeLiteral SP? ) ? ( kU_Properties SP? ) ? ']' ;

// The original oC_Properties definition is  oC_MapLiteral | oC_Parameter.
// We choose to not support parameter as properties which will be the decision for a long time.
// We then substitute with oC_MapLiteral definition. We create oC_MapLiteral only when we decide to add MAP type.
kU_Properties
    : '{' SP? ( oC_PropertyKeyName SP? ':' SP? oC_Expression SP? ( ',' SP? oC_PropertyKeyName SP? ':' SP? oC_Expression SP? )* )? '}';

oC_RelationshipTypes
    :  ':' SP? oC_RelTypeName ( SP? '|' ':'? SP? oC_RelTypeName )* ;

oC_NodeLabels
    :  oC_NodeLabel ( SP? oC_NodeLabel )* ;

oC_NodeLabel
    : ':' SP? oC_LabelName ;

// oC_RangeLiteral
//    :  '*' SP? oC_IntegerLiteral SP? '..' SP? oC_IntegerLiteral ;

// S62 to support variable length expand

RANGE : '..' ;

oC_RangeLiteral
    :  '*' SP? ( oC_RangeStartLiteral SP? )? ( '..' SP? ( oC_RangeEndLiteral SP? )? )? ;

oC_RangeStartLiteral
    : oC_IntegerLiteral ;

oC_RangeEndLiteral
    : oC_IntegerLiteral ;


oC_LabelName
    : oC_SchemaName ;

oC_RelTypeName
    : oC_SchemaName ;

oC_Expression
    : oC_OrExpression ;

oC_OrExpression
    : oC_XorExpression ( SP OR SP oC_XorExpression )* ;

OR : ( 'O' | 'o' ) ( 'R' | 'r' ) ;

oC_XorExpression
    : oC_AndExpression ( SP XOR SP oC_AndExpression )* ;

XOR : ( 'X' | 'x' ) ( 'O' | 'o' ) ( 'R' | 'r' ) ;

oC_AndExpression
    : oC_NotExpression ( SP AND SP oC_NotExpression )* ;

AND : ( 'A' | 'a' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ;

oC_NotExpression
    : ( NOT SP? )?  oC_ComparisonExpression ;

NOT : ( 'N' | 'n' ) ( 'O' | 'o' ) ( 'T' | 't' ) ;

oC_ComparisonExpression
    : kU_BitwiseOrOperatorExpression ( SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression )?
        | kU_BitwiseOrOperatorExpression ( SP? INVALID_NOT_EQUAL SP? kU_BitwiseOrOperatorExpression ) { notifyInvalidNotEqualOperator($INVALID_NOT_EQUAL); }
        | kU_BitwiseOrOperatorExpression SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression ( SP? kU_ComparisonOperator SP? kU_BitwiseOrOperatorExpression )+ { notifyNonBinaryComparison($ctx->start); }
        ;

kU_ComparisonOperator : '=' | '<>' | '<' | '<=' | '>' | '>=' ;

INVALID_NOT_EQUAL : '!=' ;

kU_BitwiseOrOperatorExpression
    : kU_BitwiseAndOperatorExpression ( SP? '|' SP? kU_BitwiseAndOperatorExpression )* ;

kU_BitwiseAndOperatorExpression
    : kU_BitShiftOperatorExpression ( SP? '&' SP? kU_BitShiftOperatorExpression )* ;

kU_BitShiftOperatorExpression
    : oC_AddOrSubtractExpression ( SP? kU_BitShiftOperator SP? oC_AddOrSubtractExpression )* ;

kU_BitShiftOperator : '>>' | '<<' ;

oC_AddOrSubtractExpression
    : oC_MultiplyDivideModuloExpression ( SP? kU_AddOrSubtractOperator SP? oC_MultiplyDivideModuloExpression )* ;

kU_AddOrSubtractOperator : '+' | '-' ;

oC_MultiplyDivideModuloExpression
    : oC_PowerOfExpression ( SP? kU_MultiplyDivideModuloOperator SP? oC_PowerOfExpression )* ;

kU_MultiplyDivideModuloOperator : '*' | '/' | '%' ;

oC_PowerOfExpression
    : oC_UnaryAddSubtractOrFactorialExpression ( SP? '^' SP? oC_UnaryAddSubtractOrFactorialExpression )* ;

oC_UnaryAddSubtractOrFactorialExpression
    : ( MINUS SP? )? oC_StringListNullOperatorExpression (SP? FACTORIAL)? ;

MINUS : '-' ;

FACTORIAL : '!' ;

oC_StringListNullOperatorExpression
    : oC_PropertyOrLabelsExpression ( oC_StringOperatorExpression | oC_ListOperatorExpression | oC_NullOperatorExpression )? ;

oC_ListOperatorExpression
    : ( kU_ListPropertyOrLabelsExpression | kU_ListExtractOperatorExpression | kU_ListSliceOperatorExpression ) oC_ListOperatorExpression ? ;

kU_ListPropertyOrLabelsExpression
    : SP IN SP? oC_PropertyOrLabelsExpression ;

kU_ListExtractOperatorExpression
    : SP ? '[' oC_Expression ']' ;

kU_ListSliceOperatorExpression
    : SP ? '[' oC_Expression? ':' oC_Expression? ']' ;

IN : ( 'I' | 'i' ) ( 'N' | 'n' )  ;

oC_StringOperatorExpression
    :  ( ( SP STARTS SP WITH ) | ( SP ENDS SP WITH ) | ( SP CONTAINS ) ) SP? oC_PropertyOrLabelsExpression ;

STARTS : ( 'S' | 's' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'R' | 'r' ) ( 'T' | 't' ) ( 'S' | 's' ) ;

ENDS : ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' ) ( 'S' | 's' ) ;

CONTAINS : ( 'C' | 'c' ) ( 'O' | 'o' ) ( 'N' | 'n' ) ( 'T' | 't' ) ( 'A' | 'a' ) ( 'I' | 'i' ) ( 'N' | 'n' ) ( 'S' | 's' ) ;

oC_NullOperatorExpression
    : ( SP IS SP NULL_ )
        | ( SP IS SP NOT SP NULL_ ) ;

IS : ( 'I' | 'i' ) ( 'S' | 's' ) ;

NULL_ : ( 'N' | 'n' ) ( 'U' | 'u' ) ( 'L' | 'l' ) ( 'L' | 'l' ) ;

oC_PropertyOrLabelsExpression
    : oC_Atom ( SP? oC_PropertyLookup )? ;

oC_Atom
    : oC_Literal
        | oC_Parameter
        | oC_CaseExpression
		| oC_ListComprehension
        | oC_PatternComprehension
		| oC_RelationshipsPattern
        | oC_ParenthesizedExpression
        | oC_FunctionInvocation
        | oC_ExistentialSubquery
        | oC_Variable
        ;

oC_Literal
    : oC_NumberLiteral
        | StringLiteral
        | oC_BooleanLiteral
        | NULL_
        | oC_ListLiteral
        ;

oC_BooleanLiteral
    : TRUE
        | FALSE
        ;

TRUE : ( 'T' | 't' ) ( 'R' | 'r' ) ( 'U' | 'u' ) ( 'E' | 'e' ) ;

FALSE : ( 'F' | 'f' ) ( 'A' | 'a' ) ( 'L' | 'l' ) ( 'S' | 's' ) ( 'E' | 'e' ) ;

oC_ListLiteral
    :  '[' SP? ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? ']' ;

oC_ParenthesizedExpression
    : '(' SP? oC_Expression SP? ')' ;

oC_RelationshipsPattern
                    :  oC_NodePattern ( SP? oC_PatternElementChain )+ ;

oC_FilterExpression
                :  oC_IdInColl ( SP? oC_Where )? ;

oC_IdInColl
        :  oC_Variable SP IN SP oC_Expression ;

oC_FunctionInvocation
    : oC_FunctionName SP? '(' SP? '*' SP? ')'
        | oC_FunctionName SP? '(' SP? ( DISTINCT SP? )? ( oC_Expression SP? ( ',' SP? oC_Expression SP? )* )? ')' ;

oC_FunctionName
    : oC_SymbolicName ;

oC_ExistentialSubquery
    :  EXISTS SP? '{' SP? MATCH SP? oC_Pattern ( SP? oC_Where )? SP? '}' ;

EXISTS : ( 'E' | 'e' ) ( 'X' | 'x' ) ( 'I' | 'i' ) ( 'S' | 's' ) ( 'T' | 't' ) ( 'S' | 's' ) ;

oC_ListComprehension
                 :  '[' SP? oC_FilterExpression ( SP? '|' SP? oC_Expression )? SP? ']' ;

oC_PatternComprehension
                    :  '[' SP? ( oC_Variable SP? '=' SP? )? oC_RelationshipsPattern SP? ( WHERE SP? oC_Expression SP? )? '|' SP? oC_Expression SP? ']' ;

oC_PropertyLookup
    : '.' SP? ( oC_PropertyKeyName ) ;

oC_CaseExpression
    :  ( ( CASE ( SP? oC_CaseAlternative )+ ) | ( CASE SP? oC_Expression ( SP? oC_CaseAlternative )+ ) ) ( SP? ELSE SP? oC_Expression )? SP? END ;

CASE : ( 'C' | 'c' ) ( 'A' | 'a' ) ( 'S' | 's' ) ( 'E' | 'e' )  ;

ELSE : ( 'E' | 'e' ) ( 'L' | 'l' ) ( 'S' | 's' ) ( 'E' | 'e' )  ;

END : ( 'E' | 'e' ) ( 'N' | 'n' ) ( 'D' | 'd' )  ;

oC_CaseAlternative
    :  WHEN SP? oC_Expression SP? THEN SP? oC_Expression ;

WHEN : ( 'W' | 'w' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'N' | 'n' )  ;

THEN : ( 'T' | 't' ) ( 'H' | 'h' ) ( 'E' | 'e' ) ( 'N' | 'n' )  ;

oC_Variable
    : oC_SymbolicName ;

StringLiteral
    : ( '"' ( StringLiteral_0 | EscapedChar )* '"' )
        | ( '\'' ( StringLiteral_1 | EscapedChar )* '\'' )
        ;

EscapedChar
    : '\\' ( '\\' | '\'' | '"' | ( 'B' | 'b' ) | ( 'F' | 'f' ) | ( 'N' | 'n' ) | ( 'R' | 'r' ) | ( 'T' | 't' ) | ( ( 'U' | 'u' ) ( HexDigit HexDigit HexDigit HexDigit ) ) | ( ( 'U' | 'u' ) ( HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit HexDigit ) ) ) ;

oC_NumberLiteral
    : oC_DoubleLiteral
        | oC_IntegerLiteral
        ;

oC_Parameter
    : '$' ( oC_SymbolicName | DecimalInteger ) ;

oC_PropertyExpression
    : oC_Atom SP? oC_PropertyLookup ;

oC_PropertyKeyName
    : oC_SchemaName ;

oC_IntegerLiteral
    : DecimalInteger ;

DecimalInteger
    : ZeroDigit
        | ( NonZeroDigit ( Digit )* )
        ;

HexLetter
    : ( 'A' | 'a' )
        | ( 'B' | 'b' )
        | ( 'C' | 'c' )
        | ( 'D' | 'd' )
        | ( 'E' | 'e' )
        | ( 'F' | 'f' )
        ;

HexDigit
    : Digit
        | HexLetter
        ;

Digit
    : ZeroDigit
        | NonZeroDigit
        ;

NonZeroDigit
    : NonZeroOctDigit
        | '8'
        | '9'
        ;

NonZeroOctDigit
    : '1'
        | '2'
        | '3'
        | '4'
        | '5'
        | '6'
        | '7'
        ;

ZeroDigit
    : '0' ;

oC_DoubleLiteral
    : RegularDecimalReal ;

RegularDecimalReal
    : ( Digit )* '.' ( Digit )+ ;

oC_SchemaName
    : oC_SymbolicName ;

oC_SymbolicName
    : UnescapedSymbolicName
        | EscapedSymbolicName {if ($EscapedSymbolicName.text == "``") { notifyEmptyToken($EscapedSymbolicName); }}
        | HexLetter
        ;

UnescapedSymbolicName
    : IdentifierStart ( IdentifierPart )* ;

IdentifierStart
    : ID_Start
        | Pc
        ;

IdentifierPart
    : ID_Continue
        | Sc
        ;

EscapedSymbolicName
    : ( '`' ( EscapedSymbolicName_0 )* '`' )+ ;

SP
  : ( WHITESPACE )+ ;

WHITESPACE
    : SPACE
        | TAB
        | LF
        | VT
        | FF
        | CR
        | FS
        | GS
        | RS
        | US
        | '\u1680'
        | '\u180e'
        | '\u2000'
        | '\u2001'
        | '\u2002'
        | '\u2003'
        | '\u2004'
        | '\u2005'
        | '\u2006'
        | '\u2008'
        | '\u2009'
        | '\u200a'
        | '\u2028'
        | '\u2029'
        | '\u205f'
        | '\u3000'
        | '\u00a0'
        | '\u2007'
        | '\u202f'
        | Comment
        ;

Comment
    : ( '/*' ( Comment_1 | ( '*' Comment_2 ) )* '*/' )
        | ( '//' ( Comment_3 )* CR? ( LF | EOF ) )
        ;

oC_LeftArrowHead
    : '<'
        | '\u27e8'
        | '\u3008'
        | '\ufe64'
        | '\uff1c'
        ;

oC_RightArrowHead
    : '>'
        | '\u27e9'
        | '\u3009'
        | '\ufe65'
        | '\uff1e'
        ;

oC_Dash
    : '-'
        | '\u00ad'
        | '\u2010'
        | '\u2011'
        | '\u2012'
        | '\u2013'
        | '\u2014'
        | '\u2015'
        | '\u2212'
        | '\ufe58'
        | '\ufe63'
        | '\uff0d'
        ;

fragment FF : [\f] ;

fragment EscapedSymbolicName_0 : ~[`] ;

fragment RS : [\u001E] ;

fragment ID_Continue : [\p{ID_Continue}] ;

fragment Comment_1 : ~[*] ;

fragment StringLiteral_1 : ~['\\] ;

fragment Comment_3 : ~[\n\r] ;

fragment Comment_2 : ~[/] ;

fragment GS : [\u001D] ;

fragment FS : [\u001C] ;

fragment CR : [\r] ;

fragment Sc : [\p{Sc}] ;

fragment SPACE : [ ] ;

fragment Pc : [\p{Pc}] ;

fragment TAB : [\t] ;

fragment StringLiteral_0 : ~["\\] ;

fragment LF : [\n] ;

fragment VT : [\u000B] ;

fragment US : [\u001F] ;

fragment ID_Start : [\p{ID_Start}] ;

// This is used to capture unknown lexer input (e.g. !) to avoid parser exception.
Unknown : .;
