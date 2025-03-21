//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tokens.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//===--------------------------------------------------------------------===//
// Statements
//===--------------------------------------------------------------------===//
class CypherStatement;

class AlterStatement;
class CallStatement;
class CopyStatement;
class CreateStatement;
class DeleteStatement;
class DropStatement;
class ExtensionStatement;
class InsertStatement;
class SelectStatement;
class TransactionStatement;
class UpdateStatement;
class PrepareStatement;
class ExecuteStatement;
class PragmaStatement;
class ShowStatement;
class ExplainStatement;
class ExportStatement;
class VacuumStatement;
class RelationStatement;
class SetStatement;
class LoadStatement;

//===--------------------------------------------------------------------===//
// Query Node
//===--------------------------------------------------------------------===//
class QueryNode;

//===--------------------------------------------------------------------===//
// Query
//===--------------------------------------------------------------------===//

class RegularQuery;
class SingleQuery;
class QueryPart;
class UpdatingClause;
class ReadingClause;
class WithClause;
class ReturnClause;
class ProjectionBody;
class PatternElement;
class NodePattern;
class PatternElementChain;
class RelPattern;
struct YieldVariable;

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class ParsedExpression;

class BetweenExpression;
class CaseExpression;
class CastExpression;
class CollateExpression;
class ColumnRefExpression;
class ComparisonExpression;
class ConjunctionExpression;
class ConstantExpression;
class DefaultExpression;
class FunctionExpression;
class LambdaExpression;
class OperatorExpression;
class ParameterExpression;
class NamedParameterExpression;
class PositionalReferenceExpression;
class PropertyExpression;
class StarExpression;
class SubqueryExpression;
class VariableExpression;
class WindowExpression;

//===--------------------------------------------------------------------===//
// Constraints
//===--------------------------------------------------------------------===//
class Constraint;

class NotNullConstraint;
class CheckConstraint;
class UniqueConstraint;
class ForeignKeyConstraint;

//===--------------------------------------------------------------------===//
// TableRefs
//===--------------------------------------------------------------------===//
class TableRef;

class BaseTableRef;
class CrossProductRef;
class JoinRef;
class SubqueryRef;
class TableFunctionRef;
class EmptyTableRef;
class ExpressionListRef;

//===--------------------------------------------------------------------===//
// Other
//===--------------------------------------------------------------------===//
struct SampleOptions;

} // namespace duckdb
