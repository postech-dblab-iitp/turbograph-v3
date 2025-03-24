//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_tokens.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

//===--------------------------------------------------------------------===//
// Query Node
//===--------------------------------------------------------------------===//
class BoundQueryNode;
class BoundSelectNode;
class BoundSetOperationNode;
class BoundRecursiveCTENode;


//===--------------------------------------------------------------------===//
// Query
//===--------------------------------------------------------------------===//

class BoundRegularQuery;
class NormalizedSingleQuery;
class NormalizedQueryPart;
class BoundNodePattern;
class BoundRelPattern;
class BoundPattern;
class QueryGraph;
class QueryGraphCollection;
class BoundMatchClause;
class BoundReadingClause;
class BoundUnwindClause;
class BoundProjectionBody;
class BoundReturnClause;
class BoundWithClause;

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//
class Expression;

class BoundAggregateExpression;
class BoundBetweenExpression;
class BoundCaseExpression;
class BoundCastExpression;
class BoundColumnRefExpression;
class BoundComparisonExpression;
class BoundConjunctionExpression;
class BoundConstantExpression;
class BoundDefaultExpression;
class BoundFunctionExpression;
class BoundOperatorExpression;
class BoundParameterExpression;
class BoundReferenceExpression;
class BoundSubqueryExpression;
class BoundUnnestExpression;
class BoundWindowExpression;

//===--------------------------------------------------------------------===//
// TableRefs
//===--------------------------------------------------------------------===//
// class BoundTableRef;

// class BoundBaseTableRef;
// class BoundCrossProductRef;
// class BoundJoinRef;
// class BoundSubqueryRef;
// class BoundTableFunction;
// class BoundEmptyTableRef;
// class BoundExpressionListRef;
// class BoundCTERef;

} // namespace duckdb
