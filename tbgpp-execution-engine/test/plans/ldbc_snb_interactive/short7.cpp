// #include "plans/query_plan_suite.hpp"



// namespace duckdb {

// CypherPipelineExecutor* is7_pipe1(QueryPlanSuite& suite);
// CypherPipelineExecutor* is7_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IS7() {

// 	std::vector<CypherPipelineExecutor*> result;
// 	auto p1 = is7_pipe1(*this);
// 	auto p2 = is7_pipe2(*this, p1);
// 	result.push_back(p1);
// 	result.push_back(p2);
// 	return result;

// }

// CypherPipelineExecutor* is7_pipe1(QueryPlanSuite& suite) {

// // scan message
// 	CypherSchema sch1;
// 	sch1.addNode("m");
// 	duckdb::Value filter_val; // person key
// 	if(suite.LDBC_SF==1) { filter_val = duckdb::Value::UBIGINT(57459); }
// 	if(suite.LDBC_SF==10) { filter_val = duckdb::Value::UBIGINT(58929); }
// 	if(suite.LDBC_SF==100) { filter_val = duckdb::Value::UBIGINT(19560); }

// // expand  m->c
// 	CypherSchema sch2 = sch1;
// 	sch2.addNode("c");

// // expand c->p
// 	CypherSchema sch3 = sch2;
// 	sch3.addNode("p");

// // optwxpand m->a (in : _m, _c, _p )
// 	CypherSchema sch4 = sch3;
// 	sch4.addNode("a");

// // optexpand a-r->p' (in ; _m _c _p _a)
// 	CypherSchema sch5 = sch4;
// 	sch5.addEdge("r");
// 	sch5.addNode("p1");

// // filter p == null or p = p1 (in : _m _c _p _a _r _p1 )
// 	vector<unique_ptr<Expression>> into_predicates;
// 	{
// 		auto test_null_val = Value::Value(LogicalType::ID);	// is this right way to define null?
// 		D_ASSERT( test_null_val.isNull() );
// 		auto filter_expr1_lhs = make_unique<BoundComparisonExpression>(
// 				ExpressionType::COMPARE_EQUAL,
// 				std::move( make_unique<BoundReferenceExpression>(LogicalType::ID, 2) ),	// p
// 				std::move( make_unique<BoundConstantExpression>( Value(LogicalType::ID)) )	// null
// 		);
// 		auto filter_expr1_rhs = make_unique<BoundComparisonExpression>(
// 				ExpressionType::COMPARE_EQUAL,
// 				std::move( make_unique<BoundReferenceExpression>(LogicalType::ID, 2) ),	// p
// 				std::move( make_unique<BoundReferenceExpression>(LogicalType::ID, 5) )	// p1
// 		);
// 		// or
// 		auto filter_expr1 = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(filter_expr1_lhs), move(filter_expr1_rhs));
// 		into_predicates.push_back(std::move(filter_expr1));
// 	}

// // fetch c (in: _m _c _p _a _r _p1 )
// 	CypherSchema sch6 = sch5;
// 	sch6.addPropertyIntoNode("c", "content", LogicalType::VARCHAR);
// 	sch6.addPropertyIntoNode("c", "creationDate", LogicalType::BIGINT);

// // fetch p (in: _m _c c.c c.cd _p _a _r _p1)
// 	CypherSchema sch7 = sch6;
// 	sch7.addPropertyIntoNode("p", "id", LogicalType::UBIGINT);
// 	sch7.addPropertyIntoNode("p", "firstName", LogicalType::VARCHAR);
// 	sch7.addPropertyIntoNode("p", "lastName", LogicalType::VARCHAR);
	

// //  (in: _m _c c.c c.cd _p _p.id _p.fn _p.ln _a _r _p1)
// 	CypherSchema sch8;

// // order by

// 	// Note that this is a left deep plan. This plan type reduces #pipeline(3->2) but larger intermediate result size.
// 	// anothor possible is bushy plan (i.e. LeftHJ ( m-c-p, m-a-r-p )

// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Comment"), PropertyKeys(), filter_expr1));	// comment only
// 	//ops
// 	ops.push_back( new PhysicalAdjIdxJoin(sch2, "m", LabelSet("Comment"), LabelSet("REPLY_OF"), ExpandDirection::INCOMING, LabelSet("Comment"), JoinType::INNER, false, true));
// 	ops.push_back( new PhysicalAdjIdxJoin(sch3, "c", LabelSet("Comment"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
// 	ops.push_back( new PhysicalAdjIdxJoin(sch4, "m", LabelSet("Comment"), LabelSet("HAS_CREATOR"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::LEFT, false, true));	// optional m->a
// 	ops.push_back( new PhysicalAdjIdxJoin(sch5, "a", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::LEFT, true, true)); // optional a-r->p1 // loadedge true
// 	ops.push_back( new PhysicalFilter(sch5, into_predicates));

	
// }

// CypherPipelineExecutor* is7_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// // order by

// // produce resutl

// }



// }