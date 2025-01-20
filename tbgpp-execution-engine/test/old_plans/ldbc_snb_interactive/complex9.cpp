// #include "plans/query_plan_suite.hpp"


// namespace s62 {

// CypherPipelineExecutor* ic9_pipe1(QueryPlanSuite& suite);
// CypherPipelineExecutor* ic9_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);
// CypherPipelineExecutor* ic9_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe);

// std::vector<CypherPipelineExecutor*> QueryPlanSuite::LDBC_IC9() {

// 	std::vector<CypherPipelineExecutor*> result;
// 	auto p1 = ic9_pipe1(*this);
// 	auto p2 = ic9_pipe2(*this, p1);
// 	auto p3 = ic9_pipe2(*this, p2);
// 	result.push_back(p1);
// 	result.push_back(p2);
// 	result.push_back(p3);
// 	return result;
// }

// CypherPipelineExecutor* ic9_pipe1(QueryPlanSuite& suite) {

// 	// scan person
// 	Schema sch1;
// 	sch1.addNode("root");
// 	s62::Value filter_val; // person key
// 	if(suite.LDBC_SF==1) { filter_val = s62::Value::UBIGINT(14); }
// 	if(suite.LDBC_SF==10) { filter_val = s62::Value::UBIGINT(14); }
// 	if(suite.LDBC_SF==100) { filter_val = s62::Value::UBIGINT(14); }

// 	// person->friend
// 	Schema sch2 = sch1;
// 	sch2.addNode("friend");

// 	// filter (_root, _friend)
// 	vector<unique_ptr<Expression>> filter_exprs;
// 	{
// 		unique_ptr<Expression> filter_expr1;
// 		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, 
// 							make_unique<BoundReferenceExpression>(LogicalType::ID, 0),
// 							make_unique<BoundReferenceExpression>(LogicalType::ID, 1)
// 						);
// 		filter_exprs.push_back(move(filter_expr1));
// 	}

// 	// projection	(_root, _friend)
// 	Schema sch3;
// 	sch3.addNode("friend");
// 	vector<unique_ptr<Expression>> proj_exprs;
// 	{
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::ID, 1) ) );	// friendid
// 	}

// 	// agg - distinct friend ids  (_friend)
// 	vector<unique_ptr<Expression>> agg_exprs;
// 	vector<unique_ptr<Expression>> agg_groups;
// 	agg_groups.push_back( make_unique<BoundReferenceExpression>(LogicalType::ID, 0) ); // distinct friends

// // pipe

// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// 	ops.push_back( new PhysicalNodeScan(sch1, LabelSet("Person"), vector<string>(), "id", filter_val));
// 	// ops
// 	ops.push_back( new PhysicalAdjIdxJoin(sch2, "root", LabelSet("Person"), LabelSet("KNOWS"), ExpandDirection::OUTGOING, LabelSet("Person"), JoinType::INNER, false, true));
// 	ops.push_back( new PhysicalFilter(sch2, move(filter_exprs)));
// 	ops.push_back( new PhysicalProjection(sch3, move(proj_exprs)));
// 	// sink
// 	ops.push_back( new PhysicalHashAggregate(sch3, move(agg_exprs), move(agg_groups)));
	
// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe);
// 	return pipeexec;

// }
// CypherPipelineExecutor* ic9_pipe2(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// 	// friend->comment
// 	Schema sch1;
// 	sch1.addNode("friend");
// 	sch1.addNode("message");	// comment

// 	// attach comment seek
// 	Schema sch2 = sch1;
// 	sch2.addPropertyIntoNode("message", "id", LogicalType::UBIGINT);
// 	sch2.addPropertyIntoNode("message", "content", LogicalType::VARCHAR);
// 	sch2.addPropertyIntoNode("message", "creationDate", LogicalType::BIGINT);
// 	PropertyKeys m_keys;
// 	m_keys.push_back("id");
// 	m_keys.push_back("content");
// 	m_keys.push_back("creationDate");

// 	// filter (_fr _msg m.id m.c .cd)
// 	vector<unique_ptr<Expression>> filter_exprs;
// 	{
// 		unique_ptr<Expression> filter_expr1;
// 		filter_expr1 = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, 
// 							make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 4),	// creationdate
// 							make_unique<BoundConstantExpression>( Value::BIGINT(1289908800000) )	// constant 
// 						);
// 		filter_exprs.push_back(move(filter_expr1));
// 	}

// 	// attach friend id
// 	Schema sch3 = sch2;
// 	sch3.addPropertyIntoNode("friend", "id", LogicalType::UBIGINT);
// 	sch3.addPropertyIntoNode("friend", "firstName", LogicalType::VARCHAR);
// 	sch3.addPropertyIntoNode("friend", "lastName", LogicalType::VARCHAR);
// 	PropertyKeys f_keys;
// 	f_keys.push_back("id");
// 	f_keys.push_back("firstName");
// 	f_keys.push_back("lastName");

// 	// projection (_fr f.id f.fn f.ln _msg m.id m.c m.cd )
// 	Schema sch4;
// 	sch4.addColumn("personId", LogicalType::UBIGINT);
// 	sch4.addColumn("personFirstName", LogicalType::VARCHAR);
// 	sch4.addColumn("personLastName", LogicalType::VARCHAR);
// 	sch4.addColumn("commentId", LogicalType::UBIGINT);
// 	sch4.addColumn("commentContent", LogicalType::VARCHAR);
// 	sch4.addColumn("commentCreationDate", LogicalType::BIGINT);
// 	vector<unique_ptr<Expression>> proj_exprs;
// 	{
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 1) ) );	// fid
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 2) ) );	// ffn
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 3) ) );	// fln
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 5) ) );	// mid
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::VARCHAR, 6) ) );	// mc
// 		proj_exprs.push_back( move( make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 7) ) );	// mcd
// 	}

// 	// orderby
// 	unique_ptr<Expression> order_expr_1 = make_unique<BoundReferenceExpression>(LogicalType::BIGINT, 5);		// creationdate
// 	BoundOrderByNode order1(OrderType::DESCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_1));
// 	unique_ptr<Expression> order_expr_2 = make_unique<BoundReferenceExpression>(LogicalType::UBIGINT, 3);		// commentid
// 	BoundOrderByNode order2(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, move(order_expr_2));
// 	vector<BoundOrderByNode> orders;
// 	orders.push_back(move(order1));
// 	orders.push_back(move(order2));

// // pipe

// 	std::vector<CypherPhysicalOperator *> ops;
// 	//src
// // TODO write herer
// 	// ops
	
// // TODO write her
// 	// sink
// 	ops.push_back( new PhysicalTopNSort(sch4, move(orders)));


// 	vector<CypherPipelineExecutor*> childs;
// 	childs.push_back(prev_pipe);
// 	auto pipe = new CypherPipeline(ops);
// 	auto ctx = new ExecutionContext(&(suite.context));
// 	auto pipeexec = new CypherPipelineExecutor(ctx, pipe, childs);
// 	return pipeexec;

// }
// CypherPipelineExecutor* ic9_pipe3(QueryPlanSuite& suite, CypherPipelineExecutor* prev_pipe) {

// // TODO herer
// 	//produce

// 	// childs!!

// }



// }