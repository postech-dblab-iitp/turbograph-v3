// #include <iostream>

// #include "gpos/_api.h"
// #include "naucrates/init.h"
// #include "gpopt/init.h"

// #include "unittest/gpopt/engine/CEngineTest.h"
// #include "gpos/test/CUnittest.h"
// #include "gpos/common/CMainArgs.h"

// #include "gpos/base.h"
// #include "unittest/gpopt/CTestUtils.h"
// #include "gpopt/engine/CEngine.h"
// #include "gpopt/operators/CExpression.h"
// #include "gpopt/base/CColRef.h"
// #include "gpos/memory/CMemoryPool.h"
// #include "naucrates/md/CMDIdGPDB.h"
// #include "gpopt/operators/CLogicalGet.h"

// #include "gpos/_api.h"
// #include "gpos/common/CMainArgs.h"
// #include "gpos/memory/CAutoMemoryPool.h"
// #include "gpos/test/CFSimulatorTestExt.h"
// #include "gpos/test/CUnittest.h"
// #include "gpos/types.h"

// #include "gpopt/engine/CEnumeratorConfig.h"
// #include "gpopt/engine/CStatisticsConfig.h"
// #include "gpopt/init.h"
// #include "gpopt/base/CColRefSetIter.h"
// #include "gpopt/mdcache/CMDCache.h"
// #include "gpopt/minidump/CMinidumperUtils.h"
// #include "gpopt/optimizer/COptimizerConfig.h"
// #include "gpopt/xforms/CXformFactory.h"
// #include "gpopt/eval/CConstExprEvaluatorDefault.h"
// #include "gpopt/base/CDistributionSpecStrictSingleton.h"

// #include "planner/planner.hpp"

// #include "naucrates/init.h"

// #include "gpopt/operators/CLogicalInnerJoin.h"

// #include "gpopt/metadata/CTableDescriptor.h"

// #include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
// #include "CypherLexer.h"
// #include "kuzu/parser/transformer.h"
// #include "kuzu/binder/binder.h"

// #include "BTNode.h"

// #include "naucrates/traceflags/traceflags.h"



// #include "mdprovider/MDProviderS62.h"

// using namespace antlr4;
// using namespace gpopt;

// int _main(int argc, char** argv) {
 
// 	std::cout << "compiler test start" << std::endl;
// 	std::vector <std::string> _tokens;
// 	for (int i=1; i < argc; ++i) {
// 		_tokens.push_back(std::string(argv[i]));
// 	}

// 	// std::string query = "MATCH (n) RETURN n;";
// 	//std::string query = "EXPLAIN MATCH (n) RETURN n;";
// 	//std::string query = "MATCH (n), (m)-[r]->(z) WITH n,m,r,z MATCH (x), (y) RETURN m,n,x,y UNION MATCH (n) RETURN n;";
// 	//std::string query = "MATCH (a:Person1:Person|Person3)-[x]->(b), (bbb)-[y]->(c) MATCH (c)-[z]->(d) MATCH (e) RETURN a.name,x,b,bbb,y,c,z,d,e";
// 	// TODO in return clause, cases like id(n) should be considered as well.

// 	std::string query = "";
// 	for (auto t: _tokens) {
// 		query += t;
// 	}
// 	if( query == "" ) {
// 		return 1;
// 	}
// 	//std::string query = "MATCH (n) RETURN n;";

// 	std::cout << "Query => " << std::endl << query << std::endl ;
// 	auto inputStream = ANTLRInputStream(query);
//     //auto parserErrorListener = ParserErrorListener();

// // Lexer
// 	std::cout << "[TEST] calling lexer" << std::endl;
//     auto cypherLexer = CypherLexer(&inputStream);
//     //cypherLexer.removeErrorListeners();
//     //cypherLexer.addErrorListener(&parserErrorListener);
// 	auto tokens = CommonTokenStream(&cypherLexer);
//     tokens.fill();

// // Parser
// 	std::cout << "[TEST] generating and calling KuzuCypherParser" << std::endl;
// 	auto kuzuCypherParser = s62::parser::KuzuCypherParser(&tokens);

// 	// std::cout << (kcp.oC_AnyCypherOption()->oC_Explain() != nullptr) << std::endl;
// 	// std::cout << (kcp.oC_AnyCypherOption()->oC_Profile() != nullptr) << std::endl;

// // Sematic parsing
// 	// Transformer
// 	std::cout << "[TEST] generating transformer" << std::endl;
// 	s62::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
// 	std::cout << "[TEST] calling transformer" << std::endl;
// 	auto statement = transformer.transform();
	
// 	// Binder
// 	std::cout << "[TEST] generating binder" << std::endl;
// 	auto binder = s62::binder::Binder(nullptr);
// 	std::cout << "[TEST] calling binder" << std::endl;
// 	auto boundStatement = binder.bind(*statement);
// 	s62::binder::BoundStatement * bst = boundStatement.get();
// 	BTTree<s62::binder::ParseTreeNode> printer(bst, &s62::binder::ParseTreeNode::getChildNodes, &s62::binder::BoundStatement::getName);
// 	// WARNING - printer should be disabled when processing following compilation step.
// 	std::cout << "Tree => " << std::endl;
// 	printer.print();
// 	std::cout << std::endl;

// // our logical plan
// 	std::string path = "../s62-compiler/test/minidumps/gdb_test1_a-r-b.mdp";
// 	auto planner = s62::Planner(s62::MDProviderType::MEMORY, nullptr, path);
// 	planner.execute(bst);
// }

// int main(int argc, char** argv) {
// 	try{
// 		int out = _main(argc, argv);
// 	} catch (CException ex) {
// 		std::cerr << ex.Major() << " " << ex.Minor() << std::endl;
// 	}
// }
