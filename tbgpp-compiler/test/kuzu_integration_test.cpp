#include <iostream>

#include "gpos/_api.h"
#include "naucrates/init.h"
#include "gpopt/init.h"

#include "gpos/base.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "gpos/memory/CMemoryPool.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "gpopt/operators/CLogicalGet.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "cypher_lexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"

#include "BTNode.h"

using namespace antlr4;
using namespace gpopt;

int _main(int argc, char** argv) {
 
	std::cout << "compiler test start" << std::endl;
	// std::string query = "MATCH (n) RETURN n;";
	//std::string query = "EXPLAIN MATCH (n) RETURN n;";
	std::string query = "MATCH (n), (m)-[r]->(z) WITH n,m,r,z MATCH (x), (y) RETURN m,n,x,y UNION MATCH (n) RETURN n;";
	
	//std::string query = "MATCH (n) RETURN n;";

	std::cout << "Query => " << std::endl << query << std::endl ;
	auto inputStream = ANTLRInputStream(query);
    //auto parserErrorListener = ParserErrorListener();

// Lexer
	std::cout << "[TEST] calling lexer" << std::endl;
    auto cypherLexer = CypherLexer(&inputStream);
    //cypherLexer.removeErrorListeners();
    //cypherLexer.addErrorListener(&parserErrorListener);
	auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

// Parser
	std::cout << "[TEST] generating and calling KuzuCypherParser" << std::endl;
	auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);

	// std::cout << (kcp.oC_AnyCypherOption()->oC_Explain() != nullptr) << std::endl;
	// std::cout << (kcp.oC_AnyCypherOption()->oC_Profile() != nullptr) << std::endl;

// Sematic parsing
	// Transformer
	std::cout << "[TEST] generating transformer" << std::endl;
	kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
	std::cout << "[TEST] calling transformer" << std::endl;
	auto statement = transformer.transform();
	
	// Binder
	std::cout << "[TEST] generating binder" << std::endl;
	auto binder = kuzu::binder::Binder();
	std::cout << "[TEST] calling binder" << std::endl;
	auto boundStatement = binder.bind(*statement);
	kuzu::binder::BoundStatement * bst = boundStatement.get();
	BTTree<kuzu::binder::ParseTreeNode> printer(bst, &kuzu::binder::ParseTreeNode::getChildren, &kuzu::binder::BoundStatement::getName);
	// WARNING - printer should be disabled when processing following compilation step.
	std::cout << "Tree => " << std::endl;
	printer.print();
	std::cout << std::endl;


// bind orca

	std::cout << "[TEST] orca init" << std::endl;
	struct gpos_init_params gpos_params = {NULL};

	gpos_init(&gpos_params);
	gpdxl_init();
	gpopt_init();

	std::cout << "[TEST] orca memory pool" << std::endl;
	std::cout << "[TEST] orca engine" << std::endl;
	
	//CExpression *pexprLeft = PexprLogicalGet(mp);

	std::cout << "compiler test end" << std::endl;
	return 0;
}

int main(int argc, char** argv) {
	try{
		int out = _main(argc, argv);
	} catch (CException ex) {
		std::cerr << ex.Major() << " " << ex.Minor() << std::endl;
	}
}