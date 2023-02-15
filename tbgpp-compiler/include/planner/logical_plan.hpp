#pragma once

#include "main/client_context.hpp"

#include <iostream>

#include "gpos/_api.h"
#include "naucrates/init.h"
#include "gpopt/init.h"

#include "unittest/gpopt/engine/CEngineTest.h"
#include "gpos/test/CUnittest.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/base.h"
#include "unittest/gpopt/CTestUtils.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "gpos/memory/CMemoryPool.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "gpopt/operators/CLogicalGet.h"

#include "gpos/_api.h"
#include "gpos/common/CMainArgs.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CFSimulatorTestExt.h"
#include "gpos/test/CUnittest.h"
#include "gpos/types.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/init.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/base/CDistributionSpecStrictSingleton.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CScalarIdent.h"

// #include "planner/planner.hpp"

#include "naucrates/init.h"

#include "gpopt/operators/CLogicalInnerJoin.h"

#include "gpopt/metadata/CTableDescriptor.h"

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "kuzu/binder/bound_statement.h"
#include "kuzu/binder/query/reading_clause/bound_reading_clause.h"
#include "kuzu/binder/query/reading_clause/bound_match_clause.h"
#include "kuzu/binder/expression/expression.h"

#include "assert.hpp"	
#include "logical_schema.hpp"

// TODO need to cleanup imports

namespace s62 {

using namespace kuzu::binder;

class LogicalPlan {

public:
	LogicalPlan(CExpression* tree_root, LogicalSchema root_schema)
		: tree_root(tree_root), root_schema(root_schema) {
		D_ASSERT( tree_root != nullptr );
	}
	~LogicalPlan() {}

	void addUnaryParentOp(CExpression* parent) {
		tree_root = parent;
		// update internal states
	}
	void addBinaryParentOp(CExpression* parent, LogicalPlan* rhs_sibling) {
		tree_root = parent;
		// update internal states

		// TODO after integrating information, lhs must deallocate rhs after merge.
	}

 	inline CExpression* getPlanExpr() { return tree_root; }
	inline LogicalSchema* getSchema() { return &root_schema; }
	

private:
	CExpression* tree_root;
	LogicalSchema root_schema;

};


}