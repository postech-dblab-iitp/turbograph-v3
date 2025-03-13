#pragma once

#include <iostream>
#include "gpopt/operators/CExpression.h"
#include "gpopt/base/CColRef.h"
#include "converter/c2o/logical_schema.hpp"

namespace s62 {

using namespace gpopt;

class LogicalPlan {

public:
	LogicalPlan(CExpression* tree_root, LogicalSchema root_schema, CColRefArray* root_colref_array)
		: tree_root(tree_root), root_schema(root_schema) {

		D_ASSERT( tree_root != nullptr );
		D_ASSERT( root_colref_array != nullptr);
		D_ASSERT(root_schema.size() == root_colref_array->Size());
	}
	LogicalPlan(CExpression* tree_root, LogicalSchema root_schema)
		: tree_root(tree_root), root_schema(root_schema){
			
		D_ASSERT( tree_root != nullptr );
	}
	~LogicalPlan() {}

	void setTreeRoot(CExpression *root) {
		tree_root = root;
	}

	void addUnaryParentOp(CExpression* parent) {
		tree_root = parent;
	}

	void addBinaryParentOp(CExpression* parent, LogicalPlan* rhs_sibling) {
		tree_root = parent;
	}

	void setSchema(LogicalSchema root_schema_) {
		root_schema = move(root_schema_);
	}
	
 	inline CExpression* getPlanExpr() { return tree_root; }
	inline LogicalSchema* getSchema() { return &root_schema; }
	

private:
	CExpression* tree_root;
	LogicalSchema root_schema;
};


}