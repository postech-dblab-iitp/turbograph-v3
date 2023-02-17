#include "planner.hpp"

#include <string>
#include <limits>

#include "mdprovider/MDProviderTBGPP.h"
#include "execution/physical_operator/physical_produce_results.hpp"


namespace s62 {

void Planner::pGenPhysicalPlan(CExpression* orca_plan_root) {

	vector<duckdb::CypherPhysicalOperator*> final_pipeline_ops = *pTraverseTransformPhysicalPlan(orca_plan_root);
	
	// finally, append PhysicalProduceResults
	duckdb::CypherSchema null_schema;
	auto op = new duckdb::PhysicalProduceResults(null_schema);
	final_pipeline_ops.push_back(op);
	D_ASSERT(final_pipeline_ops.size() > 0);
	auto final_pipeline = new duckdb::CypherPipeline(final_pipeline_ops);

	this->pipelines.push_back(final_pipeline);

	// validate plan

	// final physical plan is generated in Planner::pipelines
	D_ASSERT( pipelines.size() > 0 );
	return;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTraverseTransformPhysicalPlan(CExpression* plan_expr) {

	vector<duckdb::CypherPhysicalOperator*>* result = nullptr;

	// based on op pass call to the corresponding func
	D_ASSERT( plan_expr != nullptr );
	D_ASSERT( plan_expr->Pop()->FPhysical() );
	switch(plan_expr->Pop()->Eopid()) {
		case COperator::EOperatorId::EopPhysicalSerialUnionAll: {
			if( pIsUnionAllOpAccessExpression(plan_expr) ) {
				// NodeScan / NodeIndexScan
				return pTransformEopUnionAllForNodeOrEdgeScan(plan_expr);
			} else {
				// union all
				D_ASSERT(false);
			}
			break;
		}
		case COperator::EOperatorId::EopPhysicalTableScan: {
			
			break;
		}
		default:
			D_ASSERT(false);
	}
	// unreached
	D_ASSERT(false);
}



// vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopPhysicalTableScan(CExpression* plan_expr) {
	
	
// 	// foreach TableScan, 
// 		// generate oids
// 		// columns and projection list

// 	// assert len oids == len mapping

// 	// return add myoperator to the function
	
// }

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopUnionAllForNodeOrEdgeScan(CExpression* plan_expr) {

	/* MATCH UnionAll -> ComputeScalar -> TableScan|IndexScan */
	D_ASSERT(plan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalSerialUnionAll);

	// leaf node
	auto result = new vector<duckdb::CypherPhysicalOperator*>();
	vector<uint64_t> oids;
	vector<vector<int64_t>> projection_mapping;
	
	CExpressionArray *lv1_child = plan_expr->PdrgPexpr();
	const ULONG lv1_child_size = lv1_child->Size();
	for( int i=0; i<lv1_child_size; i++ ){
		// generate 
	}

	// collect object ids
	


	// for each object id, generate projection mapping. (if null projection required, add -1)

		// assert vector all sized same

	// how to get types? -> access catalog?
		// this api nono...

	// duckdb::CypherSchema tmp_schema;
	// tmp_schema.setStoredTypes();
	// duckdb::CypherPhysicalOperator* op =
	// 	duckdb::PhysicalNodeScan(CypherSchema& sch, LabelSet labels, PropertyKeys pk);
	// return add myoperator to the result
	
	return result;
}


// vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopPhysicalComputeScalar(CExpression* op) {

// 	D_ASSERT(false);
// 	return new vector<duckdb::CypherPhysicalOperator*>();
// }

// vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopPhysicalFilter(CExpression* op) {

// 	// call traverse() for child operators

// 	// construct pipeline if necessary
// 		// when constructing pipeline
// 			// transform vector into pipeline
// 			// delete that vector

// 	// return add myoperator to the function

// 	D_ASSERT(false);
// 	return new vector<duckdb::CypherPhysicalOperator*>();

// }

bool Planner::pMatchPhysicalOpPattern(CExpression* root, vector<COperator::EOperatorId>& pattern, uint64_t pattern_root_idx) {

	D_ASSERT(pattern.size() > 0);
	D_ASSERT(pattern_root_idx < pattern.size());

	// conjunctive checks
	bool match = true;

	// recursively iterate
		// if depth shorter than pattern, also return false.
	CExpressionArray *children = root->PdrgPexpr();
	const ULONG children_size = children->Size();

	// construct recursive child_pattern_match
	for( int i=0; i<children_size; i++ ){
		CExpression* child_expr = children->operator[](i);
		// check pattern for child
		if( !child_expr->Pop()->FPhysical() ) {
			// dont care other than phyiscal operator
			continue;
		}
		match = match && pMatchPhysicalOpPattern(child_expr, pattern, pattern_root_idx+1);
	}
	// check pattern for root
	match = match && root->Pop()->Eopid() == pattern[pattern_root_idx];

	return match;
}

bool Planner::pIsUnionAllOpAccessExpression(CExpression* expr) {

	auto p1 = vector<COperator::EOperatorId>({
		COperator::EOperatorId::EopPhysicalSerialUnionAll,
		COperator::EOperatorId::EopPhysicalComputeScalar,
		COperator::EOperatorId::EopPhysicalTableScan,
	});
	auto p2 = vector<COperator::EOperatorId>({
		COperator::EOperatorId::EopPhysicalSerialUnionAll,
		COperator::EOperatorId::EopPhysicalComputeScalar,
		COperator::EOperatorId::EopPhysicalIndexScan,
	});
	return pMatchPhysicalOpPattern(expr, p1) || pMatchPhysicalOpPattern(expr, p2);
}


}