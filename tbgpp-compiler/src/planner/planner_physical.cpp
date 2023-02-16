#include "planner.hpp"

#include "mdprovider/MDProviderTBGPP.h"

#include <string>
#include <limits>


namespace s62 {

void Planner::pGenPhysicalPlan(CExpression* orca_plan_root) {

	vector<duckdb::CypherPhysicalOperator*>* final_pipeline_ops = pTraverseTransformPhysicalPlan(orca_plan_root);
	// finally, append getresultset()
	
	// TODO make last result as pipeline

	// validate plan

	// final physical plan is generated in Planner::pipelines
	D_ASSERT( pipelines.size() > 0 );
	return;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTraverseTransformPhysicalPlan(CExpression* plan_expr) {

	// based on op pass call to the corresponding func
	D_ASSERT( plan_expr != nullptr );
	D_ASSERT( plan_expr->Pop()->FPhysical() );
	switch(plan_expr->Pop()->Eopid()) {
		case COperator::EOperatorId::EopPhysicalSerialUnionAll: {
			if( pIsUnionAllOpScanExpression(plan_expr) ) {
				// generate NodeScan
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
	return new vector<duckdb::CypherPhysicalOperator*>(); // TODO fixme
}

bool Planner::pIsUnionAllOpScanExpression(CExpression* plan_expr) {
	/* match pattern UnionAll -> (ComputeScalar) -> TableScan|IndexScan */
	bool result = true;
	D_ASSERT(plan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalSerialUnionAll);
	CExpressionArray *lv1_child = plan_expr->PdrgPexpr();
	const ULONG lv1_child_size = lv1_child->Size();
	for( int i=0; i<lv1_child_size; i++ ){
		CExpression* lv1_child_expr = lv1_child->operator[](i);
		if(!lv1_child_expr->Pop()->FPhysical() ) { continue; }			// check only physical operators, not scalar operators
		result = result &&
			(lv1_child_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalComputeScalar)
				|| (lv1_child_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalTableScan);
		CExpressionArray *lv2_child = lv1_child_expr->PdrgPexpr();
		const ULONG lv2_child_size = lv2_child->Size();
		for(int j=0; j<lv2_child_size; j++ ){
			CExpression* lv2_child_expr = lv2_child->operator[](j);		// check only physical operators, not scalar operators
			if( !lv2_child_expr->Pop()->FPhysical() ) { continue; }
			result = result &&
				(lv2_child_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalTableScan)
				|| (lv2_child_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan);
// TODO what if for index scan???? is this enough?
		}
	}
	return result;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopPhysicalTableScan(CExpression* op) {

	// if leaf node or start of pipeline, generate 
	auto result = new vector<duckdb::CypherPhysicalOperator*>();

	// foreach TableScan, 
		// generate 
		// columns and projection list


	// generate NodeScan

	// return add myoperator to the function
	return result;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopUnionAllForNodeOrEdgeScan(CExpression* op) {

	//  leaf node
	auto result = new vector<duckdb::CypherPhysicalOperator*>();

	// construct pipeline if necessary
		// when constructing pipeline
			// transform vector into pipeline
			// delete that vector

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

}