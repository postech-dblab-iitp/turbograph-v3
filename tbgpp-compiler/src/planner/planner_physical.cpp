#include "planner.hpp"

#include <string>
#include <limits>
// orca operators


namespace s62 {

void Planner::pGenPhysicalPlan(CExpression* orca_plan_root) {

	vector<duckdb::CypherPhysicalOperator*> final_pipeline_ops = *pTraverseTransformPhysicalPlan(orca_plan_root);
	
	// finally, append PhysicalProduceResults
	duckdb::CypherSchema null_schema;
	auto op = new duckdb::PhysicalProduceResults(null_schema);
	final_pipeline_ops.push_back(op);
	D_ASSERT(final_pipeline_ops.size() > 0);
	auto final_pipeline = new duckdb::CypherPipeline(final_pipeline_ops);

	pipelines.push_back(final_pipeline);
	
	// validate plan
	D_ASSERT( pValidatePipelines() );
	
	return;
}

bool Planner::pValidatePipelines() {
	bool ok = true;
	ok = ok && pipelines.size() > 0;
	for( auto& pipeline: pipelines ) {
		ok = ok && (pipeline->pipelineLength >= 2);
	}
	ok = ok && (pipelines[pipelines.size()-1]->GetSink()->ToString() == "ProduceResults");
	return ok;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTraverseTransformPhysicalPlan(CExpression* plan_expr) {

	vector<duckdb::CypherPhysicalOperator*>* result = nullptr;

	// based on op pass call to the corresponding func
	D_ASSERT( plan_expr != nullptr );
	D_ASSERT( plan_expr->Pop()->FPhysical() );
	switch(plan_expr->Pop()->Eopid()) {
		case COperator::EOperatorId::EopPhysicalSerialUnionAll: {
			if( pIsUnionAllOpAccessExpression(plan_expr) ) {
				/* UnionAll-ComputeScalar-TableScan|IndexScan => NodeScan|NodeIndexScan*/
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

	/* UnionAll-ComputeScalar-TableScan|IndexScan => NodeScan|NodeIndexScan*/
	D_ASSERT(plan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalSerialUnionAll);

	// leaf node
	auto result = new vector<duckdb::CypherPhysicalOperator*>();
	vector<uint64_t> oids;
	vector<vector<uint64_t>> projection_mapping;
	
	CExpressionArray *projections = plan_expr->PdrgPexpr();
	const ULONG projections_size = projections->Size();

	vector<duckdb::LogicalType> global_types;

	for( int i=0; i<projections_size; i++ ){
		CExpression* projection = projections->operator[](i);
		CExpression* scan_expr = projection->PdrgPexpr()->operator[](0);
		CExpression* proj_list_expr = projection->PdrgPexpr()->operator[](1);
		const ULONG proj_list_expr_size = proj_list_expr->PdrgPexpr()->Size();
		
		CPhysicalTableScan* scan_op = (CPhysicalTableScan*)scan_expr->Pop();
// TODO for index scan
		CMDIdGPDB* table_mdid = CMDIdGPDB::CastMdid( scan_op->Ptabdesc()->MDId() );
		OID table_obj_id = table_mdid->Oid();

		// collect object ids
		oids.push_back(table_obj_id);
		vector<duckdb::LogicalType> local_types;
		
		// for each object id, generate projection mapping. (if null projection required, ulong::max)
		projection_mapping.push_back( vector<uint64_t>() );
		for(int j = 0; j < proj_list_expr_size; j++) {
			CExpression* proj_elem = proj_list_expr->PdrgPexpr()->operator[](j);
			auto scalarident_pattern = vector<COperator::EOperatorId>({
				COperator::EOperatorId::EopScalarProjectElement,
				COperator::EOperatorId::EopScalarIdent });

			CExpression* proj_item;
			if( pMatchExprPattern(proj_elem, scalarident_pattern) ) {
				/* CScalarProjectList - CScalarIdent */
				CScalarIdent* ident_op = (CScalarIdent*)proj_elem->PdrgPexpr()->operator[](0)->Pop();
				auto col_idx = pGetColIdxOfColref( scan_expr->DeriveOutputColumns(), ident_op->Pcr());	// compare using colref positions
				projection_mapping[i].push_back(col_idx);
				// add local types
				CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(ident_op->Pcr()->RetrieveType()->MDId());
				OID type_oid = type_mdid->Oid();
				local_types.push_back( pConvertTypeOidToLogicalType(type_oid) );
				
			} else {
				/* CScalarProjectList - CScalarConst (null) */
				projection_mapping[i].push_back(std::numeric_limits<uint64_t>::max());
				CScalarConst* const_null_op = (CScalarConst*)proj_elem->PdrgPexpr()->operator[](0)->Pop();
				// add local types
				CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid( const_null_op->MdidType() );
				OID type_oid = type_mdid->Oid();
				local_types.push_back( pConvertTypeOidToLogicalType(type_oid) );
			}
		}
		// aggregate local types to global types
		if( i == 0 ) {
			for(auto& t: local_types) {
				global_types.push_back( t );
			}
		}
		// TODO else check if type conforms
	}
	// TODO assert oids size = mapping size

	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(global_types);
	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalNodeScan(tmp_schema, oids, projection_mapping);
	result->push_back(op);
	
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

bool Planner::pMatchExprPattern(CExpression* root, vector<COperator::EOperatorId>& pattern, uint64_t pattern_root_idx, bool physical_op_only) {

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
		if( physical_op_only && !child_expr->Pop()->FPhysical() ) {
			// dont care other than phyiscal operator
			continue;
		}
		match = match && pMatchExprPattern(child_expr, pattern, pattern_root_idx+1, physical_op_only);
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
	return pMatchExprPattern(expr, p1, 0, true) || pMatchExprPattern(expr, p2, 0, true);
}

uint64_t Planner::pGetColIdxOfColref(CColRefSet* refset, const CColRef* target_col) {
	
	CMemoryPool* mp = this->memory_pool;

	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
	refset->ExtractColIds(mp, colids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	const ULONG size = colids->Size();
	for (ULONG idx = 0; idx < size; idx++) {
		ULONG colid = *((*colids)[idx]);
		auto test = col_factory->LookupColRef(colid);
		if( CColRef::Equals(test, target_col) ) {
			return idx;
		}
	}
	D_ASSERT(false);
	return 0; // to prevent compiler warning
	
}


}