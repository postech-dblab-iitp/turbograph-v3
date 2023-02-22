#include "planner.hpp"

#include <string>
#include <limits>

// orca operators


// locally used duckdb operators
#include "execution/physical_operator/physical_produce_results.hpp"
#include "execution/physical_operator/physical_node_scan.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "planner/expression/bound_reference_expression.hpp"


namespace s62 {

void Planner::pGenPhysicalPlan(CExpression* orca_plan_root) {

	vector<duckdb::CypherPhysicalOperator*> final_pipeline_ops = *pTraverseTransformPhysicalPlan(orca_plan_root);
	
	// Append PhysicalProduceResults
	duckdb::CypherSchema final_output_schema = final_pipeline_ops[final_pipeline_ops.size()-1]->schema;
	auto op = new duckdb::PhysicalProduceResults(final_output_schema);
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
			} else if ( pIsUnionAllForProjection(plan_expr) ) {
				/* UnionAll => Projection */
// TODO here, unionall is a projection operator temporary. need projection operator
				return pTransformEopUnionAllForProjection(plan_expr);
			} else {
				D_ASSERT(false);
			}
			break;
		}
		case COperator::EOperatorId::EopPhysicalTableScan: {
			/* TableScan => EdgeScan*/
			return pTransformEopTableScan(plan_expr);
		}
		default:
			D_ASSERT(false);
	}
	// unreached
	D_ASSERT(false);
}


vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopTableScan(CExpression* plan_expr) {
	
	/* TableScan => NodeScan|EdgeScan*/
// TODO need edge scan version
	// leaf node
	auto result = new vector<duckdb::CypherPhysicalOperator*>();
	
	CPhysicalTableScan* scan_op = (CPhysicalTableScan*)plan_expr->Pop();

	CMDIdGPDB* table_mdid = CMDIdGPDB::CastMdid( scan_op->Ptabdesc()->MDId() );
	OID table_obj_id = table_mdid->Oid();
	vector<duckdb::LogicalType> types;
	CColRefSet* columns = plan_expr->DeriveOutputColumns();
	uint64_t cols_size = columns->Size();
	
	// oids / projection_mapping 
	vector<uint64_t> oids;
	oids.push_back(table_obj_id);
	vector<vector<uint64_t>> projection_mapping;
	vector<uint64_t> ident_mapping;
	for(uint64_t i = 0 ; i < cols_size; i++) {
		if( scan_op->PdrgpcrOutput()->IndexOf(lGetIthColRef(columns, i)) != gpos::ulong_max ) {		// check if column is scanned
			auto table_col_idx = pGetColIdxFromTable(table_obj_id, lGetIthColRef(columns, i));
			ident_mapping.push_back(table_col_idx);
		}
	}
	D_ASSERT(ident_mapping.size() == columns->Size());
	projection_mapping.push_back(ident_mapping);

	// types
	for(uint64_t i = 0 ; i < cols_size; i++) {
		if( scan_op->PdrgpcrOutput()->IndexOf(lGetIthColRef(columns, i)) != gpos::ulong_max ) {
			CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(lGetIthColRef(columns, i)->RetrieveType()->MDId());
			OID type_oid = type_mdid->Oid();
			types.push_back( pConvertTypeOidToLogicalType(type_oid) );
		}
	}
	D_ASSERT(types.size() == ident_mapping.size());

	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(types);
	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalNodeScan(tmp_schema, oids, projection_mapping);
	result->push_back(op);
	
	return result;	
}

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
		CPhysicalScan* scan_op = (CPhysicalScan*) scan_expr->Pop();
		CExpression* proj_list_expr = projection->PdrgPexpr()->operator[](1);
		const ULONG proj_list_expr_size = proj_list_expr->PdrgPexpr()->Size();
		
		// TODO for index scan?
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
				auto col_idx = pGetColIdxFromTable( table_obj_id, ident_op->Pcr());

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
				global_types.push_back(t);
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


vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopUnionAllForProjection(CExpression* plan_expr) {

	/* Non-root - call single child */
	vector<duckdb::CypherPhysicalOperator*>* result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

	vector<unique_ptr<duckdb::Expression>> proj_exprs;
	vector<duckdb::LogicalType> types;

	// refer to output
	CPhysicalSerialUnionAll* union_all_op = (CPhysicalSerialUnionAll*) plan_expr->Pop();
	CColRef2dArray* _input_arrays = union_all_op->PdrgpdrgpcrInput();
	D_ASSERT(_input_arrays->Size() == 1);

	CColRefArray* input_array = _input_arrays->operator[](0);
	CColRefArray* output_array = union_all_op->PdrgpcrOutput();

	for(ULONG out_idx=0; out_idx < output_array->Size(); out_idx++) {
		ULONG in_idx = input_array->IndexOf( output_array->operator[](out_idx) );
		D_ASSERT( in_idx != gpos::ulong_max);
		CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid( output_array->operator[](out_idx)->RetrieveType()->MDId() );
		OID type_oid = type_mdid->Oid();
		types.push_back( pConvertTypeOidToLogicalType(type_oid) );
		// gen projection expression
		proj_exprs.push_back(
			make_unique<duckdb::BoundReferenceExpression>( pConvertTypeOidToLogicalTypeId(type_oid), (int)in_idx)
		);
	}
	D_ASSERT(types.size() == proj_exprs.size());

	/* Generate operator and push */
	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(types);
	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalProjection(tmp_schema, std::move(proj_exprs));
	result->push_back(op);

	return result;
}

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
		if( pattern_root_idx + 1 < pattern.size()) {
			// more patterns to check
			match = match && pMatchExprPattern(child_expr, pattern, pattern_root_idx+1, physical_op_only);	
		}
		
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

bool Planner::pIsUnionAllForProjection(CExpression* expr) {
	auto p1 = vector<COperator::EOperatorId>({
		COperator::EOperatorId::EopPhysicalSerialUnionAll
	});
	bool is_unionall_with_single_child = expr->PdrgPexpr()->Size() == 1;
	return is_unionall_with_single_child;
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

uint64_t Planner::pGetColIdxFromTable(OID table_oid, const CColRef* target_col) {

	CMemoryPool* mp = this->memory_pool;
	auto* rel_mdid = lGenRelMdid(table_oid);
	auto* rel = lGetMDAccessor()->RetrieveRel(rel_mdid);

	D_ASSERT( table_col_mapping.count(table_oid) );
	for(int orig_col_id = 0; orig_col_id < table_col_mapping[table_oid].size(); orig_col_id++) {
		if( target_col == table_col_mapping[table_oid][orig_col_id] ) {
			return (uint64_t)orig_col_id;
		}
	}
	D_ASSERT(false);
	return 0;
}


}