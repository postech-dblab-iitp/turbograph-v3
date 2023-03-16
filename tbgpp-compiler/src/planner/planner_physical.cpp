#include "planner.hpp"

#include <string>
#include <limits>
#include <algorithm>

// orca operators

// locally used duckdb operators
#include "execution/physical_operator/physical_produce_results.hpp"
#include "execution/physical_operator/physical_node_scan.hpp"
#include "execution/physical_operator/physical_projection.hpp"
#include "execution/physical_operator/physical_adjidxjoin.hpp"
#include "execution/physical_operator/physical_id_seek.hpp"
#include "execution/physical_operator/physical_sort.hpp"
#include "execution/physical_operator/physical_top_n_sort.hpp"


#include "planner/expression/bound_reference_expression.hpp"

#include "common/enums/join_type.hpp"



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

	/* Matching order
		- UnionAll-ComputeScalar-TableScan|IndexScan => NodeScan|NodeIndexScan
		- Projection => Projection
		- TableScan => EdgeScan
		// TODO fillme
	*/

	// based on op pass call to the corresponding func
	D_ASSERT( plan_expr != nullptr );
	D_ASSERT( plan_expr->Pop()->FPhysical() );

	switch(plan_expr->Pop()->Eopid()) {
		case COperator::EOperatorId::EopPhysicalSerialUnionAll: {
			if( pIsUnionAllOpAccessExpression(plan_expr) ) {
				result = pTransformEopUnionAllForNodeOrEdgeScan(plan_expr);
			} else {
				D_ASSERT(false);
			}
			break;
		}
		case COperator::EOperatorId::EopPhysicalInnerIndexNLJoin: {
			if( pIsIndexJoinOnPhysicalID(plan_expr) ) {
				result = pTransformEopPhysicalInnerIndexNLJoinToIdSeek(plan_expr);
			} else {
				result = pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(plan_expr);
			}
			break;
		}
		case COperator::EOperatorId::EopPhysicalComputeScalarColumnar: {
			// TODO fixme need to check if P - S - C pattern or P - C pattern is applied
			result = pTransformEopProjectionColumnar(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalFilter: {
			// TODO currently only support Filter + Scan
			D_ASSERT( plan_expr->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalTableScan );
			result = pTransformEopFilterWithScanPushdown(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalTableScan: {
			result = pTransformEopTableScan(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalLimit: {
			result = pTransformEopLimit(plan_expr);
			break;
		}
		case COperator::EOperatorId::EopPhysicalSort: {
			result = pTransformEopSort(plan_expr);
			break;
		}
		default:
			D_ASSERT(false);
			break;
	}
	D_ASSERT(result != nullptr);
	
	// ASSERT that 
	
	return result;
}


vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopTableScan(CExpression* plan_expr) {
	
	auto* mp = this->memory_pool;
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
		if( scan_op->PdrgpcrOutput()->IndexOf(columns->Pdrgpcr(mp)->operator[](i)) != gpos::ulong_max ) {		// check if column is scanned
			auto table_col_idx = pGetColIdxFromTable(table_obj_id, columns->Pdrgpcr(mp)->operator[](i));
			ident_mapping.push_back(table_col_idx);
		}
	}
	D_ASSERT(ident_mapping.size() == columns->Size());
	projection_mapping.push_back(ident_mapping);

	// types
	for(uint64_t i = 0 ; i < cols_size; i++) {
		if( scan_op->PdrgpcrOutput()->IndexOf(columns->Pdrgpcr(mp)->operator[](i)) != gpos::ulong_max ) {
			CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(columns->Pdrgpcr(mp)->operator[](i)->RetrieveType()->MDId());
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

// TODO need to integrate with table scan
vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopFilterWithScanPushdown(CExpression* plan_expr) {
	/* Filter + Scan */
	CMemoryPool* mp = this->memory_pool;
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	D_ASSERT(plan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalFilter);
	D_ASSERT(plan_expr->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalTableScan);

	auto result = new vector<duckdb::CypherPhysicalOperator*>();
	
	CPhysicalFilter* filter_op = (CPhysicalFilter*)plan_expr->Pop();
	CExpression *scan_expr = plan_expr->operator[](0);
	CExpression *filter_expr = plan_expr->operator[](1);
	CPhysicalTableScan* scan_op = (CPhysicalTableScan*)scan_expr->Pop();
	
	// Find predicates
		// TODO we currently support predicate patterns with X = 1; where X is a property
	D_ASSERT( filter_expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarCmp );
	D_ASSERT( filter_expr->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent );
	D_ASSERT( filter_expr->operator[](1)->Pop()->Eopid() == COperator::EOperatorId::EopScalarConst );

	CColRefTable *lhs_colref = (CColRefTable*)(col_factory->LookupColRef( ((CScalarIdent*)filter_expr->operator[](0)->Pop())->Pcr()->Id() ));
	gpos::INT lhs_attrnum = lhs_colref->AttrNum();
	gpos::ULONG pred_attr_pos = lGetMDAccessor()->RetrieveRel(lhs_colref->GetMdidTable())->GetPosFromAttno(lhs_attrnum);

	CDatumGenericGPDB *datum = (CDatumGenericGPDB*)(((CScalarConst*)filter_expr->operator[](1)->Pop())->GetDatum());
	
	duckdb::Value literal_val = DatumSerDes::DeserializeOrcaByteArrayIntoDuckDBValue(
									CMDIdGPDB::CastMdid(datum->MDId())->Oid(),
									datum->GetByteArrayValue(),
									(uint64_t) datum->Size());


	// oids / projection_mapping 
	CMDIdGPDB* table_mdid = CMDIdGPDB::CastMdid( scan_op->Ptabdesc()->MDId() );
	OID table_obj_id = table_mdid->Oid();
	vector<duckdb::LogicalType> types;
	CColRefSet* columns = scan_expr->DeriveOutputColumns();
	uint64_t cols_size = columns->Size();
	
	// oids / projection_mapping 
	vector<uint64_t> oids;
	oids.push_back(table_obj_id);
	vector<vector<uint64_t>> projection_mapping;
	vector<uint64_t> ident_mapping;
	for(uint64_t i = 0 ; i < cols_size; i++) {
		if( scan_op->PdrgpcrOutput()->IndexOf(columns->Pdrgpcr(mp)->operator[](i)) != gpos::ulong_max ) {		// check if column is scanned
			auto table_col_idx = pGetColIdxFromTable(table_obj_id, columns->Pdrgpcr(mp)->operator[](i));
			ident_mapping.push_back(table_col_idx);
		}
	}
	D_ASSERT(ident_mapping.size() == columns->Size());
	projection_mapping.push_back(ident_mapping);

	// types
	for(uint64_t i = 0 ; i < cols_size; i++) {
		if( scan_op->PdrgpcrOutput()->IndexOf(columns->Pdrgpcr(mp)->operator[](i)) != gpos::ulong_max ) {
			CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(columns->Pdrgpcr(mp)->operator[](i)->RetrieveType()->MDId());
			OID type_oid = type_mdid->Oid();
			types.push_back( pConvertTypeOidToLogicalType(type_oid) );
		}
	}
	D_ASSERT(types.size() == ident_mapping.size());

	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(types);
	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalNodeScan(tmp_schema, oids, projection_mapping, pred_attr_pos, literal_val);
	result->push_back(op);
	
	return result;


}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopUnionAllForNodeOrEdgeScan(CExpression* plan_expr) {

	auto* mp = this->memory_pool;
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
				auto col_idx = pGetColIdxFromTable(table_obj_id, ident_op->Pcr());

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



vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopProjectionColumnar(CExpression* plan_expr) {

	CMemoryPool* mp = this->memory_pool;

	/* Non-root - call single child */
	vector<duckdb::CypherPhysicalOperator*>* result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

	vector<unique_ptr<duckdb::Expression>> proj_exprs;
	vector<duckdb::LogicalType> types;

	CPhysicalComputeScalarColumnar* proj_op = (CPhysicalComputeScalarColumnar*) plan_expr->Pop();
	CExpression *pexprProjRelational = (*plan_expr)[0];	// Prev op
	CColRefArray* child_cols = pexprProjRelational->DeriveOutputColumns()->Pdrgpcr(mp);
	CExpression *pexprProjList = (*plan_expr)[1];		// Projection list

	for(ULONG elem_idx = 0; elem_idx < pexprProjList->Arity(); elem_idx ++ ){
		CExpression *pexprProjElem = pexprProjList->operator[](elem_idx);	// CScalarProjectElement
		CExpression *pexprScalarExpr = pexprProjElem->operator[](0);		// CScalar... - expr tree root
// TODO need to recursively build plan!!! - need separate function for this.
		switch (pexprScalarExpr->Pop()->Eopid()) {
			case COperator::EopScalarIdent: {
				CScalarIdent* ident_op = (CScalarIdent*)pexprScalarExpr->Pop();
				// which operator it belongs to???
				ULONG child_index = child_cols->IndexOf(ident_op->Pcr());
				D_ASSERT(child_index != gpos::ulong_max);
				CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(ident_op->Pcr()->RetrieveType()->MDId() );
				OID type_oid = type_mdid->Oid();
				types.push_back( pConvertTypeOidToLogicalType(type_oid) );
				proj_exprs.push_back(
					make_unique<duckdb::BoundReferenceExpression>( pConvertTypeOidToLogicalTypeId(type_oid), (int)child_index )
				);
				break;
			}
			default: {
				D_ASSERT(false); // NOT implemented yet
			}
		}
	}

	D_ASSERT( pexprProjList->Arity() == proj_exprs.size() );

	/* Generate operator and push */
	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(types);
	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalProjection(tmp_schema, std::move(proj_exprs));
	result->push_back(op);

	child_cols->Release();

	return result;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopPhysicalInnerIndexNLJoinToAdjIdxJoin(CExpression* plan_expr) {

	CMemoryPool* mp = this->memory_pool;

	// actuall since this adjidxjoin there is second child, we integrate as single child
		// first child = outer (blablabla...)
		// second child = inner ( proj - idxscan (x = y))

	/* Non-root - call single child */
	vector<duckdb::CypherPhysicalOperator*>* result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));
	
	vector<duckdb::LogicalType> types;

	CPhysicalInnerIndexNLJoin* proj_op = (CPhysicalInnerIndexNLJoin*) plan_expr->Pop();
	CColRefArray* output_cols = plan_expr->DeriveOutputColumns()->Pdrgpcr(mp);
	CExpression *pexprOuter = (*plan_expr)[0];
	CColRefArray* outer_cols = pexprOuter->DeriveOutputColumns()->Pdrgpcr(mp);	// size 1
	CExpression *pexprInner = (*plan_expr)[1];
	CColRefArray* inner_cols = pexprInner->DeriveOutputColumns()->Pdrgpcr(mp);	// size 2 230303 (_tid, _sid) => we map to (_eid, _sid), which is ok for now....
	// 230303 TODO  -> inner_cols last value 똥값. 0x0로찍힘.

	unordered_map<ULONG, uint64_t> id_map;
	std::vector<uint32_t> outer_col_map;
	std::vector<uint32_t> inner_col_map;

	for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
		CColRef *col = (*output_cols)[col_idx];
		ULONG col_id = col->Id();
		id_map.insert(std::make_pair(col_id, col_idx));
		// fprintf(stdout, "AdjIdxJoin Insert %d %d\n", col_id, col_idx);

		CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId() );
		OID type_oid = type_mdid->Oid();
		types.push_back(pConvertTypeOidToLogicalType(type_oid));
	}

	// 230303 srcidxcol = 0 (_id)
	uint64_t sid_col_idx = 0;

	// TODO load only adjidx id => hardcode load sid only (we use backward idx)

	uint64_t adjidx_obj_id;	// 230303
	std::vector<uint32_t> sccmp_colids;
	CExpression* inner_root = pexprInner;
	while(true) {
		if(inner_root->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan) {
			// IdxScan
			CPhysicalIndexScan* idxscan_op = (CPhysicalIndexScan*)inner_root->Pop();
			CMDIdGPDB* index_mdid = CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
			gpos::ULONG oid = index_mdid->Oid();
			adjidx_obj_id = (uint64_t) oid;

			// Get JoinColumnID
			for (uint32_t i = 0; i < inner_root->operator[](0)->Arity(); i++) {
				CScalarIdent *sc_ident = (CScalarIdent *)(inner_root->operator[](0)->operator[](i)->Pop());
				sccmp_colids.push_back(sc_ident->Pcr()->Id());
			}
		}
		// reached to the bottom
		if( inner_root->Arity() == 0 ) {
			break;
		} else {
			inner_root = inner_root->operator[](0);	// pass first child in linear plan
		}
	}
	D_ASSERT(inner_root != pexprInner);

	bool sid_col_idx_found = false;
	// Construct mapping info
	for(ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++){
		CColRef* col = outer_cols->operator[](col_idx);
		ULONG col_id = col->Id();
		auto id_idx = id_map.at(col_id); // std::out_of_range exception if col_id does not exist in id_map
		outer_col_map.push_back(id_idx);
		auto it = std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
		if (it != sccmp_colids.end()) {
			D_ASSERT(!sid_col_idx_found);
			sid_col_idx = col_idx;
			sid_col_idx_found = true;
		}
	}
	D_ASSERT(sid_col_idx_found);
	// fprintf(stdout, "AdjIdxJoin found = %s, sid_col_idx = %ld\n", sid_col_idx_found ? "true": "false", sid_col_idx);
	for(ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
		if (col_idx == 0 || col_idx >= 3) continue; // skip _id and other properties for edge table.. TODO temporary
		CColRef* col = inner_cols->operator[](col_idx);
		ULONG col_id = col->Id();
		auto id_idx = id_map.at(col_id); // std::out_of_range exception if col_id does not exist in id_map
		inner_col_map.push_back(id_idx);
	}

	/* Generate operator and push */
	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(types);

	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalAdjIdxJoin(tmp_schema, adjidx_obj_id, duckdb::JoinType::INNER, sid_col_idx, true,
									   outer_col_map, inner_col_map);	// 230303 schema conforms by coincidence

	result->push_back(op);

	output_cols->Release();
	outer_cols->Release();
	inner_cols->Release();

	return result;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopPhysicalInnerIndexNLJoinToIdSeek(CExpression* plan_expr) {

	CMemoryPool* mp = this->memory_pool;

	/* Non-root - call single child */
	vector<duckdb::CypherPhysicalOperator*>* result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

	vector<duckdb::LogicalType> types;

	CPhysicalInnerIndexNLJoin* proj_op = (CPhysicalInnerIndexNLJoin*) plan_expr->Pop();
	CColRefArray* output_cols = plan_expr->DeriveOutputColumns()->Pdrgpcr(mp);
	CExpression *pexprOuter = (*plan_expr)[0];
	CColRefArray* outer_cols = pexprOuter->DeriveOutputColumns()->Pdrgpcr(mp);
	CExpression *pexprInner = (*plan_expr)[1];
	CColRefArray* inner_cols = pexprInner->DeriveOutputColumns()->Pdrgpcr(mp);

	unordered_map<ULONG, uint64_t> id_map;
	std::vector<uint32_t> outer_col_map;
	std::vector<uint32_t> inner_col_map;

	for (ULONG col_idx = 0; col_idx < output_cols->Size(); col_idx++) {
		CColRef *col = (*output_cols)[col_idx];
		ULONG col_id = col->Id();
		id_map.insert(std::make_pair(col_id, col_idx));
		// fprintf(stdout, "IdSeek Insert %d %d\n", col_id, col_idx);

		CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId() );
		OID type_oid = type_mdid->Oid();
		types.push_back(pConvertTypeOidToLogicalType(type_oid));
	}

	uint64_t idx_obj_id;	// 230303
	uint64_t sid_col_idx;
	CExpression *inner_root = pexprInner;

	vector<uint64_t> oids;
	vector<vector<uint64_t>> projection_mapping;
	vector<uint64_t> first_table_mapping;
	vector<uint32_t> sccmp_colids;
	vector<uint32_t> scident_colids;

	bool do_projection_on_idxscan = false;
	// temporary TODO check this logic is generally true
	// D_ASSERT(inner_root->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalComputeScalarColumnar);
	// if (inner_root->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalComputeScalarColumnar) {
	// 	D_ASSERT(inner_root->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan);
	// 	D_ASSERT(inner_root->operator[](1)->Pop()->Eopid() == COperator::EOperatorId::EopScalarProjectList);
	// 	if (inner_root->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan) {
	// 		CExpression *idxscan_expr = inner_root->operator[](0);
	// 		// IdxScan
	// 		CPhysicalIndexScan* idxscan_op = (CPhysicalIndexScan*)idxscan_expr->Pop();
	// 		CMDIdGPDB* index_mdid = CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
	// 		gpos::ULONG oid = index_mdid->Oid();
	// 		idx_obj_id = (uint64_t)oid;

	// 		// Get JoinColumnID
	// 		for (uint32_t i = 0; i < idxscan_expr->operator[](0)->Arity(); i++) {
	// 			CScalarIdent *sc_ident = (CScalarIdent *)(idxscan_expr->operator[](0)->operator[](i)->Pop());
	// 			sccmp_colids.push_back(sc_ident->Pcr()->Id());
	// 		}

	// 		// TODO 230303 there may be additional projection - we CURRENTLY do not consider projection
	// 		CColRefArray* output = idxscan_op->PdrgpcrOutput();
	// 		for( ULONG i = 0; i < output->Size(); i++) {
	// 			CColRef* colref = output->operator[](i);
	// 			OID table_obj_id = CMDIdGPDB::CastMdid(((CColRefTable*) colref)->GetMdidTable())->Oid();
	// 			if (i == 0) { oids.push_back((uint64_t)table_obj_id); }
	// 			auto table_col_idx = pGetColIdxFromTable(table_obj_id, colref);
	// 			first_table_mapping.push_back(table_col_idx);
	// 		}
	// 	}
	// 	if (inner_root->operator[](1)->Pop()->Eopid() == COperator::EOperatorId::EopScalarProjectList) {
	// 		CExpression *projlist_expr = inner_root->operator[](1);
	// 		for (uint32_t i = 0; i < projlist_expr->Arity(); i++) {
	// 			CExpression *expr = projlist_expr->operator[](i);
	// 			D_ASSERT(expr->Pop()->Eopid() == COperator::EOperatorId::EopScalarProjectElement);
	// 			D_ASSERT(expr->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent);
	// 			CScalarIdent *sc_ident = (CScalarIdent *)(expr->operator[](0)->Pop());
	// 			scident_colids.push_back(sc_ident->Pcr()->Id());
	// 		}
	// 	}
	// }
	while(true) {
		if (inner_root->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan) {
			// IdxScan
			CPhysicalIndexScan* idxscan_op = (CPhysicalIndexScan*)inner_root->Pop();
			CMDIdGPDB* index_mdid = CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
			gpos::ULONG oid = index_mdid->Oid();
			idx_obj_id = (uint64_t)oid;

			// Get JoinColumnID
			for (uint32_t i = 0; i < inner_root->operator[](0)->Arity(); i++) {
				CScalarIdent *sc_ident = (CScalarIdent *)(inner_root->operator[](0)->operator[](i)->Pop());
				sccmp_colids.push_back(sc_ident->Pcr()->Id());
			}

			// TODO 230303 there may be additional projection - we CURRENTLY do not consider projection
			CColRefArray* output = idxscan_op->PdrgpcrOutput();
			for( ULONG i = 0; i < output->Size(); i++) {
				CColRef* colref = output->operator[](i);
				OID table_obj_id = CMDIdGPDB::CastMdid(((CColRefTable*) colref)->GetMdidTable())->Oid();
				if (i == 0) { oids.push_back((uint64_t)table_obj_id); }
				auto table_col_idx = pGetColIdxFromTable(table_obj_id, colref);
				first_table_mapping.push_back(table_col_idx);
			}
		} else if (inner_root->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalComputeScalarColumnar) {

		}
		// reached to the bottom
		if( inner_root->Arity() == 0 ) {
			break;
		} else {
			inner_root = inner_root->operator[](0);	// pass first child in linear plan
		}
	}

	projection_mapping.push_back(first_table_mapping);
	D_ASSERT(inner_root != pexprInner);

	D_ASSERT(oids.size() == 1);
	D_ASSERT(projection_mapping.size() == 1);

	bool sid_col_idx_found = false;
	// Construct mapping info
	for(ULONG col_idx = 0; col_idx < outer_cols->Size(); col_idx++){
		CColRef* col = outer_cols->operator[](col_idx);
		ULONG col_id = col->Id();
		auto id_idx = id_map.at(col_id); // std::out_of_range exception if col_id does not exist in id_map
		outer_col_map.push_back(id_idx);
		auto it = std::find(sccmp_colids.begin(), sccmp_colids.end(), col_id);
		if (it != sccmp_colids.end()) {
			D_ASSERT(!sid_col_idx_found);
			const WCHAR *w_str_buffer = L"_tid";
			CWStringConst colid_name(w_str_buffer);
			if (col->Name().Pstr()->Equals(&colid_name)) { // temporary..
				// sid_col_idx = col_idx + 1;
				sid_col_idx = col_idx;
			} else {
				sid_col_idx = col_idx;
			}
			sid_col_idx_found = true;
		}
	}
	D_ASSERT(sid_col_idx_found);
	// fprintf(stdout, "IdSeek found = %s, sid_col_idx = %ld\n", sid_col_idx_found ? "true": "false", sid_col_idx);
	
	for(ULONG col_idx = 0; col_idx < inner_cols->Size(); col_idx++) {
		CColRef* col = inner_cols->operator[](col_idx);
		ULONG col_id = col->Id();
		auto id_idx = id_map.at(col_id); // std::out_of_range exception if col_id does not exist in id_map
		inner_col_map.push_back(id_idx);
		// fprintf(stdout, "inner_col_map push %d %ld\n", col_id, id_idx);
	}

	/* Generate operator and push */
	duckdb::CypherSchema tmp_schema;
	tmp_schema.setStoredTypes(types);
	duckdb::CypherPhysicalOperator* op =
		new duckdb::PhysicalIdSeek(tmp_schema, sid_col_idx, oids, projection_mapping, outer_col_map, inner_col_map);	// 230303 schema conforms by coincidence

	result->push_back(op);

	output_cols->Release();
	outer_cols->Release();
	inner_cols->Release();

	return result;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopLimit(CExpression* plan_expr) {

	CMemoryPool* mp = this->memory_pool;

	/* Non-root - call single child */
	vector<duckdb::CypherPhysicalOperator*> *result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

	// TODO currently Limit do not anything
	return result;
}

vector<duckdb::CypherPhysicalOperator*>* Planner::pTransformEopSort(CExpression* plan_expr) {

	CMemoryPool* mp = this->memory_pool;

	/* Non-root - call single child */
	vector<duckdb::CypherPhysicalOperator *> *result = pTraverseTransformPhysicalPlan(plan_expr->PdrgPexpr()->operator[](0));

	CPhysicalSort *proj_op = (CPhysicalSort*) plan_expr->Pop();

	const COrderSpec *pos = proj_op->Pos();

	vector<duckdb::BoundOrderByNode> orders;
	for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++) {
		const CColRef *col = pos->Pcr(ul);
		ULONG col_id= col->Id();
		CMDIdGPDB* type_mdid = CMDIdGPDB::CastMdid(col->RetrieveType()->MDId() );
		OID type_oid = type_mdid->Oid();
		
		INT attrnum = 1;
		unique_ptr<duckdb::Expression> order_expr = make_unique<duckdb::BoundReferenceExpression>(pConvertTypeOidToLogicalType(type_oid), attrnum); // TODO attrnum
		duckdb::BoundOrderByNode order(duckdb::OrderType::ASCENDING, translateNullType(pos->Ent(ul)), move(order_expr)); // TODO how to determine ordertype??
		orders.push_back(move(order));
	}

	duckdb::CypherSchema tmp_schema;
	duckdb::CypherPhysicalOperator *last_op = result->back();
	tmp_schema.setStoredTypes(last_op->GetTypes());
	duckdb::CypherPhysicalOperator *op =
		new duckdb::PhysicalTopNSort(tmp_schema, move(orders), 1000, 0); // TODO we have topn sort only..
	result->push_back(op);

	// break pipeline
	auto pipeline = new duckdb::CypherPipeline(*result);
	pipelines.push_back(pipeline);

	auto new_result = new vector<duckdb::CypherPhysicalOperator*>();
	new_result->push_back(op);

	return new_result;
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


bool Planner::pIsIndexJoinOnPhysicalID(CExpression* plan_expr) {
	// if id seek then true, else adjidxjoin

	D_ASSERT( plan_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalInnerIndexNLJoin );

	CPhysicalInnerIndexNLJoin* proj_op = (CPhysicalInnerIndexNLJoin*) plan_expr->Pop();
	CExpression *pexprInner = (*plan_expr)[1];

	CExpression* inner_root = pexprInner;
	uint64_t idx_obj_id;
	while(true) {
		if(inner_root->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalIndexScan) {
			// IdxScan
			CPhysicalIndexScan* idxscan_op = (CPhysicalIndexScan*)inner_root->Pop();
			CMDIdGPDB* index_mdid = CMDIdGPDB::CastMdid(idxscan_op->Pindexdesc()->MDId());
			gpos::ULONG oid = index_mdid->Oid();
			idx_obj_id = (uint64_t) oid;
		}
		// reached to the bottom
		if( inner_root->Arity() == 0 ) {
			break;
		} else {
			inner_root = inner_root->operator[](0);	// pass first child in linear plan
		}
	}
	D_ASSERT(inner_root != pexprInner);

	// search catalog
	duckdb::Catalog &cat_instance = context->db->GetCatalog();
	duckdb::IndexCatalogEntry *index_cat = 
		(duckdb::IndexCatalogEntry *)cat_instance.GetEntry(*context, DEFAULT_SCHEMA, idx_obj_id);
	if( index_cat->GetIndexType() == duckdb::IndexType::PHYSICAL_ID ) {
		return true;
 	} else {
		return false;
	}
}

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
		COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
		COperator::EOperatorId::EopPhysicalTableScan,
	});
	auto p2 = vector<COperator::EOperatorId>({
		COperator::EOperatorId::EopPhysicalSerialUnionAll,
		COperator::EOperatorId::EopPhysicalComputeScalarColumnar,
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

uint64_t Planner::pGetColIdxFromTable(OID table_oid, const CColRef *target_col) {

	CMemoryPool *mp = this->memory_pool;

	CColRefTable* colref_table = (CColRefTable*) target_col;
	INT attr_no = colref_table->AttrNum();
	if( attr_no == (INT)-1 ) { 
		return 0;
	} else{
		return (uint64_t)attr_no;
	}
}

bool Planner::pIsColumnarProjectionSimpleProject(CExpression* proj_expr) {

	// check if all projetion expressions are CscalarIdent
	D_ASSERT(proj_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalComputeScalarColumnar );
	D_ASSERT(proj_expr->Arity() == 2);	// 0 prev 1 projlist

	bool result = true;
	ULONG proj_size = proj_expr->operator[](1)->Arity();
	for(ULONG proj_idx = 0; proj_idx < proj_size; proj_idx++) {
		result = result &&
					// list -> idx'th element -> ident
					(proj_expr->operator[](1)->operator[](proj_idx)->operator[](0)->Pop()->Eopid() == COperator::EOperatorId::EopScalarIdent);
	}
	return result;
}

CColRefArray* Planner::pGetUnderlyingColRefsOfColumnarProjection(CColRefArray* output_colrefs, CExpression* proj_expr) {

	// the input output_colrefs should only contain output columns in the output columns of proj_expr
	D_ASSERT(proj_expr->Pop()->Eopid() == COperator::EOperatorId::EopPhysicalComputeScalarColumnar );
	D_ASSERT(proj_expr->Arity() == 2);	// 0 prev 1 projlist

	CMemoryPool* mp = this->memory_pool;
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CColRefArray* result = GPOS_NEW(mp) CColRefArray(mp);
	result->AddRef();

	ULONG proj_size = proj_expr->operator[](1)->Arity();
	for(ULONG idx = 0; idx < output_colrefs->Size(); idx++) {
		int found_idx = -1;
		// find colref
		for(ULONG proj_idx = 0; proj_idx < proj_size; proj_idx++) {
			// list -> idx'th element -> ident
			CScalarProjectElement* op = (CScalarProjectElement*) proj_expr->operator[](1)->operator[](proj_idx)->Pop();
			if( op->Pcr()->Id() == output_colrefs->operator[](idx)->Id() ) {
				// found
				found_idx = proj_idx;
				CScalarIdent* matching_ident = (CScalarIdent*) proj_expr->operator[](1)->operator[](proj_idx)->operator[](0)->Pop();
				result->Append(col_factory->LookupColRef(matching_ident->Pcr()->Id()));
				
			}
		}
		D_ASSERT(found_idx >= 0);
	}
	D_ASSERT( output_colrefs->Size() == result->Size() );
	return result;
}

duckdb::OrderByNullType Planner::translateNullType(COrderSpec::ENullTreatment ent) {
	switch (ent) {
	case COrderSpec::ENullTreatment::EntAuto:
		return duckdb::OrderByNullType::ORDER_DEFAULT;
	case COrderSpec::ENullTreatment::EntFirst:
		return duckdb::OrderByNullType::NULLS_FIRST;
	case COrderSpec::ENullTreatment::EntLast:
		return duckdb::OrderByNullType::NULLS_LAST;
	case COrderSpec::ENullTreatment::EntSentinel:
		D_ASSERT(false);
	}
}


}