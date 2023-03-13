#include "planner.hpp"
#include "mdprovider/MDProviderTBGPP.h"

#include <string>
#include <limits>

// Refer to https://s3.amazonaws.com/artifacts.opencypher.org/railroad/Cypher.html

namespace s62 {

CExpression* Planner::lGetLogicalPlan() {

	GPOS_ASSERT( this->bound_statement != nullptr );
	auto& regularQuery = *((BoundRegularQuery*)(this->bound_statement));

	// TODO need union between single queries
	GPOS_ASSERT( regularQuery.getNumSingleQueries() == 1);
	vector<CExpression*> childLogicalPlans(regularQuery.getNumSingleQueries());
	for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
		childLogicalPlans[i] = lPlanSingleQuery(*regularQuery.getSingleQuery(i));
	}
	return childLogicalPlans[0];
}

CExpression* Planner::lPlanSingleQuery(const NormalizedSingleQuery& singleQuery) {

	// TODO refer kuzu properties to scan
		// populate properties to scan
    	// propertiesToScan.clear();

	CExpression* plan;
	GPOS_ASSERT(singleQuery.getNumQueryParts() == 1);
	for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
		// TODO plan object may need to be pushed up once again. thus the function signature need to be changed as well.
        auto plan_obj = lPlanQueryPart(*singleQuery.getQueryPart(i), nullptr);
		plan = plan_obj->getPlanExpr();
    }
	return plan;
}

LogicalPlan* Planner::lPlanQueryPart(
	const NormalizedQueryPart& queryPart, LogicalPlan* prev_plan) {

	LogicalPlan* cur_plan = prev_plan;
	for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
        cur_plan = lPlanReadingClause(queryPart.getReadingClause(i), cur_plan);
    }
	GPOS_ASSERT( queryPart.getNumUpdatingClause() == 0);
	// plan projectionBody after reading clauses
	if (queryPart.hasProjectionBody()) {
		
		// WITH ...
		cur_plan = lPlanProjectionBody(cur_plan, queryPart.getProjectionBody());
		
		if (queryPart.hasProjectionBodyPredicate()) {
			// WITH ... WHERE ...
				// Need to check semantics on whether filter or project should be planned first on behalf of other.
				// maybe filter first?
			GPOS_ASSERT(false); // filter not yet implemented.
			// appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
        }
    }
	return cur_plan;
}

LogicalPlan* Planner::lPlanProjectionBody(LogicalPlan* plan, BoundProjectionBody* proj_body) {

	/* Aggregate - generate LogicalGbAgg series */
	if(proj_body->hasAggregationExpressions()) {
		GPOS_ASSERT(false);
		// TODO plan is manipulated
		// maybe need to split function without agg and with agg.
	}

	/* Scalar projection - using CLogicalProject */
		// find all projection expressions that requires new columns
		// generate logicalproiection and record the mappings

	// maintain new mappings

	/* Simple projection - switch orders between columns; use lPlanProjectionOnColIds */
	vector<uint64_t> simple_proj_colids;
	const auto& proj_exprs = proj_body->getProjectionExpressions();
	for( auto& proj_expr: proj_exprs) {
		auto& proj_expr_type = proj_expr.get()->expressionType;
		if(proj_expr_type == kuzu::common::ExpressionType::PROPERTY) {
			// first depth projection = simple projection
			PropertyExpression* prop_expr = (PropertyExpression*)(proj_expr.get());
			string k1 = prop_expr->getVariableName();
			string k2 = prop_expr->getPropertyName();
			uint64_t idx = plan->getSchema()->getIdxOfKey(k1, k2);
			simple_proj_colids.push_back(idx);
		} else {
			// currently do not allow other cases
			GPOS_ASSERT(false);
		}
	}
	plan = lPlanProjectionOnColIds(plan, simple_proj_colids);
	
	/* OrderBy */
	if(proj_body->hasOrderByExpressions()) {
		// orderByExpressions
		// isAscOrders
		GPOS_ASSERT(false);
	}
	
	/* Skip limit */
	if( proj_body->hasSkipOrLimit() ) {
		// CLogicalLimit
		GPOS_ASSERT(false);
	}

	/* Distinct */
	if(proj_body->getIsDistinct()) {
		GPOS_ASSERT(false);
	}

	GPOS_ASSERT(plan != nullptr);
	return plan;
}

LogicalPlan* Planner::lPlanReadingClause(
	BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan) {

	LogicalPlan* plan;

	auto readingClauseType = boundReadingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        plan = lPlanMatchClause(boundReadingClause, prev_plan);
    } break;
    case ClauseType::UNWIND: {
        plan = lPlanUnwindClause(boundReadingClause, prev_plan);
    } break;
    default:
        GPOS_ASSERT(false);
    }

	return plan;
}

LogicalPlan* Planner::lPlanMatchClause(
	BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan) {

	auto boundMatchClause = (BoundMatchClause*)boundReadingClause;
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    expression_vector predicates = boundMatchClause->hasWhereExpression() ?	
                          boundMatchClause->getWhereExpression()->splitOnAND() :	// CNF form
                          expression_vector{};

	LogicalPlan* plan;
    if (boundMatchClause->getIsOptional()) {
        GPOS_ASSERT(false);
		// TODO optionalmatch
    } else {
		plan = lPlanRegularMatch(*queryGraphCollection, prev_plan);
    }

	// TODO append edge isomorphism
		// TODO need to know about the label info...
		// for each qg in qgc, 
			// list all edges
				// nested for loops


	// plan selection on match clause
	if( predicates.size() > 0 ) {
		plan = lPlanSelection(std::move(predicates), plan);
	}

	return plan;
}

LogicalPlan* Planner::lPlanUnwindClause(
	BoundReadingClause* boundReadingClause, LogicalPlan* prev_plan) {
	GPOS_ASSERT(false);
	return nullptr;
}

LogicalPlan* Planner::lPlanRegularMatch(const QueryGraphCollection& qgc, LogicalPlan* prev_plan) {

	LogicalPlan* plan = nullptr;

	string ID_COLNAME = "_id";
	string SID_COLNAME = "_sid";
	string TID_COLNAME = "_tid";


	LogicalPlan* qg_plan = prev_plan;

	GPOS_ASSERT( qgc.getNumQueryGraphs() > 0 );
	for(int idx=0; idx < qgc.getNumQueryGraphs(); idx++){
		QueryGraph* qg = qgc.getQueryGraph(idx);

		for(int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
			RelExpression* qedge = qg->getQueryRel(edge_idx).get();
			NodeExpression* lhs = qedge->getSrcNode().get();
			NodeExpression* rhs = qedge->getDstNode().get();
			string edge_name = qedge->getUniqueName();
			string lhs_name = qedge->getSrcNode()->getUniqueName();
			string rhs_name = qedge->getDstNode()->getUniqueName();

			bool is_lhs_bound = false;
			bool is_rhs_bound = false;
			if(qg_plan != nullptr) {
				is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name) ? true : false;
				is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name) ? true : false;
			}

			LogicalPlan* hop_plan;

			LogicalPlan* lhs_plan;
			// A join R
			if( !is_lhs_bound ) {
				lhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)lhs, true);
			} else {
				// lhs bound
				lhs_plan = qg_plan;
			}
			GPOS_ASSERT(lhs_plan != nullptr);
			// Scan R
			LogicalPlan* edge_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)qedge, false);
			
			// TODO need to make this as a function
			auto join_expr = lExprLogicalJoinOnId(lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
				lhs_plan->getSchema()->getIdxOfKey(lhs_name, ID_COLNAME),
				edge_plan->getSchema()->getIdxOfKey(edge_name, SID_COLNAME)
			);
			lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
			lhs_plan->addBinaryParentOp(join_expr, edge_plan);
			
			// R join B
			if( is_lhs_bound && is_rhs_bound ) {
				// no join necessary - add filter predicate
				GPOS_ASSERT(false);
				hop_plan = qg_plan; // TODO fixme
			} else {
				LogicalPlan* rhs_plan;
				// join necessary
				if(!is_rhs_bound) {
					rhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)rhs, true);
				} else {
					// lhs unbound and rhs bound
					rhs_plan = qg_plan;
				}
				// (AR) join B
				auto join_expr = lExprLogicalJoinOnId(lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
					lhs_plan->getSchema()->getIdxOfKey(edge_name, TID_COLNAME),
					rhs_plan->getSchema()->getIdxOfKey(rhs_name, ID_COLNAME)
				);
				lhs_plan->getSchema()->appendSchema(rhs_plan->getSchema());
				lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
				hop_plan = lhs_plan;
			}
			GPOS_ASSERT(hop_plan != nullptr);
			// When lhs, rhs is unbound, qg_plan is not merged with the hop_plan. Thus cartprod.
			if ( (qg_plan != nullptr) && (!is_lhs_bound) && (!is_rhs_bound)) {
				auto cart_expr = lExprLogicalCartProd(qg_plan->getPlanExpr(), hop_plan->getPlanExpr());
				qg_plan->getSchema()->appendSchema(hop_plan->getSchema());
				qg_plan->addBinaryParentOp(cart_expr, hop_plan);
			} else {
				qg_plan = hop_plan;
			}
			GPOS_ASSERT(qg_plan != nullptr);
		}
		// if no edge, this is single node scan case
		if(qg->getQueryNodes().size() == 1) {
			LogicalPlan* nodescan_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)qg->getQueryNodes()[0].get(), true);
			if(qg_plan == nullptr) {
				qg_plan = nodescan_plan;
			} else {
				// cartprod
				auto cart_expr = lExprLogicalCartProd(qg_plan->getPlanExpr(), nodescan_plan->getPlanExpr());
				qg_plan->getSchema()->appendSchema(nodescan_plan->getSchema());
				qg_plan->addBinaryParentOp(cart_expr, nodescan_plan);
			}
		}
		GPOS_ASSERT(qg_plan != nullptr);
	}

	return qg_plan;

}

LogicalPlan* Planner::lPlanSelection(const expression_vector& predicates, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;
	// the predicates are in CNF form
	
	CExpressionArray* cnf_exprs = GPOS_NEW(mp) CExpressionArray(mp);
	for(auto& pred: predicates) {
		cnf_exprs->Append(lExprScalarExpression(pred.get(), prev_plan));
	}

	// orca supports N-ary AND
	CExpression* selection_expr = CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopAnd, cnf_exprs);
	prev_plan->addUnaryParentOp(selection_expr);

	// schema is never changed
	return prev_plan;
}

LogicalPlan* Planner::lPlanProjectionOnColIds(LogicalPlan* plan, vector<uint64_t>& col_ids) {

	CMemoryPool* mp = this->memory_pool;

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	
// TODO fixme
	CColRefArray* plan_cols = plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp);
	CColRefArray* proj_cols = GPOS_NEW(mp) CColRefArray(mp);

	LogicalSchema schema;
	CExpression* scalar_proj_elem;
	for(auto& col_id: col_ids){
		CColRef *colref = plan_cols->operator[](col_id);
		CColRef *new_colref = col_factory->PcrCreate(colref->RetrieveType(), colref->TypeModifier(), colref->Name());	// generate new reference having same name
		CExpression* ident_expr = GPOS_NEW(mp)
				CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
		scalar_proj_elem = GPOS_NEW(mp) CExpression(
			mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), ident_expr);
		proj_array->Append(scalar_proj_elem);	
	}

	// Our columnar projection
	CExpression *pexprPrjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
	CExpression *proj_expr =  GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp), plan->getPlanExpr(), pexprPrjList);

	plan->addUnaryParentOp(proj_expr);
	// TODO add schema here!!
	return plan;
}

LogicalPlan* Planner::lPlanNodeOrRelExpr(NodeOrRelExpression* node_expr, bool is_node) {

	auto table_oids = node_expr->getTableIDs();
	GPOS_ASSERT(table_oids.size() >= 1);

	map<uint64_t, map<uint64_t, uint64_t>> schema_proj_mapping;	// maps from new_col_id->old_col_id
	for( auto& t_oid: table_oids) {
		schema_proj_mapping.insert({t_oid, map<uint64_t, uint64_t>()});
	}
	GPOS_ASSERT(schema_proj_mapping.size() == table_oids.size());

	// these properties include system columns (e.g. _id)
	auto& prop_exprs = node_expr->getPropertyExpressions();
	for( int colidx=0; colidx < prop_exprs.size(); colidx++) {
		auto& _prop_expr = prop_exprs[colidx];
		PropertyExpression* expr = static_cast<PropertyExpression*>(_prop_expr.get());
		for( auto& t_oid: table_oids) {
			if( expr->hasPropertyID(t_oid) ) {
				// table has property
				schema_proj_mapping.find(t_oid)->
					second.insert({(uint64_t)colidx, (uint64_t)(expr->getPropertyID(t_oid))});
			} else {
				// need to be projected as null column
				schema_proj_mapping.find(t_oid)->
					second.insert({(uint64_t)colidx, std::numeric_limits<uint64_t>::max()});
			}
		}
	}
	
	// get plan
	CExpression* plan_expr;
	auto node_name = node_expr->getUniqueName();
	auto node_name_print = node_expr->getRawName();

	// always do schema mapping even when single table
	plan_expr = lExprLogicalGetNodeOrEdge(node_name_print, table_oids, &schema_proj_mapping, true);

	// insert node schema
	LogicalSchema schema;
	for(int col_idx = 0; col_idx < prop_exprs.size(); col_idx++ ) {
		auto& _prop_expr = prop_exprs[col_idx];
		PropertyExpression* expr = static_cast<PropertyExpression*>(_prop_expr.get());
		string expr_name = expr->getRawName();
		if( is_node ) {
			schema.appendNodeProperty(node_name, expr_name);
		}  else {
			schema.appendEdgeProperty(node_name, expr_name);
		}
	}
	GPOS_ASSERT( schema.getNumPropertiesOfKey(node_name) == prop_exprs.size() );

	LogicalPlan* plan = new LogicalPlan(plan_expr, schema);
	GPOS_ASSERT( !plan->getSchema()->isEmpty() );
	return plan;
}


CExpression* Planner::lExprScalarExpression(Expression* expression, LogicalPlan* prev_plan) {

	auto expr_type = expression->expressionType;
	if( isExpressionComparison(expr_type) ) {
		return lExprScalarComparisonExpr(expression);
	} else if( PROPERTY == expr_type) {
		return lExprScalarPropertyExpr(expression, prev_plan);	// property access need to access previous plan
	} else if ( isExpressionLiteral(expr_type) ) {
		return lExprScalarLiteralExpr(expression);
	} else {
		D_ASSERT(false);
	}
}

CExpression* Planner::lExprScalarComparisonExpr(Expression* expression) {

	ScalarFunctionExpression* comp_expr = (ScalarFunctionExpression*) expression;
	D_ASSERT( comp_expr->getNumChildren() == 2);	// S62 not sure how kuzu generates comparison expression, now assume 2
	// lhs, rhs
	// CExpression* lhs_scalar_expr = lExprScalarExpression(children[0].get());
	// CExpression* rhs_scalar_expr = lExprScalarExpression(children[1].get());

	// access MDA and serach function mdid regarding oid type
		// todo write convert function from duckdb oid to target oid
	
	// return corresponding expression
}	

CExpression* Planner::lExprScalarPropertyExpr(Expression* expression, LogicalPlan* prev_plan) {

	// TODO generate scalarident
		// from 
	
}

CExpression* Planner::lExprScalarLiteralExpr(Expression* expression) {

	// TODO generate scalar const


}


/*
	Single tables may not 
*/
CExpression* Planner::lExprLogicalGetNodeOrEdge(string name, vector<uint64_t> relation_oids,
	map<uint64_t, map<uint64_t, uint64_t>>* schema_proj_mapping, bool insert_projection) {
	
	CMemoryPool* mp = this->memory_pool;

	CExpression* union_plan = nullptr;
	const bool do_schema_mapping = insert_projection;
	GPOS_ASSERT(relation_oids.size() > 0);

	// generate type infos to the projected schema
	uint64_t num_proj_cols;								// size of the union schema
	vector<pair<gpmd::IMDId*, gpos::INT>> union_schema_types;	// mdid type and type modifier for both types
	num_proj_cols =
		(*schema_proj_mapping)[relation_oids[0]].size() > 0
			? (*schema_proj_mapping)[relation_oids[0]].rbegin()->first + 1
			: 0;
	// iterate schema_projection mapping
	for( int col_idx = 0; col_idx < num_proj_cols; col_idx++) {
		// foreach mappings
		uint64_t valid_oid;
		uint64_t valid_cid = std::numeric_limits<uint64_t>::max();

		for( auto& oid: relation_oids) {
			uint64_t idx_to_try = (*schema_proj_mapping)[oid].find(col_idx)->second;
			if (idx_to_try != std::numeric_limits<uint64_t>::max() ) {
				valid_oid = oid;
				valid_cid = idx_to_try;
			}
		}			
		GPOS_ASSERT(valid_cid != std::numeric_limits<uint64_t>::max());
		// extract info and maintain vector of column type infos
		gpmd::IMDId* col_type_imdid = lGetRelMd(valid_oid)->GetMdCol(valid_cid)->MdidType();
		gpos::INT col_type_modifier = lGetRelMd(valid_oid)->GetMdCol(valid_cid)->TypeModifier();
		union_schema_types.push_back( make_pair(col_type_imdid, col_type_modifier) );
	}

	CColRefArray* idx0_output_array;
	for( int idx = 0; idx < relation_oids.size(); idx++) {
		auto& oid = relation_oids[idx];

		CExpression * expr;
		const gpos::ULONG num_cols = lGetRelMd(oid)->ColumnCount();

		GPOS_ASSERT(num_cols != 0);
		expr = lExprLogicalGet(oid, name);

		// conform schema if necessary
		CColRefArray* output_array;
		vector<uint64_t> project_cols;
		if(do_schema_mapping) {
			auto& mapping = (*schema_proj_mapping)[oid];
			assert(num_proj_cols == mapping.size());
			for(int proj_col_idx = 0; proj_col_idx < num_proj_cols; proj_col_idx++) {
				project_cols.push_back(mapping.find(proj_col_idx)->second);
			}
			GPOS_ASSERT(project_cols.size() > 0);
			auto proj_result = lExprScalarAddSchemaConformProject(expr, project_cols, &union_schema_types);
			expr = proj_result.first;
			output_array = proj_result.second;
		} else {
			output_array = expr->DeriveOutputColumns()->Pdrgpcr(mp);
		}

		/* Single table might not have the identical column mapping with original table. Thus projection is required */
		if(idx == 0) {
			// REL
			union_plan = expr;
			idx0_output_array = output_array;
		} else if (idx == 1) {
			// REL + REL
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, idx0_output_array, expr, output_array);
		} else {
			// UNION + REL
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, union_plan->DeriveOutputColumns()->Pdrgpcr(mp), expr, output_array);
		}
	}

	return union_plan;
}

CExpression * Planner::lExprLogicalGet(uint64_t obj_id, string rel_name, string alias) {
	CMemoryPool* mp = this->memory_pool;

	if(alias == "") { alias = rel_name; }

	const CWStringConst* table_name_cwstring = lGetMDAccessor()->RetrieveRel(lGenRelMdid(obj_id))->Mdname().GetMDName();
	wstring table_name_ws(table_name_cwstring->GetBuffer());
	string table_name(table_name_ws.begin(), table_name_ws.end());
	std::string print_name = "(" + rel_name + ") " + table_name;
	CWStringConst strName(std::wstring(print_name.begin(), print_name.end()).c_str());
	CTableDescriptor *ptabdesc =
		lCreateTableDesc(mp,
					   lGenRelMdid(obj_id),	// 6.objid.0.0
					   CName(&strName));	// debug purpose table string

	// manage original table columns
	if( !table_col_mapping.count(obj_id) ) {
		table_col_mapping[obj_id] = std::vector<CColRef*>();
	}


	CWStringConst strAlias(std::wstring(alias.begin(), alias.end()).c_str());

	CLogicalGet *pop = GPOS_NEW(mp) CLogicalGet(
		mp, GPOS_NEW(mp) CName(mp, CName(&strAlias)), ptabdesc);

	CExpression *scan_expr = GPOS_NEW(mp) CExpression(mp, pop);
	CColRefArray *arr = pop->PdrgpcrOutput();
	for (ULONG ul = 0; ul < arr->Size(); ul++) {
		CColRef *ref = (*arr)[ul];
		table_col_mapping[obj_id].push_back(ref);
		ref->MarkAsUnknown();
	}
	return scan_expr;
}

CExpression * Planner::lExprLogicalUnionAllWithMapping(CExpression* lhs, CColRefArray* lhs_mapping, CExpression* rhs, CColRefArray* rhs_mapping) {

	GPOS_ASSERT( rhs_mapping != nullptr ); // must be binary

	CMemoryPool* mp = this->memory_pool;

	CColRefArray *pdrgpcrOutput = GPOS_NEW(mp) CColRefArray(mp);
	CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);
	
	// output columns of the union
	pdrgpcrOutput->AppendArray(lhs_mapping);
	pdrgpcrOutput->AddRef();
	pdrgdrgpcrInput->Append(lhs_mapping);
	pdrgdrgpcrInput->Append(rhs_mapping);
	
	// Binary Union ALL
	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgdrgpcrInput),
		lhs, rhs);

}

CExpression * Planner::lExprLogicalUnionAll(CExpression* lhs, CExpression* rhs) {

	CMemoryPool* mp = this->memory_pool;
	return lExprLogicalUnionAllWithMapping(
				lhs, lhs->DeriveOutputColumns()->Pdrgpcr(mp),
				rhs, rhs->DeriveOutputColumns()->Pdrgpcr(mp));

}

/*
 * CExpression* returns result of projected schema.
*/
std::pair<CExpression*, CColRefArray*> Planner::lExprScalarAddSchemaConformProject(CExpression* relation,
	vector<uint64_t> col_ids_to_project, vector<pair<gpmd::IMDId*, gpos::INT>>* target_schema_types
) {
	// col_ids_to_project may include std::numeric_limits<uint64_t>::max(),
	// which indicates null projection
	CMemoryPool* mp = this->memory_pool;

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray* output_col_array = GPOS_NEW(mp) CColRefArray(mp);

	uint64_t target_col_id = 0;
	for(auto col_id: col_ids_to_project) {
		CExpression* scalar_proj_elem;
		if( col_id == std::numeric_limits<uint64_t>::max()) {
			GPOS_ASSERT(target_schema_types != nullptr);
			// project null column
			auto& type_info = (*target_schema_types)[target_col_id];
			CExpression* null_expr =
				CUtils::PexprScalarConstNull(mp, lGetMDAccessor()->RetrieveType(type_info.first) , type_info.second);
			const CWStringConst col_name_str(GPOS_WSZ_LIT("const_null"));
			CName col_name(&col_name_str);
			CColRef *new_colref = col_factory->PcrCreate( lGetMDAccessor()->RetrieveType(type_info.first), type_info.second, col_name);
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), null_expr);
			
			proj_array->Append(scalar_proj_elem);
			output_col_array->Append(new_colref);
			
		} else {
			// project non-null column
			CColRef *colref = relation->DeriveOutputColumns()->Pdrgpcr(mp)->operator[](col_id);
			CColRef *new_colref = col_factory->PcrCreate(colref->RetrieveType(), colref->TypeModifier(), colref->Name());	// generate new reference having same name
				// TODO S62 disable
			CExpression* ident_expr = GPOS_NEW(mp)
					CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), ident_expr); // TODO S62 change to colref

			proj_array->Append(scalar_proj_elem);	
			output_col_array->Append(colref);
		}
		target_col_id++;
	}

	// Our columnar projection
	CExpression *pexprPrjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
	CExpression *proj_expr =  GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp), relation, pexprPrjList);

	return make_pair(proj_expr, output_col_array);

}

CExpression * Planner::lExprLogicalJoinOnId(CExpression* lhs, CExpression* rhs,
		uint64_t lhs_pos, uint64_t rhs_pos, bool project_out_lhs_key, bool project_out_rhs_key) {
		
	/*
		if V join R, set (project_out_lhs, project_out_rhs) as (0,1)
		if R join V, set (project_out_lhs, project_out_rhs) as (1,0)
	*/

	CMemoryPool* mp = this->memory_pool;

	CColRefArray* lcols = lhs->DeriveOutputColumns()->Pdrgpcr(mp);
	auto lhs_size = lcols->Size();
	CColRefArray* rcols = rhs->DeriveOutputColumns()->Pdrgpcr(mp);
	auto rhs_size = rcols->Size();

	CColRef *pcrLeft = lcols->operator[](lhs_pos);
	CColRef *pcrRight =  rcols->operator[](rhs_pos);

	lhs->AddRef();
	rhs->AddRef();

	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
	auto join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprEquality);

	// TODO need function for join projection
	// if( project_out_lhs_key || project_out_rhs_key ) {
	// 	uint64_t idx_to_project_out = (project_out_lhs_key == true) ? (lhs_pos) : (lhs_size + rhs_pos);
	// 	join_result = lExprScalarProjectionExceptColIds(join_result, vector<uint64_t>({idx_to_project_out}));
	// }

	return join_result;
}


CExpression * Planner::lExprLogicalCartProd(CExpression* lhs, CExpression* rhs) {
	/* Perform cartesian product = inner join on predicate true	*/

	CMemoryPool* mp = this->memory_pool; 

	CExpression *pexprTrue = CUtils::PexprScalarConstBool(mp, true, false);
	auto prod_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprTrue);
	GPOS_ASSERT( CUtils::FCrossJoin(prod_result) );
	return prod_result;
}

CTableDescriptor * Planner::lCreateTableDesc(CMemoryPool *mp, IMDId *mdid,
						   const CName &nameTable, gpos::BOOL fPartitioned) {

	CTableDescriptor *ptabdesc = lTabdescPlainWithColNameFormat(mp, mdid,
										  GPOS_WSZ_LIT("column_%04d"),				// format notused
										  nameTable, true /* is_nullable */);
										
	// if (fPartitioned) {
	// 	GPOS_ASSERT(false);
	// 	ptabdesc->AddPartitionColumn(0);
	// }
	// create a keyset containing the first column
	// CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp, num_cols);
	// pbs->ExchangeSet(0);
	// ptabdesc->FAddKeySet(pbs);

	return ptabdesc;
}

CTableDescriptor * Planner::lTabdescPlainWithColNameFormat(
		CMemoryPool *mp, IMDId *mdid, const WCHAR *wszColNameFormat,
		const CName &nameTable, gpos::BOOL is_nullable  // define nullable columns
	) {

	CWStringDynamic *str_name = GPOS_NEW(mp) CWStringDynamic(mp);
	CTableDescriptor *ptabdesc = GPOS_NEW(mp) CTableDescriptor(
		mp, mdid, nameTable,
		false,	// convert_hash_to_random
		IMDRelation::EreldistrMasterOnly, IMDRelation::ErelstorageHeap,
		0  // ulExecuteAsUser
	);
	
	auto num_cols = lGetMDAccessor()->RetrieveRel(mdid)->ColumnCount();
	for (ULONG i = 0; i < num_cols; i++) {
		const IMDColumn *md_col = lGetMDAccessor()->RetrieveRel(mdid)->GetMdCol(i);
		IMDId *type_id = md_col->MdidType();
		const IMDType* pmdtype = lGetMDAccessor()->RetrieveType(type_id);
		INT type_modifier = md_col->TypeModifier();
		CName colname(md_col->Mdname().GetMDName());
		INT attno = md_col->AttrNum();
		CColumnDescriptor *pcoldesc = GPOS_NEW(mp)
			CColumnDescriptor(mp, pmdtype, type_modifier,
							  colname, attno, is_nullable);
		ptabdesc->AddColumn(pcoldesc);
	}

	GPOS_DELETE(str_name);
	return ptabdesc;

}


} // s62