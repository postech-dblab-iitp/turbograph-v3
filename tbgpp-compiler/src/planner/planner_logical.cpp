#include "planner.hpp"
#include "mdprovider/MDProviderTBGPP.h"

#include <string>
#include <limits>

// Refer to https://s3.amazonaws.com/artifacts.opencypher.org/railroad/Cypher.html

namespace s62 {

LogicalPlan * Planner::lGetLogicalPlan() {

	GPOS_ASSERT( this->bound_statement != nullptr );
	auto& regularQuery = *((BoundRegularQuery*)(this->bound_statement));

	// TODO need union between single queries
	GPOS_ASSERT( regularQuery.getNumSingleQueries() == 1);
	vector<LogicalPlan*> childLogicalPlans(regularQuery.getNumSingleQueries());
	for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
		childLogicalPlans[i] = lPlanSingleQuery(*regularQuery.getSingleQuery(i));
	}
	return childLogicalPlans[0];
}

LogicalPlan * Planner::lPlanSingleQuery(const NormalizedSingleQuery& singleQuery) {

	// TODO refer kuzu properties to scan
		// populate properties to scan
    	// propertiesToScan.clear();

	LogicalPlan* cur_plan = nullptr;
	for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        cur_plan = lPlanQueryPart(*singleQuery.getQueryPart(i), cur_plan);
    }
	return cur_plan;
}

LogicalPlan* Planner::lPlanQueryPart(
	const NormalizedQueryPart& queryPart, LogicalPlan* prev_plan) {

	LogicalPlan* cur_plan = prev_plan;
	for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
		// There may be no reading clause, WITH ... WITH
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
			cur_plan = lPlanSelection(queryPart.getProjectionBodyPredicate().get()->splitOnAND(), cur_plan);
			// appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
        }
    }
	return cur_plan;
}

LogicalPlan* Planner::lPlanProjectionBody(LogicalPlan* plan, BoundProjectionBody* proj_body) {

	CMemoryPool* mp = this->memory_pool;

	// check if agregation necessary
	bool agg_required = false;
	for (auto& projectionExpression : proj_body->getProjectionExpressions()) {
        if (lTryGenerateScalarIdent(projectionExpression.get(), plan) == NULL	// new expression to be evaluated
			&& projectionExpression->hasAggregationExpression()					// has aggregation
			 ) {
            agg_required = true;
        }
    }


	/* Aggregate - generate LogicalGbAgg series */
	if(agg_required) {
		plan = lPlanGroupBy(proj_body->getProjectionExpressions(), plan);	// TODO what if agg + projection
		// TODO plan is manipulated

		// handle all non-agg projection + original columns first
			// orca will remove unnecessary columns so no worries! 

	} else {
		plan = lPlanProjection(proj_body->getProjectionExpressions(), plan);
	}

	/* OrderBy */
	if (proj_body->hasOrderByExpressions()) {
		// orderByExpressions
		// isAscOrders
		const expression_vector &orderby_expr = proj_body->getOrderByExpressions(); 
		const vector<bool> sort_orders = proj_body->getSortingOrders(); // if true asc
		plan = lPlanOrderBy(orderby_expr, sort_orders, plan);
	}

	/* Scalar projection - using CLogicalProject */
		// find all projection expressions that requires new columns
		// generate logicalproiection and record the mappings

	// maintain new mappings

	/* Distinct */
	if (proj_body->getIsDistinct()) {
		plan = lPlanDistinct(plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp), plan);
	}

	/* Skip limit */
	if (proj_body->hasSkipOrLimit()) {
		plan = lPlanSkipOrLimit(proj_body, plan);
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
	plan = lPlanRegularMatch(*queryGraphCollection, prev_plan, boundMatchClause->getIsOptional());

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

LogicalPlan *Planner::lPlanRegularMatch(const QueryGraphCollection& qgc, LogicalPlan* prev_plan, bool is_optional_match) {
	
	LogicalPlan *plan = nullptr;

	string ID_COLNAME = "_id";
	string SID_COLNAME = "_sid";
	string TID_COLNAME = "_tid";

	s62::LogicalSchema *prev_plan_schema;	// maintain snapshot of the prev. schema, which is used for optional match
	if (prev_plan == nullptr) {
		prev_plan_schema = new s62::LogicalSchema();
	} else {
		prev_plan_schema = prev_plan->getSchema();
	}
	
	LogicalPlan *qg_plan = prev_plan;

	GPOS_ASSERT(qgc.getNumQueryGraphs() > 0);
	for (int idx = 0; idx < qgc.getNumQueryGraphs(); idx++){
		QueryGraph *qg = qgc.getQueryGraph(idx);

		for (int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
			RelExpression *qedge = qg->getQueryRel(edge_idx).get();
			NodeExpression *lhs = qedge->getSrcNode().get();
			NodeExpression *rhs = qedge->getDstNode().get();
			string edge_name = qedge->getUniqueName();
			string lhs_name = qedge->getSrcNode()->getUniqueName();
			string rhs_name = qedge->getDstNode()->getUniqueName();

			bool is_lhs_bound = false;
			bool is_rhs_bound = false;
			if (qg_plan != nullptr) {
				is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name) ? true : false;
				is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name) ? true : false;
			}

			// case for variable length join
			bool is_pathjoin = qedge->getLowerBound() != 1 || qedge->getUpperBound() != 1;

			LogicalPlan* hop_plan;
			LogicalPlan* lhs_plan;
			// A join R
			if (!is_lhs_bound) {
				lhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)lhs, true);
			} else {
				// lhs bound
				lhs_plan = qg_plan;
			}
			GPOS_ASSERT(lhs_plan != nullptr);

			// Scan R
			LogicalPlan *edge_plan;
			CExpression *a_r_join_expr;
			auto ar_join_type = is_optional_match && prev_plan_schema->isNodeBound(lhs_name) ? // check optional match
					gpopt::COperator::EOperatorId::EopLogicalLeftOuterJoin :
					gpopt::COperator::EOperatorId::EopLogicalInnerJoin;
			if (!is_pathjoin) {
				edge_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)qedge, false);
				// A join R
				a_r_join_expr = lExprLogicalJoin(lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
					lhs_plan->getSchema()->getColRefOfKey(lhs_name, ID_COLNAME),
					edge_plan->getSchema()->getColRefOfKey(edge_name, SID_COLNAME),
					ar_join_type);
			} else {
				edge_plan = lPlanPathGet((RelExpression*)qedge);
				// A pathjoin R
				a_r_join_expr = lExprLogicalPathJoin(lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
					lhs_plan->getSchema()->getColRefOfKey(lhs_name, ID_COLNAME),
					edge_plan->getSchema()->getColRefOfKey(edge_name, SID_COLNAME),
					qedge->getLowerBound(), qedge->getUpperBound(),
					ar_join_type);
			}
			lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
			lhs_plan->addBinaryParentOp(a_r_join_expr, edge_plan);
			
			// R join B
			if (is_lhs_bound && is_rhs_bound) {
				// no join necessary - add filter predicate on edge.tid = rhs.id
				CMemoryPool* mp = this->memory_pool;
				hop_plan = lhs_plan;
				CExpression* selection_expr = CUtils::PexprLogicalSelect(mp, lhs_plan->getPlanExpr(),
					lExprScalarCmpEq( lExprScalarPropertyExpr(edge_name, TID_COLNAME, lhs_plan), lExprScalarPropertyExpr(rhs_name, ID_COLNAME, lhs_plan) )
				);
				hop_plan->addUnaryParentOp(selection_expr);
			} else {
				LogicalPlan* rhs_plan;
				// join necessary
				if (!is_rhs_bound) {
					rhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)rhs, true);
				} else {
					// lhs unbound and rhs bound
					rhs_plan = qg_plan;
				}
				// (AR) join B
				auto rb_join_type = is_optional_match && prev_plan_schema->isNodeBound(rhs_name) ? // check optional match
					gpopt::COperator::EOperatorId::EopLogicalLeftOuterJoin :
					gpopt::COperator::EOperatorId::EopLogicalInnerJoin;
					// gpopt::COperator::EOperatorId::EopLogicalRightOuterJoin :
				auto join_expr = lExprLogicalJoin(lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
					lhs_plan->getSchema()->getColRefOfKey(edge_name, TID_COLNAME),
					rhs_plan->getSchema()->getColRefOfKey(rhs_name, ID_COLNAME),
					rb_join_type);
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
		if (qg->getQueryNodes().size() == 1) {
			LogicalPlan *nodescan_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)qg->getQueryNodes()[0].get(), true);
			if (qg_plan == nullptr) {
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


LogicalPlan* Planner::lPlanRegularMatchFromSubquery(const QueryGraphCollection& qgc, LogicalPlan* outer_plan) {

	// no optional match
	LogicalPlan* plan = nullptr;
	GPOS_ASSERT(outer_plan != nullptr);

	CMemoryPool* mp = this->memory_pool;

	string ID_COLNAME = "_id";
	string SID_COLNAME = "_sid";
	string TID_COLNAME = "_tid";

	LogicalPlan* qg_plan = nullptr; // start from nowhere, global subquery plan
	GPOS_ASSERT( qgc.getNumQueryGraphs() > 0 );

	for (int idx = 0; idx < qgc.getNumQueryGraphs(); idx++) {
		QueryGraph* qg = qgc.getQueryGraph(idx);

		for (int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
			RelExpression* qedge = qg->getQueryRel(edge_idx).get();
			NodeExpression* lhs = qedge->getSrcNode().get();
			NodeExpression* rhs = qedge->getDstNode().get();
			string edge_name = qedge->getUniqueName();
			string lhs_name = qedge->getSrcNode()->getUniqueName();
			string rhs_name = qedge->getDstNode()->getUniqueName();
			bool is_pathjoin = qedge->getLowerBound() != 1 || qedge->getUpperBound() != 1;

			bool is_lhs_bound = false;
			bool is_rhs_bound = false;
			bool is_lhs_bound_on_outer = false;
			bool is_rhs_bound_on_outer = false;
			if (qg_plan != nullptr) {
				is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name) ? true : false;
				is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name) ? true : false;
			}
			if (outer_plan) {
				// check bound with outer plan
				is_lhs_bound_on_outer = outer_plan->getSchema()->isNodeBound(lhs_name);
				is_lhs_bound = is_lhs_bound || is_lhs_bound_on_outer;
				is_rhs_bound_on_outer = outer_plan->getSchema()->isNodeBound(rhs_name);
				is_rhs_bound = is_rhs_bound || is_rhs_bound_on_outer;
			}

			LogicalPlan *hop_plan;	// constructed plan of a single hop
			// Plan R
			LogicalPlan *lhs_plan; LogicalPlan *edge_plan; LogicalPlan *rhs_plan;
			if (!is_pathjoin) {
				edge_plan = lPlanNodeOrRelExpr((NodeOrRelExpression *)qedge, false);
			} else {
				edge_plan = lPlanPathGet((RelExpression *)qedge);
			}
			D_ASSERT(edge_plan != nullptr);

			// Plan A - R
			CExpression *a_r_join_expr;
			if (!is_lhs_bound || (is_lhs_bound && !is_lhs_bound_on_outer)) {	// not bound or bound on inner
				// Join with R
				if (!is_lhs_bound) {
					lhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)lhs, true);
				} else { 
					lhs_plan = qg_plan; 
				}
				a_r_join_expr = lExprLogicalJoin(lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
					lhs_plan->getSchema()->getColRefOfKey(lhs_name, ID_COLNAME),
					edge_plan->getSchema()->getColRefOfKey(edge_name, SID_COLNAME),
					gpopt::COperator::EOperatorId::EopLogicalInnerJoin);
				lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
				lhs_plan->addBinaryParentOp(a_r_join_expr, edge_plan);
			} else {
				// No join, only filter with A
				lhs_plan = edge_plan;
				CExpression *selection_expr = CUtils::PexprLogicalSelect(mp, lhs_plan->getPlanExpr(),
					lExprScalarCmpEq(
						lExprScalarPropertyExpr(lhs_name, ID_COLNAME, outer_plan),
						lExprScalarPropertyExpr(edge_name, SID_COLNAME, lhs_plan) ));
				lhs_plan->addUnaryParentOp(selection_expr);
			}
			D_ASSERT(lhs_plan != nullptr);
			
			// Plan R - B
			if (is_lhs_bound && is_rhs_bound) {
				// no join necessary - add filter predicate on edge.tid = rhs.id
				CMemoryPool *mp = this->memory_pool;
				hop_plan = lhs_plan;
				CExpression *right_pred = is_rhs_bound_on_outer?
					lExprScalarPropertyExpr(rhs_name, ID_COLNAME, outer_plan)
					: lExprScalarPropertyExpr(rhs_name, ID_COLNAME, hop_plan);
				CExpression *selection_expr = CUtils::PexprLogicalSelect(mp, lhs_plan->getPlanExpr(),
					lExprScalarCmpEq(
						lExprScalarPropertyExpr(edge_name, TID_COLNAME, lhs_plan),
						right_pred )
				);
				hop_plan->addUnaryParentOp(selection_expr);
			} else if (!is_rhs_bound || (is_rhs_bound && !is_rhs_bound_on_outer)) {
				// join case
				if (!is_rhs_bound) {
					rhs_plan = lPlanNodeOrRelExpr((NodeOrRelExpression*)rhs, true);
				} else {
					rhs_plan = qg_plan;
				}
				auto join_expr = lExprLogicalJoin(lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
					lhs_plan->getSchema()->getColRefOfKey(edge_name, TID_COLNAME),
					rhs_plan->getSchema()->getColRefOfKey(rhs_name, ID_COLNAME),
					gpopt::COperator::EOperatorId::EopLogicalInnerJoin);
				lhs_plan->getSchema()->appendSchema(rhs_plan->getSchema());
				lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
				hop_plan = lhs_plan;
			} else {
				D_ASSERT(is_rhs_bound_on_outer);
				hop_plan = lhs_plan;
				CExpression* selection_expr = CUtils::PexprLogicalSelect(mp, hop_plan->getPlanExpr(),
					lExprScalarCmpEq(
						lExprScalarPropertyExpr(rhs_name, ID_COLNAME, outer_plan),
						lExprScalarPropertyExpr(edge_name, TID_COLNAME, hop_plan) ));
				hop_plan->addUnaryParentOp(selection_expr);
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
			auto node = (NodeOrRelExpression*)qg->getQueryNodes()[0].get();
			string node_name = node->getUniqueName();
			if( outer_plan->getSchema()->isNodeBound(node_name)) {
				D_ASSERT(false); // Not sure about the logic here. different from SQL
			}
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
	}
	GPOS_ASSERT(qg_plan != nullptr);

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
	CExpression* pred_expr;
	if( cnf_exprs->Size() > 1 ) {
		pred_expr = CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EBoolOperator::EboolopAnd, cnf_exprs);
	} else {
		pred_expr = cnf_exprs->operator[](0);
	}

	CExpression* selection_expr = CUtils::PexprLogicalSelect(mp, prev_plan->getPlanExpr(), pred_expr);
	prev_plan->addUnaryParentOp(selection_expr);

	// schema is never changed
	return prev_plan;
}


LogicalPlan* Planner::lPlanProjection(const expression_vector& expressions, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray* colrefs = GPOS_NEW(mp) CColRefArray(mp);
	LogicalSchema new_schema;

	for (auto& proj_expr_ptr: expressions) {
		vector<CExpression*> generated_exprs;
		vector<CColRef*> generated_colrefs;
		kuzu::binder::Expression* proj_expr = proj_expr_ptr.get();
		if( proj_expr->expressionType != kuzu::common::ExpressionType::VARIABLE ) {
			CExpression* expr = lExprScalarExpression(proj_expr, prev_plan);
			D_ASSERT( expr->Pop()->FScalar());
			string col_name = proj_expr->hasAlias() ? proj_expr->getAlias() : proj_expr->getUniqueName();
			if( CUtils::FScalarIdent(expr) ) {
				// reuse colref
				CColRef* orig_colref = col_factory->LookupColRef(((CScalarIdent*)(expr->Pop()))->Pcr()->Id());
				generated_colrefs.push_back(orig_colref);
				if( proj_expr->expressionType == kuzu::common::ExpressionType::PROPERTY ) {
					// considered as property only when users can still access as node property.
					// otherwise considered as general column
					PropertyExpression* prop_expr = (PropertyExpression*) proj_expr;
					if( prev_plan->getSchema()->isNodeBound(prop_expr->getVariableName()) ) {
						new_schema.appendNodeProperty(prop_expr->getVariableName(), prop_expr->getPropertyName(), orig_colref);
					} else {
						new_schema.appendEdgeProperty(prop_expr->getVariableName(), prop_expr->getPropertyName(), orig_colref);
					}
				} else {
					new_schema.appendColumn(col_name, generated_colrefs.back());
				}
				
			} else {
				// get new colref
				CScalar* scalar_op = (CScalar*)(expr->Pop());
				std::wstring w_col_name = L"";
				w_col_name.assign(col_name.begin(), col_name.end());
				const CWStringConst col_name_str(w_col_name.c_str());
				CName col_cname(&col_name_str);
				CColRef *new_colref = col_factory->PcrCreate(
					lGetMDAccessor()->RetrieveType(scalar_op->MdidType()), scalar_op->TypeModifier(), col_cname);
				generated_colrefs.push_back(new_colref);
				new_schema.appendColumn(col_name, generated_colrefs.back());
			}
			generated_exprs.push_back(expr);
			// add new property
		} else {
			// Handle kuzu::common::ExpressionType::VARIABLE here.
			kuzu::binder::NodeOrRelExpression* var_expr = (kuzu::binder::NodeOrRelExpression*)(proj_expr);
			auto var_colrefs = prev_plan->getSchema()->getAllColRefsOfKey(var_expr->getUniqueName());
			for( auto& colref: var_colrefs ) {
				generated_colrefs.push_back(colref);
				generated_exprs.push_back( lExprScalarPropertyExpr( var_expr->getUniqueName(), prev_plan->getSchema()->getPropertyNameOfColRef(var_expr->getUniqueName(), colref), prev_plan) );
				// TODO aliasing???
			}
			if( var_expr->getDataType().typeID == DataTypeID::NODE ) {
				new_schema.copyNodeFrom(prev_plan->getSchema(), var_expr->getUniqueName());
			} else { // rel
				new_schema.copyEdgeFrom(prev_plan->getSchema(), var_expr->getUniqueName());
			}
		}
		D_ASSERT(generated_exprs.size() > 0 && generated_exprs.size() == generated_colrefs.size());
		for(int expr_idx = 0; expr_idx < generated_exprs.size(); expr_idx++ ){
			CExpression* scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, generated_colrefs[expr_idx]), generated_exprs[expr_idx]);
			proj_array->Append(scalar_proj_elem);
		}
	}
	
	// Our columnar projection
	CExpression *pexprPrjList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
	CExpression *proj_expr =  GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp), prev_plan->getPlanExpr(), pexprPrjList);

	prev_plan->addUnaryParentOp(proj_expr);
	prev_plan->setSchema(move(new_schema));

	return prev_plan;
}

LogicalPlan* Planner::lPlanGroupBy(const expression_vector &expressions, LogicalPlan* prev_plan) {

	CMemoryPool* mp = this->memory_pool;
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CExpressionArray *agg_columns = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray* key_columns = GPOS_NEW(mp) CColRefArray(mp);
	agg_columns->AddRef();
	key_columns->AddRef();
	
	LogicalSchema new_schema;

	for( auto& proj_expr_ptr: expressions) {
		
		kuzu::binder::Expression* proj_expr = proj_expr_ptr.get();
		string col_name = proj_expr->getUniqueName();
		string col_name_print = proj_expr->hasAlias() ? proj_expr->getAlias() : proj_expr->getUniqueName();

		if(proj_expr->hasAggregationExpression()) {
			// AGG COLUMNS
			CExpression* expr = lExprScalarExpression(proj_expr, prev_plan);
			
			// get new colref and add to schema
			CScalar* scalar_op = (CScalar*)(expr->Pop());
			std::wstring w_col_name = L"";
			w_col_name.assign(col_name_print.begin(), col_name_print.end());
			const CWStringConst col_name_str(w_col_name.c_str());
			CName col_cname(&col_name_str);
			CColRef *new_colref = col_factory->PcrCreate(
				lGetMDAccessor()->RetrieveType(scalar_op->MdidType()), scalar_op->TypeModifier(), col_cname);	// col_name_print
			new_schema.appendColumn(col_name, new_colref);	// col_name
			
			// add to agg_columns
			auto* proj_elem = GPOS_NEW(mp)
				CExpression(mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), expr);
			agg_columns->Append(proj_elem);

		} else {
			// KEY COLUMNS
			if( proj_expr->expressionType == kuzu::common::ExpressionType::PROPERTY ) {
				CExpression* expr = lExprScalarExpression(proj_expr, prev_plan);
				// add original colref to schema
				CColRef* orig_colref = col_factory->LookupColRef(((CScalarIdent*)(expr->Pop()))->Pcr()->Id());
				if( proj_expr->expressionType == kuzu::common::ExpressionType::PROPERTY && !proj_expr->hasAlias() ) {
					// considered as property only when users can still access as node property.
					// otherwise considered as general column
					PropertyExpression* prop_expr = (PropertyExpression*) proj_expr;
					if( prev_plan->getSchema()->isNodeBound(prop_expr->getVariableName()) ) {
						new_schema.appendNodeProperty(prop_expr->getVariableName(), prop_expr->getPropertyName(), orig_colref);
					} else {
						new_schema.appendEdgeProperty(prop_expr->getVariableName(), prop_expr->getPropertyName(), orig_colref);
					}
				} else {
					// handle as column
					new_schema.appendColumn(col_name, orig_colref);
				}
				// add to key_columns
				key_columns->Append(orig_colref);
			} else if( proj_expr->expressionType == kuzu::common::ExpressionType::VARIABLE ) {
				// e.g. WITH person, AGG(...), ...
				auto property_columns = prev_plan->getSchema()->getAllColRefsOfKey(proj_expr->getUniqueName());
				for( auto& col: property_columns ) {
					// consider all columns as key columns
					// TODO this is inefficient
					key_columns->Append(col);
					if( prev_plan->getSchema()->isNodeBound(proj_expr->getUniqueName()) ) {
						// consider as node
						new_schema.appendNodeProperty(
							proj_expr->getUniqueName(),
							prev_plan->getSchema()->getPropertyNameOfColRef(proj_expr->getUniqueName(), col),
							col
						);
					} else {
						// considera as edge
						new_schema.appendEdgeProperty(
							proj_expr->getUniqueName(),
							prev_plan->getSchema()->getPropertyNameOfColRef(proj_expr->getUniqueName(), col),
							col
						);
					}

				}

			}
			
		}
		
	}

	CExpression *pexprList =
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), agg_columns);
	CExpression *agg_expr = CUtils::PexprLogicalGbAggGlobal(mp, key_columns, prev_plan->getPlanExpr(), pexprList);

	prev_plan->addUnaryParentOp(agg_expr);
	prev_plan->setSchema(move(new_schema));
	
	return prev_plan;
}

LogicalPlan *Planner::lPlanOrderBy(const expression_vector &orderby_exprs, const vector<bool> sort_orders, LogicalPlan *prev_plan) {
	CMemoryPool* mp = this->memory_pool;

	D_ASSERT(orderby_exprs.size() == sort_orders.size());

	vector<CColRef*> sort_colrefs;
	for (auto &orderby_expr: orderby_exprs) {
		auto &orderby_expr_type = orderby_expr.get()->expressionType;
		if(orderby_expr_type == kuzu::common::ExpressionType::PROPERTY) {
			// first depth projection = simple projection
			PropertyExpression *prop_expr = (PropertyExpression *)(orderby_expr.get());
			string k1 = prop_expr->getVariableName();
			string k2 = prop_expr->getPropertyName();
			CColRef* key_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
			// fallback to alias
			if (key_colref == NULL && prop_expr->hasAlias()) {
				k1 = prop_expr->getAlias();
				k2 = ""; 
				key_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
			}
			D_ASSERT(key_colref != NULL);
			sort_colrefs.push_back(key_colref);
		} else {
			CColRef* key_colref = prev_plan->getSchema()->getColRefOfKey(orderby_expr->getUniqueName(), "");
			// fallback to alias
			if (key_colref == NULL && orderby_expr->hasAlias()) {
				key_colref = prev_plan->getSchema()->getColRefOfKey(orderby_expr->getAlias(), "");
			}
			D_ASSERT(key_colref != NULL);
			sort_colrefs.push_back(key_colref);
		}
	}

	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);

	for (uint64_t i = 0; i < sort_colrefs.size(); i++) {
		CColRef *colref = sort_colrefs[i];

		IMDType::ECmpType sort_type = sort_orders[i] == true ? IMDType::EcmptL : IMDType::EcmptG; 	// TODO not sure...
		IMDId *mdid = colref->RetrieveType()->GetMdidForCmpType(sort_type); 
		pos->Append(mdid, colref, COrderSpec::EntLast);
	}

	CLogicalLimit *popLimit = GPOS_NEW(mp)
		CLogicalLimit(mp, pos, true /* fGlobal */, false /* fHasCount */,
					  true /*fTopLimitUnderDML*/);
	CExpression *pexprLimitOffset = CUtils::PexprScalarConstInt8(mp, 0/*ulOffSet*/);
	CExpression *pexprLimitCount = CUtils::PexprScalarConstInt8(mp, 0/*count*/, true/*is_null*/);

	CExpression *plan_orderby_expr = GPOS_NEW(mp)
		CExpression(mp, popLimit, prev_plan->getPlanExpr(), pexprLimitOffset, pexprLimitCount);

	prev_plan->addUnaryParentOp(plan_orderby_expr); // TODO ternary op?..
	return prev_plan;
}

LogicalPlan *Planner::lPlanDistinct(CColRefArray *colrefs, LogicalPlan *prev_plan) {
	CMemoryPool* mp = this->memory_pool;

	CLogicalGbAgg *pop_gbagg = 
		GPOS_NEW(mp) CLogicalGbAgg(
			mp, colrefs, COperator::EgbaggtypeGlobal /*egbaggtype*/);
	CExpression *gbagg_expr =  GPOS_NEW(mp)
		CExpression(mp, pop_gbagg, prev_plan->getPlanExpr(),
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));
	colrefs->AddRef();
	
	prev_plan->addUnaryParentOp(gbagg_expr);
	
	return prev_plan;
}

LogicalPlan *Planner::lPlanPathGet(RelExpression* edge_expr) {

	CMemoryPool* mp = this->memory_pool;

	auto table_oids = edge_expr->getTableIDs();

	// generate three columns, based on the first table
	CColRefArray* cols;
	LogicalSchema schema;
	auto edge_name = edge_expr->getUniqueName();
	auto edge_name_expr = edge_expr->getUniqueName();

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRefArray *path_output_cols = GPOS_NEW(mp) CColRefArray(mp);

	// tabledescs for underlying table
	CTableDescriptorArray *path_table_descs = GPOS_NEW(mp) CTableDescriptorArray(mp);
	path_table_descs->AddRef();
	for( auto& obj_id: table_oids ) {
		path_table_descs->Append(lCreateTableDescForRel(lGenRelMdid(obj_id), edge_name));
	}
	D_ASSERT(path_table_descs->Size() == table_oids.size() );

	CColumnDescriptorArray *pdrgpcoldesc = path_table_descs->operator[](0)->Pdrgpcoldesc(); // TODO need to change for Union All case
	IMDId *mdid_table = path_table_descs->operator[](0)->MDId(); // TODO need to change for Union All case
	auto &prop_exprs = edge_expr->getPropertyExpressions();
	D_ASSERT(pdrgpcoldesc->Size() >= prop_exprs.size());
	for( int colidx=0; colidx < prop_exprs.size(); colidx++) {
		auto& _prop_expr = prop_exprs[colidx];
		PropertyExpression *expr = static_cast<PropertyExpression*>(_prop_expr.get());

		// if property name in _id, _sid, _tid		
		if( expr->getPropertyName() == "_id" || expr->getPropertyName() == "_sid" || expr->getPropertyName() == "_tid") {
			gpmd::IMDId* col_type_imdid = lGetRelMd(table_oids[0])->GetMdCol(expr->getPropertyID(table_oids[0]))->MdidType();
			gpos::INT col_type_modifier = lGetRelMd(table_oids[0])->GetMdCol(expr->getPropertyID(table_oids[0]))->TypeModifier();

			std::string property_name = expr->getPropertyName();
			std::wstring property_print_name = L"";
			property_print_name.assign(property_name.begin(), property_name.end());
			const CWStringConst col_name_str(property_print_name.c_str());
			CName col_name(&col_name_str);
			
			CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[colidx];
			// generate colref
			// CColRef *new_colref = col_factory->PcrCreate(
			// 	lGetMDAccessor()->RetrieveType(col_type_imdid), col_type_modifier, col_name);
			CColRef *new_colref = col_factory->PcrCreate(
				pcoldesc, col_name, 0, false /* mark_as_used */, mdid_table); // TODO ulOpSourceId?
			path_output_cols->Append(new_colref);

			// add to schema
			schema.appendEdgeProperty(edge_name, property_name, new_colref);
		}
	}
	D_ASSERT( schema.getNumPropertiesOfKey(edge_name) == 3);
	D_ASSERT( path_output_cols->Size() == 3 );
	
	// generate get expression
	std::wstring w_alias = L"";
	w_alias.assign(edge_name.begin(), edge_name.end());
	const CWStringConst path_name_str(w_alias.c_str());

	CLogicalPathGet *pop = GPOS_NEW(mp) CLogicalPathGet(
		mp, GPOS_NEW(mp) CName(mp, CName(&path_name_str)), path_table_descs, path_output_cols,
		(gpos::INT) edge_expr->getLowerBound(), (gpos::INT) edge_expr->getUpperBound()
	);
	CExpression *path_get_expr = GPOS_NEW(mp) CExpression(mp, pop);

	path_output_cols->AddRef();

	LogicalPlan* plan = new LogicalPlan(path_get_expr, schema);
	GPOS_ASSERT( !plan->getSchema()->isEmpty() );
	return plan;
}

LogicalPlan *Planner::lPlanSkipOrLimit(BoundProjectionBody *proj_body, LogicalPlan *prev_plan) {
	CMemoryPool* mp = this->memory_pool;
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	bool hasCount = proj_body->hasLimit();
	CLogicalLimit *popLimit = GPOS_NEW(mp)
		CLogicalLimit(mp, pos, true /* fGlobal */, hasCount /* fHasCount */,
					  true /*fTopLimitUnderDML*/);
	CExpression *pexprLimitOffset, *pexprLimitCount;

	if (proj_body->hasSkip()) {
		pexprLimitOffset = CUtils::PexprScalarConstInt8(mp, proj_body->getSkipNumber()/*ulOffSet*/);
	} else {
		pexprLimitOffset = CUtils::PexprScalarConstInt8(mp, 0/*ulOffSet*/);
	}

	if (proj_body->hasLimit()) {
		pexprLimitCount = CUtils::PexprScalarConstInt8(mp, proj_body->getLimitNumber()/*count*/, false/*is_null*/);
	} else {
		pexprLimitCount = CUtils::PexprScalarConstInt8(mp, 0/*count*/, true/*is_null*/);
	}

	CExpression *plan_orderby_expr = GPOS_NEW(mp)
		CExpression(mp, popLimit, prev_plan->getPlanExpr(), pexprLimitOffset, pexprLimitCount);

	prev_plan->addUnaryParentOp(plan_orderby_expr); // TODO ternary op?..
	return prev_plan;
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
	auto node_name = node_expr->getUniqueName();
	auto node_name_print = node_expr->hasAlias() ? node_expr->getAlias(): node_expr->getUniqueName();

	// auto get_output = lExprLogicalGetNodeOrEdge(node_name_print, table_oids, &schema_proj_mapping, true);
	auto get_output = lExprLogicalGetNodeOrEdge(node_name_print, table_oids, &schema_proj_mapping, false); // schema mapping necessary only when UNION ALL inserted
	CExpression* plan_expr = get_output.first;
	D_ASSERT( prop_exprs.size() == get_output.second->Size() );

	// insert node schema
	LogicalSchema schema;
	for(int col_idx = 0; col_idx < prop_exprs.size(); col_idx++ ) {
		auto& _prop_expr = prop_exprs[col_idx];
		PropertyExpression* expr = static_cast<PropertyExpression*>(_prop_expr.get());
		string expr_name = expr->getUniqueName();
		if( is_node ) {
			schema.appendNodeProperty(node_name, expr_name, get_output.second->operator[](col_idx));
		}  else {
			schema.appendEdgeProperty(node_name, expr_name, get_output.second->operator[](col_idx));
		}
	}
	GPOS_ASSERT( schema.getNumPropertiesOfKey(node_name) == prop_exprs.size() );

	LogicalPlan* plan = new LogicalPlan(plan_expr, schema);
	GPOS_ASSERT( !plan->getSchema()->isEmpty() );
	return plan;
}



/*
*/
std::pair<CExpression*, CColRefArray*> Planner::lExprLogicalGetNodeOrEdge(string name, vector<uint64_t> &relation_oids,
	map<uint64_t, map<uint64_t, uint64_t>> *schema_proj_mapping, bool insert_projection) {
	
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
	for (int col_idx = 0; col_idx < num_proj_cols; col_idx++) {
		// foreach mappings
		uint64_t valid_oid;
		uint64_t valid_cid = std::numeric_limits<uint64_t>::max();

		for (auto& oid: relation_oids) {
			uint64_t idx_to_try = (*schema_proj_mapping)[oid].find(col_idx)->second;
			if (idx_to_try != std::numeric_limits<uint64_t>::max()) {
				valid_oid = oid;
				valid_cid = idx_to_try;
				break;
			}
		}
		GPOS_ASSERT(valid_cid != std::numeric_limits<uint64_t>::max());
		// extract info and maintain vector of column type infos
		gpmd::IMDId* col_type_imdid = lGetRelMd(valid_oid)->GetMdCol(valid_cid)->MdidType();
		gpos::INT col_type_modifier = lGetRelMd(valid_oid)->GetMdCol(valid_cid)->TypeModifier();
		union_schema_types.push_back(make_pair(col_type_imdid, col_type_modifier));
	}

	CColRefArray* idx0_output_array;
	for (int idx = 0; idx < relation_oids.size(); idx++) {
		auto& oid = relation_oids[idx];

		CExpression *expr;
		const gpos::ULONG num_cols = lGetRelMd(oid)->ColumnCount();

		GPOS_ASSERT(num_cols != 0);
		expr = lExprLogicalGet(oid, name);

		// conform schema if necessary
		CColRefArray* output_array;
		vector<uint64_t> project_col_ids;
		if (do_schema_mapping) {
			auto& mapping = (*schema_proj_mapping)[oid];
			assert(num_proj_cols == mapping.size());
			for(int proj_col_idx = 0; proj_col_idx < num_proj_cols; proj_col_idx++) {
				project_col_ids.push_back(mapping.find(proj_col_idx)->second);
			}
			GPOS_ASSERT(project_col_ids.size() > 0);
			auto proj_result = lExprScalarAddSchemaConformProject(expr, project_col_ids, &union_schema_types);
			expr = proj_result.first;
			output_array = proj_result.second;
		} else {
			// the output of logicalGet is always sorted, thus it is ok to use DeriveOutputColumns() here.
			output_array = expr->DeriveOutputColumns()->Pdrgpcr(mp);
		}

		/* Single table might not have the identical column mapping with original table. Thus projection is required */
		if (idx == 0) {
			// REL
			union_plan = expr;
			idx0_output_array = output_array;
		} else {
			// REL/UNION + REL
			// As the result of Union ALL keeps the colrefs of LHS, we always set lhs array as idx0_output_array
			union_plan = lExprLogicalUnionAllWithMapping(
				union_plan, idx0_output_array, expr, output_array);
		}
	}

	return make_pair(union_plan, idx0_output_array);
}

CExpression *Planner::lExprLogicalGet(uint64_t obj_id, string rel_name, string alias) {
	CMemoryPool* mp = this->memory_pool;

	if(alias == "") { alias = rel_name; }

	CTableDescriptor* ptabdesc = lCreateTableDescForRel( lGenRelMdid(obj_id), rel_name);

	std::wstring w_alias = L"";
	w_alias.assign(alias.begin(), alias.end());
	CWStringConst strAlias(w_alias.c_str());

	CLogicalGet *pop = GPOS_NEW(mp) CLogicalGet(
		mp, GPOS_NEW(mp) CName(mp, CName(&strAlias)), ptabdesc);

	CExpression *scan_expr = GPOS_NEW(mp) CExpression(mp, pop);
	CColRefArray *arr = pop->PdrgpcrOutput();
	for (ULONG ul = 0; ul < arr->Size(); ul++) {
		CColRef *ref = (*arr)[ul];
		ref->MarkAsUnknown();
	}
	return scan_expr;
}

CExpression *Planner::lExprLogicalUnionAllWithMapping(CExpression* lhs, CColRefArray* lhs_mapping, CExpression* rhs, CColRefArray* rhs_mapping) {
	GPOS_ASSERT(rhs_mapping != nullptr); // must be binary

	CMemoryPool *mp = this->memory_pool;

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


/*
 * CExpression* returns result of projected schema.
*/
std::pair<CExpression*, CColRefArray*> Planner::lExprScalarAddSchemaConformProject(CExpression* relation,
	vector<uint64_t> col_ids_to_project, vector<pair<gpmd::IMDId*, gpos::INT>>* target_schema_types) {
	// col_ids_to_project may include std::numeric_limits<uint64_t>::max(),
	// which indicates null projection
	CMemoryPool* mp = this->memory_pool;

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray* output_col_array = GPOS_NEW(mp) CColRefArray(mp);
	output_col_array->AddRef();

	uint64_t target_col_id = 0;
	for (auto col_id: col_ids_to_project) {
		CExpression* scalar_proj_elem;
		if (col_id == std::numeric_limits<uint64_t>::max()) {
			GPOS_ASSERT(target_schema_types != nullptr);
			// project null column
			auto& type_info = (*target_schema_types)[target_col_id];
			CExpression* null_expr =
				CUtils::PexprScalarConstNull(mp, lGetMDAccessor()->RetrieveType(type_info.first) , type_info.second);
			const CWStringConst col_name_str(GPOS_WSZ_LIT("const_null"));
			CName col_name(&col_name_str);
			CColRef *new_colref = col_factory->PcrCreate(lGetMDAccessor()->RetrieveType(type_info.first), type_info.second, col_name);
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), null_expr);
			
			proj_array->Append(scalar_proj_elem);
			output_col_array->Append(new_colref);
		} else {
			// project non-null column
			// the output of logicalGet is always sorted, thus it is ok to use DeriveOutputColumns() here.
			CColRef *colref = relation->DeriveOutputColumns()->Pdrgpcr(mp)->operator[](col_id);
			CExpression* ident_expr = GPOS_NEW(mp)
					CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
			scalar_proj_elem = GPOS_NEW(mp) CExpression(
				mp, GPOS_NEW(mp) CScalarProjectElement(mp, colref), ident_expr); // ident element do not assign new colref id

			proj_array->Append(scalar_proj_elem);	
			output_col_array->Append(colref);
			// ######
			// CColRef *colref = relation->DeriveOutputColumns()->Pdrgpcr(mp)->operator[](col_id);
			// CColRef *new_colref = col_factory->PcrCreate(colref->RetrieveType(), colref->TypeModifier(), colref->Name());	// generate new reference having same name
			// CExpression* ident_expr = GPOS_NEW(mp)
			// 		CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
			// scalar_proj_elem = GPOS_NEW(mp) CExpression(
			// 	mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref), ident_expr); // TODO S62 change to colref
			// proj_array->Append(scalar_proj_elem);	
			// output_col_array->Append(new_colref);
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

CExpression * Planner::lExprLogicalJoin(CExpression* lhs, CExpression* rhs,
		CColRef* lhs_colref, CColRef* rhs_colref, gpopt::COperator::EOperatorId join_op) {
		
	CMemoryPool* mp = this->memory_pool;

	CColRef *pcrLeft = lhs_colref;
	CColRef *pcrRight = rhs_colref;

	lhs->AddRef();
	rhs->AddRef();

	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
	pexprEquality->AddRef();
	
	CExpression* join_result;
	switch(join_op) {
		case gpopt::COperator::EOperatorId::EopLogicalInnerJoin: {
			join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprEquality);
break;
		}
		case gpopt::COperator::EOperatorId::EopLogicalRightOuterJoin: {
			join_result = CUtils::PexprLogicalJoin<CLogicalRightOuterJoin>(mp, lhs, rhs, pexprEquality);
			break;
		}
		case gpopt::COperator::EOperatorId::EopLogicalLeftOuterJoin: {
			join_result = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(mp, lhs, rhs, pexprEquality);
			break;
		}
		default:
			D_ASSERT(false);
		
	}
	D_ASSERT(join_result != NULL);

	return join_result;
}

CExpression* Planner::lExprLogicalPathJoin(CExpression* lhs, CExpression* rhs,
		CColRef* lhs_colref, CColRef* rhs_colref, int32_t lower_bound, int32_t upper_bound, gpopt::COperator::EOperatorId join_op) {

	CMemoryPool* mp = this->memory_pool;

	D_ASSERT(join_op == gpopt::COperator::EOperatorId::EopLogicalInnerJoin);

	CColRef *pcrLeft = lhs_colref;
	CColRef *pcrRight = rhs_colref;
	lhs->AddRef();
	rhs->AddRef();

	CExpression *pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
	pexprEquality->AddRef();

	auto join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprEquality);

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


CTableDescriptor * Planner::lCreateTableDescForRel(CMDIdGPDB* rel_mdid, std::string rel_name) {

	CMemoryPool* mp = this->memory_pool; 

	const CWStringConst* table_name_cwstring = lGetMDAccessor()->RetrieveRel(rel_mdid)->Mdname().GetMDName();
	wstring table_name_ws(table_name_cwstring->GetBuffer());
	string table_name(table_name_ws.begin(), table_name_ws.end());
	std::wstring w_print_name = L"";
	w_print_name.assign(table_name.begin(), table_name.end());
	CWStringConst strName(w_print_name.c_str());
	CTableDescriptor *ptabdesc =
		lCreateTableDesc(mp,
					   rel_mdid,			// 6.objid.0.0
					   CName(&strName),	// debug purpose table string
					   rel_name
					   );	
	
	ptabdesc->AddRef();
	return ptabdesc;
}

CTableDescriptor * Planner::lCreateTableDesc(CMemoryPool *mp, IMDId *mdid,
						   const CName &nameTable, string rel_name, gpos::BOOL fPartitioned) {

	CTableDescriptor *ptabdesc = lTabdescPlainWithColNameFormat(mp, mdid,
										  GPOS_WSZ_LIT("column_%04d"),				// format notused
										  nameTable,
										  rel_name,
										  false /* is_nullable */);	// TODO retrieve isnullable from the storage!
										
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
		const CName &nameTable, string rel_name, gpos::BOOL is_nullable  // define nullable columns
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


		wstring col_name_ws(md_col->Mdname().GetMDName()->GetBuffer());
		string col_name(col_name_ws.begin(), col_name_ws.end());
		if(rel_name != "") {
			col_name = rel_name + "." + col_name;
		}
		std::wstring col_print_name = L"";
		col_print_name.assign(col_name.begin(), col_name.end());
		CWStringConst strName(col_print_name.c_str());

		CName colname(&strName);
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