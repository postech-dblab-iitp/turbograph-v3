#include "optimizer/mdprovider/MDProviderTBGPP.h"
#include "planner/planner.hpp"

#include <limits>
#include <string>
#include <numeric>
#include <functional>

namespace s62 {

LogicalPlan *Planner::lGetLogicalPlan()
{
    return nullptr;
    // GPOS_ASSERT(this->bound_statement != nullptr);
    // auto &regularQuery = *((BoundRegularQuery *)(this->bound_statement));

    // // TODO need union between single queries
    // GPOS_ASSERT(regularQuery.getNumSingleQueries() == 1);
    // vector<LogicalPlan *> childLogicalPlans(regularQuery.getNumSingleQueries());
    // for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
    //     childLogicalPlans[i] =
    //         lPlanSingleQuery(*regularQuery.getSingleQuery(i));
    // }
    // return childLogicalPlans[0];
}

// LogicalPlan *Planner::lPlanSingleQuery(const NormalizedSingleQuery &singleQuery)
// {
//     CMemoryPool *mp = this->memory_pool;
//     LogicalPlan *cur_plan = nullptr;
//     // TODO refer kuzu properties to scan
//     // populate properties to scan
//     // propertiesToScan.clear();

//     for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
//         cur_plan = lPlanQueryPart(*singleQuery.getQueryPart(i), cur_plan);
//     }

//     return cur_plan;
// }

// LogicalPlan *Planner::lPlanQueryPart(const NormalizedQueryPart &queryPart,
//                                      LogicalPlan *prev_plan)
// {
//     LogicalPlan *cur_plan = prev_plan;
//     for (auto i = 0u; i < queryPart.getNumReadingClause(); i++) {
//         // There may be no reading clause, WITH ... WITH
//         cur_plan = lPlanReadingClause(queryPart.getReadingClause(i), cur_plan);
//     }
//     GPOS_ASSERT(queryPart.getNumUpdatingClause() == 0);
//     // plan projectionBody after reading clauses
//     if (queryPart.hasProjectionBody()) {
//         // WITH ...
//         cur_plan = lPlanProjectionBody(cur_plan, queryPart.getProjectionBody());

//         if (queryPart.hasProjectionBodyPredicate()) {
//             // WITH ... WHERE ...
//             // Need to check semantics on whether filter or project should be planned first on behalf of other.
//             // maybe filter first?
//             cur_plan = lPlanSelection(
//                 queryPart.getProjectionBodyPredicate().get()->splitOnAND(),
//                 cur_plan);
//             // appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
//         }
//     }
//     return cur_plan;
// }

// LogicalPlan *Planner::lPlanProjectionBody(LogicalPlan *plan,
//                                           BoundProjectionBody *proj_body)
// {
//     CMemoryPool *mp = this->memory_pool;

//     // check if agregation necessary
//     bool agg_required = false;
//     for (auto &projectionExpression : proj_body->getProjectionExpressions()) {
//         if (lTryGenerateScalarIdent(projectionExpression.get(), plan) ==
//                 NULL  // new expression to be evaluated
//             &&
//             projectionExpression->hasAggregationExpression()  // has aggregation
//         ) {
//             agg_required = true;
//         }
//     }

//     /* Aggregate - generate LogicalGbAgg series */
//     if (agg_required) {
//         plan = lPlanGroupBy(proj_body->getProjectionExpressions(),
//                             plan);  // TODO what if agg + projection
//                                     // TODO plan is manipulated

//         // handle all non-agg projection + original columns first
//         // orca will remove unnecessary columns so no worries!
//     }
//     else {
//         plan = lPlanProjection(proj_body->getProjectionExpressions(), plan);
//     }

//     /* OrderBy */
//     if (proj_body->hasOrderByExpressions()) {
//         // orderByExpressions
//         // isAscOrders
//         const expression_vector &orderby_expr =
//             proj_body->getOrderByExpressions();
//         const vector<bool> sort_orders =
//             proj_body->getSortingOrders();  // if true asc
//         plan = lPlanOrderBy(orderby_expr, sort_orders, plan);
//     }

//     /* Scalar projection - using CLogicalProject */
//     // find all projection expressions that requires new columns
//     // generate logicalproiection and record the mappings

//     // maintain new mappings

//     /* Distinct */
//     if (proj_body->getIsDistinct()) {
//         plan = lPlanDistinct(
//             proj_body->getProjectionExpressions(),
//             plan->getPlanExpr()->DeriveOutputColumns()->Pdrgpcr(mp), plan);
//     }

//     /* Skip limit */
//     if (proj_body->hasSkipOrLimit()) {
//         plan = lPlanSkipOrLimit(proj_body, plan);
//     }

//     GPOS_ASSERT(plan != nullptr);
//     return plan;
// }

// LogicalPlan *Planner::lPlanReadingClause(BoundReadingClause *boundReadingClause,
//                                          LogicalPlan *prev_plan)
// {
//     LogicalPlan *plan;

//     auto readingClauseType = boundReadingClause->getClauseType();
//     switch (readingClauseType) {
//         case ClauseType::MATCH: {
//             plan = lPlanMatchClause(boundReadingClause, prev_plan);
//         } break;
//         case ClauseType::UNWIND: {
//             plan = lPlanUnwindClause(boundReadingClause, prev_plan);
//         } break;
//         default:
//             GPOS_ASSERT(false);
//     }

//     return plan;
// }

// LogicalPlan *Planner::lPlanMatchClause(BoundReadingClause *boundReadingClause,
//                                        LogicalPlan *prev_plan)
// {
//     auto boundMatchClause = (BoundMatchClause *)boundReadingClause;
//     auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
//     expression_vector predicates =
//         boundMatchClause->hasWhereExpression()
//             ? boundMatchClause->getWhereExpression()->splitOnAND()
//             :  // CNF form
//             expression_vector{};

//     LogicalPlan *plan;
//     if (!boundMatchClause->getIsOptional()) {
//         plan = lPlanRegularMatch(*queryGraphCollection, prev_plan);
//     }
//     else {
//         plan = lPlanRegularOptionalMatch(*queryGraphCollection, prev_plan);
//     }

//     // TODO append edge isomorphism
//     // TODO need to know about the label info...
//     // for each qg in qgc,
//     // list all edges
//     // nested for loops

//     // plan selection on match clause
//     if (predicates.size() > 0) {
//         plan = lPlanSelection(std::move(predicates), plan);
//     }

//     return plan;
// }

// LogicalPlan *Planner::lPlanUnwindClause(BoundReadingClause *boundReadingClause,
//                                         LogicalPlan *prev_plan)
// {
//     GPOS_ASSERT(false);
//     return nullptr;
// }

// LogicalPlan *Planner::lPlanRegularMatch(const QueryGraphCollection &qgc,
//                                         LogicalPlan *prev_plan)
// {
//     LogicalPlan *plan = nullptr;
//     LogicalPlan *qg_plan = prev_plan;

//     GPOS_ASSERT(qgc.getNumQueryGraphs() > 0);
//     for (int idx = 0; idx < qgc.getNumQueryGraphs(); idx++) {
//         QueryGraph *qg = qgc.getQueryGraph(idx);

//         if (qg->getNumQueryRels() >= 1) {
//             for (int edge_idx = 0; edge_idx < qg->getNumQueryRels();
//                  edge_idx++) {
//                 RelExpression *qedge = qg->getQueryRel(edge_idx).get();
//                 NodeExpression *lhs = qedge->getSrcNode().get();
//                 NodeExpression *rhs = qedge->getDstNode().get();
//                 string edge_name = qedge->getUniqueName();
//                 string lhs_name = lhs->getUniqueName();
//                 string rhs_name = rhs->getUniqueName();

//                 bool is_lhs_bound = false;
//                 bool is_rhs_bound = false;
//                 if (qg_plan != nullptr) {
//                     is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name)
//                                        ? true
//                                        : false;
//                     is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name)
//                                        ? true
//                                        : false;
//                 }

//                 // case for variable length join
//                 bool is_pathjoin =
//                     qedge->getLowerBound() != 1 || qedge->getUpperBound() != 1;

//                 LogicalPlan *hop_plan;
//                 LogicalPlan *lhs_plan;
//                 // A join R
//                 if (!is_lhs_bound) {
//                     lhs_plan =
//                         lPlanNodeOrRelExpr((NodeOrRelExpression *)lhs);
//                 }
//                 else {
//                     // lhs bound
//                     lhs_plan = qg_plan;
//                 }
//                 GPOS_ASSERT(lhs_plan != nullptr);

//                 // case for shortest path join
//                 bool is_shortestpath =
//                     qg->getQueryGraphType() == QueryGraphType::SHORTEST;

//                 // create shorest path operator
//                 if (is_shortestpath) {
//                     D_ASSERT(qg->getNumQueryRels() == 1);
//                     D_ASSERT(is_pathjoin);
//                     D_ASSERT(is_lhs_bound);
//                     D_ASSERT(is_rhs_bound);

//                     qg_plan = lPlanShortestPath(qg, lhs, qedge, rhs, lhs_plan);
//                     break;
//                 }

//                 bool is_all_shortestpath =
//                     qg->getQueryGraphType() == QueryGraphType::ALL_SHORTEST;

//                 if (is_all_shortestpath) {
//                     D_ASSERT(qg->getNumQueryRels() == 1);
//                     D_ASSERT(is_pathjoin);
//                     D_ASSERT(is_lhs_bound);
//                     D_ASSERT(is_rhs_bound);

//                     qg_plan = lPlanAllShortestPath(qg, lhs, qedge, rhs, lhs_plan);
//                     break;
//                 }

//                 // Scan R
//                 LogicalPlan *edge_plan;
//                 CExpression *a_r_join_expr;
//                 auto ar_join_type =
//                     gpopt::COperator::EOperatorId::EopLogicalInnerJoin;

//                 edge_plan = is_pathjoin
//                                 ? lPlanPathGet((RelExpression *)qedge)
//                                 : lPlanNodeOrRelExpr(
//                                       (NodeOrRelExpression *)qedge);

//                 a_r_join_expr =
//                     is_pathjoin
//                         ? lExprLogicalPathJoin(
//                               lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
//                               lhs_plan->getSchema()->getColRefOfKey(lhs_name,
//                                                                     ID_COLNAME_ID),
//                               edge_plan->getSchema()->getColRefOfKey(
//                                   edge_name, SID_COLNAME_ID),
//                               qedge->getLowerBound(), qedge->getUpperBound(),
//                               ar_join_type)
//                         : lExprLogicalJoin(
//                               lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
//                               lhs_plan->getSchema()->getColRefOfKey(lhs_name,
//                                                                     ID_COLNAME_ID),
//                               edge_plan->getSchema()->getColRefOfKey(
//                                   edge_name, SID_COLNAME_ID),
//                               ar_join_type, nullptr);
//                 lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
//                 lhs_plan->addBinaryParentOp(a_r_join_expr, edge_plan);

//                 // R join B
//                 if (is_lhs_bound && is_rhs_bound) {
//                     // no join necessary - add filter predicate on edge.tid = rhs.id
//                     CMemoryPool *mp = this->memory_pool;
//                     hop_plan = lhs_plan;
//                     CExpression *selection_expr = CUtils::PexprLogicalSelect(
//                         mp, lhs_plan->getPlanExpr(),
//                         lExprScalarCmpEq(lExprScalarPropertyExpr(
//                                              edge_name, TID_COLNAME_ID, lhs_plan),
//                                          lExprScalarPropertyExpr(
//                                              rhs_name, ID_COLNAME_ID, lhs_plan)));
//                     hop_plan->addUnaryParentOp(selection_expr);
//                 }
//                 else {
//                     LogicalPlan *rhs_plan;
//                     // join necessary
//                     if (!is_rhs_bound) {
//                         rhs_plan = lPlanNodeOrRelExpr(
//                             (NodeOrRelExpression *)rhs);
//                     }
//                     else {
//                         // lhs unbound and rhs bound
//                         rhs_plan = qg_plan;
//                     }
//                     // (AR) join B
//                     auto rb_join_type =
//                         gpopt::COperator::EOperatorId::EopLogicalInnerJoin;
//                     auto join_expr = lExprLogicalJoin(
//                         lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
//                         lhs_plan->getSchema()->getColRefOfKey(edge_name,
//                                                               TID_COLNAME_ID),
//                         rhs_plan->getSchema()->getColRefOfKey(rhs_name,
//                                                               ID_COLNAME_ID),
//                         rb_join_type, nullptr);
//                     lhs_plan->getSchema()->appendSchema(rhs_plan->getSchema());
//                     lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
//                     hop_plan = lhs_plan;
//                 }
//                 GPOS_ASSERT(hop_plan != nullptr);

//                 // When lhs, rhs is unbound, qg_plan is not merged with the hop_plan. Thus cartprod.
//                 if ((qg_plan != nullptr) && (!is_lhs_bound) &&
//                     (!is_rhs_bound)) {
//                     auto cart_expr = lExprLogicalCartProd(
//                         qg_plan->getPlanExpr(), hop_plan->getPlanExpr());
//                     qg_plan->getSchema()->appendSchema(hop_plan->getSchema());
//                     qg_plan->addBinaryParentOp(cart_expr, hop_plan);
//                 }
//                 else {
//                     qg_plan = hop_plan;
//                 }
//                 GPOS_ASSERT(qg_plan != nullptr);
//             }
//         }
//         else {
//             // if no edge, this is single node scan case
//             D_ASSERT(qg->getQueryNodes().size() == 1);

//             LogicalPlan *nodescan_plan = lPlanNodeOrRelExpr(
//                 (NodeOrRelExpression *)qg->getQueryNodes()[0].get());
//             if (qg_plan == nullptr) {
//                 qg_plan = nodescan_plan;
//             }
//             else {
//                 // cartprod
//                 auto cart_expr = lExprLogicalCartProd(
//                     qg_plan->getPlanExpr(), nodescan_plan->getPlanExpr());
//                 qg_plan->getSchema()->appendSchema(nodescan_plan->getSchema());
//                 qg_plan->addBinaryParentOp(cart_expr, nodescan_plan);
//             }
//         }
//     }
//     GPOS_ASSERT(qg_plan != nullptr);

//     return qg_plan;
// }

// LogicalPlan *Planner::lPlanRegularOptionalMatch(const QueryGraphCollection &qgc,
//                                                 LogicalPlan *prev_plan)
// {
//     LogicalPlan *plan = nullptr;

//     // maintain snapshot of the prev. schema, which is used for optional match
//     s62::LogicalSchema *prev_plan_schema;
//     if (prev_plan == nullptr) {
//         prev_plan_schema = new s62::LogicalSchema();
//     }
//     else {
//         prev_plan_schema = prev_plan->getSchema();
//     }

//     LogicalPlan *qg_plan = prev_plan;

//     bool is_forward_traverse = true;  // true for forward, false for backward
//     int start_edge_idx = -1;          // where to start planning
//     D_ASSERT(qgc.getNumQueryGraphs() == 1);

//     QueryGraph *qg = qgc.getQueryGraph(0);
//     for (int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
//         if (edge_idx == 0) {
//             RelExpression *qedge = qg->getQueryRel(edge_idx).get();
//             string lhs_name = qedge->getSrcNode()->getUniqueName();
//             string rhs_name = qedge->getDstNode()->getUniqueName();
//             if (prev_plan_schema->isNodeBound(lhs_name) ||
//                 prev_plan_schema->isNodeBound(rhs_name)) {
//                 is_forward_traverse = true;
//                 start_edge_idx = edge_idx;
//                 break;
//             }
//         }

//         if (edge_idx == qg->getNumQueryRels() - 1) {
//             RelExpression *qedge = qg->getQueryRel(edge_idx).get();
//             string lhs_name = qedge->getSrcNode()->getUniqueName();
//             string rhs_name = qedge->getDstNode()->getUniqueName();
//             if (prev_plan_schema->isNodeBound(lhs_name) ||
//                 prev_plan_schema->isNodeBound(rhs_name)) {
//                 is_forward_traverse = false;
//                 start_edge_idx = edge_idx;
//                 break;
//             }
//         }
//     }

//     // TODO currently, we allow these cases only
//     GPOS_ASSERT(start_edge_idx == 0 ||
//                 start_edge_idx == qg->getNumQueryRels() - 1);

//     GPOS_ASSERT(qgc.getNumQueryGraphs() > 0);
//     for (int idx = 0; idx < qgc.getNumQueryGraphs(); idx++) {
//         QueryGraph *qg = qgc.getQueryGraph(idx);
//         auto qg_type = qg->getQueryGraphType();
//         int edge_idx = is_forward_traverse ? 0 : qg->getNumQueryRels() - 1;
//         if (qg->getNumQueryRels() >= 1) {
//             for (;;) {
//                 bool is_src_lhs = false;
//                 RelExpression *qedge = qg->getQueryRel(edge_idx).get();
//                 NodeExpression *lhs, *rhs;
//                 if (prev_plan_schema->isNodeBound(
//                         qedge->getSrcNode()->getUniqueName())) {
//                     lhs = qedge->getSrcNode().get();
//                     rhs = qedge->getDstNode().get();
//                     is_src_lhs = true;
//                 }
//                 else if (prev_plan_schema->isNodeBound(
//                              qedge->getDstNode()->getUniqueName())) {
//                     lhs = qedge->getDstNode().get();
//                     rhs = qedge->getSrcNode().get();
//                     is_src_lhs = false;
//                 }
//                 else {
//                     lhs = qedge->getSrcNode().get();
//                     rhs = qedge->getDstNode().get();
//                     is_src_lhs = true;
//                 }
//                 string edge_name = qedge->getUniqueName();
//                 string lhs_name = lhs->getUniqueName();
//                 string rhs_name = rhs->getUniqueName();

//                 bool is_lhs_bound = false;
//                 bool is_rhs_bound = false;
//                 if (qg_plan != nullptr) {
//                     is_lhs_bound = qg_plan->getSchema()->isNodeBound(lhs_name)
//                                        ? true
//                                        : false;
//                     is_rhs_bound = qg_plan->getSchema()->isNodeBound(rhs_name)
//                                        ? true
//                                        : false;
//                 }

//                 // case for variable length join
//                 bool is_pathjoin =
//                     qedge->getLowerBound() != 1 || qedge->getUpperBound() != 1;

//                 // case for optional match
//                 bool push_selection_pred_into_join =
//                     is_lhs_bound && is_rhs_bound;

//                 LogicalPlan *hop_plan;
//                 LogicalPlan *lhs_plan;
//                 // A join R
//                 if (!is_lhs_bound) {
//                     lhs_plan =
//                         lPlanNodeOrRelExpr((NodeOrRelExpression *)lhs);
//                 }
//                 else {
//                     // lhs bound
//                     lhs_plan = qg_plan;
//                 }
//                 GPOS_ASSERT(lhs_plan != nullptr);

//                 // case for shortest path join
//                 bool is_shortestpath = qg_type == QueryGraphType::SHORTEST;

//                 // create shorest path operator
//                 if (is_shortestpath) {
//                     D_ASSERT(qg->getNumQueryRels() == 1);
//                     D_ASSERT(is_pathjoin);
//                     D_ASSERT(is_lhs_bound);
//                     D_ASSERT(is_rhs_bound);

//                     qg_plan = lPlanShortestPath(qg, lhs, qedge, rhs, lhs_plan);
//                     break;
//                 }

//                 bool is_all_shortestpath = qg_type == QueryGraphType::ALL_SHORTEST;

//                 if (is_shortestpath) {
//                     D_ASSERT(qg->getNumQueryRels() == 1);
//                     D_ASSERT(is_pathjoin);
//                     D_ASSERT(is_lhs_bound);
//                     D_ASSERT(is_rhs_bound);

//                     qg_plan = lPlanAllShortestPath(qg, lhs, qedge, rhs, lhs_plan);
//                     break;
//                 }

//                 // Scan R
//                 LogicalPlan *edge_plan;
//                 CExpression *a_r_join_expr;
//                 auto ar_join_type =
//                     gpopt::COperator::EOperatorId::EopLogicalLeftOuterJoin;
//                 CExpression *additional_join_pred = nullptr;

//                 edge_plan = is_pathjoin
//                                 ? lPlanPathGet((RelExpression *)qedge)
//                                 : lPlanNodeOrRelExpr(
//                                       (NodeOrRelExpression *)qedge);

//                 lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
//                 if (push_selection_pred_into_join) {
//                     additional_join_pred = lExprScalarCmpEq(
//                         lExprScalarPropertyExpr(
//                             edge_name, is_src_lhs ? TID_COLNAME_ID : SID_COLNAME_ID,
//                             lhs_plan),
//                         lExprScalarPropertyExpr(rhs_name, ID_COLNAME_ID,
//                                                 lhs_plan));
//                 }
//                 a_r_join_expr =
//                     is_pathjoin
//                         ? lExprLogicalPathJoin(
//                               lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
//                               lhs_plan->getSchema()->getColRefOfKey(lhs_name,
//                                                                     ID_COLNAME_ID),
//                               edge_plan->getSchema()->getColRefOfKey(
//                                   edge_name,
//                                   is_src_lhs ? SID_COLNAME_ID : TID_COLNAME_ID),
//                               qedge->getLowerBound(), qedge->getUpperBound(),
//                               ar_join_type)
//                         : lExprLogicalJoin(
//                               lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
//                               lhs_plan->getSchema()->getColRefOfKey(lhs_name,
//                                                                     ID_COLNAME_ID),
//                               edge_plan->getSchema()->getColRefOfKey(
//                                   edge_name,
//                                   is_src_lhs ? SID_COLNAME_ID : TID_COLNAME_ID),
//                               ar_join_type, additional_join_pred);
//                 lhs_plan->addBinaryParentOp(a_r_join_expr, edge_plan);

//                 // R join B
//                 if (!push_selection_pred_into_join) {
//                     if (is_lhs_bound && is_rhs_bound) {
//                         // no join necessary - add filter predicate on edge.tid = rhs.id
//                         CMemoryPool *mp = this->memory_pool;
//                         hop_plan = lhs_plan;
//                         CExpression *selection_expr =
//                             CUtils::PexprLogicalSelect(
//                                 mp, lhs_plan->getPlanExpr(),
//                                 lExprScalarCmpEq(
//                                     lExprScalarPropertyExpr(
//                                         edge_name,
//                                         is_src_lhs ? TID_COLNAME_ID : SID_COLNAME_ID,
//                                         lhs_plan),
//                                     lExprScalarPropertyExpr(
//                                         rhs_name, ID_COLNAME_ID, lhs_plan)));
//                         hop_plan->addUnaryParentOp(selection_expr);
//                     }
//                     else {
//                         LogicalPlan *rhs_plan;
//                         // join necessary
//                         if (!is_rhs_bound) {
//                             rhs_plan = lPlanNodeOrRelExpr(
//                                 (NodeOrRelExpression *)rhs);
//                         }
//                         else {
//                             // lhs unbound and rhs bound
//                             rhs_plan = qg_plan;
//                         }
//                         // (AR) join B
//                         auto rb_join_type = gpopt::COperator::EOperatorId::
//                             EopLogicalLeftOuterJoin;
//                         auto join_expr = lExprLogicalJoin(
//                             lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
//                             lhs_plan->getSchema()->getColRefOfKey(
//                                 edge_name,
//                                 is_src_lhs ? TID_COLNAME_ID : SID_COLNAME_ID),
//                             rhs_plan->getSchema()->getColRefOfKey(rhs_name,
//                                                                   ID_COLNAME_ID),
//                             rb_join_type, nullptr);
//                         lhs_plan->getSchema()->appendSchema(
//                             rhs_plan->getSchema());
//                         lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
//                         hop_plan = lhs_plan;
//                     }
//                 }
//                 else {
//                     hop_plan = lhs_plan;
//                 }
//                 GPOS_ASSERT(hop_plan != nullptr);

//                 // When lhs, rhs is unbound, qg_plan is not merged with the hop_plan. Thus cartprod.
//                 if ((qg_plan != nullptr) && (!is_lhs_bound) &&
//                     (!is_rhs_bound)) {
//                     auto cart_expr = lExprLogicalCartProd(
//                         qg_plan->getPlanExpr(), hop_plan->getPlanExpr());
//                     qg_plan->getSchema()->appendSchema(hop_plan->getSchema());
//                     qg_plan->addBinaryParentOp(cart_expr, hop_plan);
//                 }
//                 else {
//                     qg_plan = hop_plan;
//                 }
//                 GPOS_ASSERT(qg_plan != nullptr);

//                 if (is_forward_traverse) {
//                     edge_idx++;
//                     if (edge_idx >= qg->getNumQueryRels()) {
//                         break;
//                     }
//                 }
//                 else {
//                     edge_idx--;
//                     if (edge_idx < 0) {
//                         break;
//                     }
//                 }
//             }
//         }
//         else {
//             // if no edge, this is single node scan case
//             D_ASSERT(qg->getQueryNodes().size() == 1);

//             LogicalPlan *nodescan_plan = lPlanNodeOrRelExpr(
//                 (NodeOrRelExpression *)qg->getQueryNodes()[0].get());
//             if (qg_plan == nullptr) {
//                 qg_plan = nodescan_plan;
//             }
//             else {
//                 // cartprod
//                 auto cart_expr = lExprLogicalCartProd(
//                     qg_plan->getPlanExpr(), nodescan_plan->getPlanExpr());
//                 qg_plan->getSchema()->appendSchema(nodescan_plan->getSchema());
//                 qg_plan->addBinaryParentOp(cart_expr, nodescan_plan);
//             }
//         }
//         GPOS_ASSERT(qg_plan != nullptr);
//     }
//     GPOS_ASSERT(qg_plan != nullptr);

//     return qg_plan;
// }

// LogicalPlan *Planner::lPlanRegularMatchFromSubquery(
//     const QueryGraphCollection &qgc, LogicalPlan *outer_plan)
// {
//     // no optional match
//     LogicalPlan *plan = nullptr;
//     GPOS_ASSERT(outer_plan != nullptr);

//     CMemoryPool *mp = this->memory_pool;

//     LogicalPlan *qg_plan = nullptr;  // start from nowhere, global subquery plan
//     GPOS_ASSERT(qgc.getNumQueryGraphs() > 0);

//     for (int idx = 0; idx < qgc.getNumQueryGraphs(); idx++) {
//         QueryGraph *qg = qgc.getQueryGraph(idx);

//         for (int edge_idx = 0; edge_idx < qg->getNumQueryRels(); edge_idx++) {
//             RelExpression *qedge = qg->getQueryRel(edge_idx).get();
//             NodeExpression *lhs = qedge->getSrcNode().get();
//             NodeExpression *rhs = qedge->getDstNode().get();
//             string edge_name = qedge->getUniqueName();
//             string lhs_name = qedge->getSrcNode()->getUniqueName();
//             string rhs_name = qedge->getDstNode()->getUniqueName();
//             bool is_pathjoin =
//                 qedge->getLowerBound() != 1 || qedge->getUpperBound() != 1;

//             bool is_lhs_bound = false;
//             bool is_rhs_bound = false;
//             bool is_lhs_bound_on_outer = false;
//             bool is_rhs_bound_on_outer = false;
//             if (qg_plan != nullptr) {
//                 is_lhs_bound =
//                     qg_plan->getSchema()->isNodeBound(lhs_name) ? true : false;
//                 is_rhs_bound =
//                     qg_plan->getSchema()->isNodeBound(rhs_name) ? true : false;
//             }
//             if (outer_plan) {
//                 // check bound with outer plan
//                 is_lhs_bound_on_outer =
//                     outer_plan->getSchema()->isNodeBound(lhs_name);
//                 is_lhs_bound = is_lhs_bound || is_lhs_bound_on_outer;
//                 is_rhs_bound_on_outer =
//                     outer_plan->getSchema()->isNodeBound(rhs_name);
//                 is_rhs_bound = is_rhs_bound || is_rhs_bound_on_outer;
//             }

//             LogicalPlan *hop_plan;  // constructed plan of a single hop
//             // Plan R
//             LogicalPlan *lhs_plan;
//             LogicalPlan *edge_plan;
//             LogicalPlan *rhs_plan;
//             if (!is_pathjoin) {
//                 edge_plan =
//                     lPlanNodeOrRelExpr((NodeOrRelExpression *)qedge);
//             }
//             else {
//                 edge_plan = lPlanPathGet((RelExpression *)qedge);
//             }
//             D_ASSERT(edge_plan != nullptr);

//             // Plan A - R
//             CExpression *a_r_join_expr;
//             if (!is_lhs_bound ||
//                 (is_lhs_bound &&
//                  !is_lhs_bound_on_outer)) {  // not bound or bound on inner
//                 // Join with R
//                 if (!is_lhs_bound) {
//                     lhs_plan =
//                         lPlanNodeOrRelExpr((NodeOrRelExpression *)lhs);
//                 }
//                 else {
//                     lhs_plan = qg_plan;
//                 }
//                 a_r_join_expr = lExprLogicalJoin(
//                     lhs_plan->getPlanExpr(), edge_plan->getPlanExpr(),
//                     lhs_plan->getSchema()->getColRefOfKey(lhs_name, ID_COLNAME_ID),
//                     edge_plan->getSchema()->getColRefOfKey(edge_name,
//                                                            SID_COLNAME_ID),
//                     gpopt::COperator::EOperatorId::EopLogicalInnerJoin,
//                     nullptr);
//                 lhs_plan->getSchema()->appendSchema(edge_plan->getSchema());
//                 lhs_plan->addBinaryParentOp(a_r_join_expr, edge_plan);
//             }
//             else {
//                 // No join, only filter with A
//                 lhs_plan = edge_plan;
//                 CExpression *selection_expr = CUtils::PexprLogicalSelect(
//                     mp, lhs_plan->getPlanExpr(),
//                     lExprScalarCmpEq(lExprScalarPropertyExpr(
//                                          lhs_name, ID_COLNAME_ID, outer_plan),
//                                      lExprScalarPropertyExpr(
//                                          edge_name, SID_COLNAME_ID, lhs_plan)));
//                 lhs_plan->addUnaryParentOp(selection_expr);
//             }
//             D_ASSERT(lhs_plan != nullptr);

//             // Plan R - B
//             if (is_lhs_bound && is_rhs_bound && is_rhs_bound_on_outer) {
//                 // no join necessary - add filter predicate on edge.tid = rhs.id
//                 CMemoryPool *mp = this->memory_pool;
//                 hop_plan = lhs_plan;
//                 CExpression *right_pred =
//                     is_rhs_bound_on_outer
//                         ? lExprScalarPropertyExpr(rhs_name, ID_COLNAME_ID,
//                                                   outer_plan)
//                         : lExprScalarPropertyExpr(rhs_name, ID_COLNAME_ID,
//                                                   hop_plan);
//                 CExpression *selection_expr = CUtils::PexprLogicalSelect(
//                     mp, lhs_plan->getPlanExpr(),
//                     lExprScalarCmpEq(lExprScalarPropertyExpr(
//                                          edge_name, TID_COLNAME_ID, lhs_plan),
//                                      right_pred));
//                 hop_plan->addUnaryParentOp(selection_expr);
//             }
//             else if (!is_rhs_bound ||
//                      (is_rhs_bound && !is_rhs_bound_on_outer)) {
//                 // join case
//                 if (!is_rhs_bound) {
//                     rhs_plan =
//                         lPlanNodeOrRelExpr((NodeOrRelExpression *)rhs);
//                 }
//                 else {
//                     rhs_plan = qg_plan;
//                 }
//                 auto join_expr = lExprLogicalJoin(
//                     lhs_plan->getPlanExpr(), rhs_plan->getPlanExpr(),
//                     lhs_plan->getSchema()->getColRefOfKey(edge_name,
//                                                           TID_COLNAME_ID),
//                     rhs_plan->getSchema()->getColRefOfKey(rhs_name, ID_COLNAME_ID),
//                     gpopt::COperator::EOperatorId::EopLogicalInnerJoin,
//                     nullptr);
//                 lhs_plan->getSchema()->appendSchema(rhs_plan->getSchema());
//                 lhs_plan->addBinaryParentOp(join_expr, rhs_plan);
//                 hop_plan = lhs_plan;
//             }
//             else {
//                 D_ASSERT(is_rhs_bound_on_outer);
//                 hop_plan = lhs_plan;
//                 CExpression *selection_expr = CUtils::PexprLogicalSelect(
//                     mp, hop_plan->getPlanExpr(),
//                     lExprScalarCmpEq(lExprScalarPropertyExpr(
//                                          rhs_name, ID_COLNAME_ID, outer_plan),
//                                      lExprScalarPropertyExpr(
//                                          edge_name, TID_COLNAME_ID, hop_plan)));
//                 hop_plan->addUnaryParentOp(selection_expr);
//             }
//             GPOS_ASSERT(hop_plan != nullptr);
//             // When lhs, rhs is unbound, qg_plan is not merged with the hop_plan. Thus cartprod.
//             if ((qg_plan != nullptr) && (!is_lhs_bound) && (!is_rhs_bound)) {
//                 auto cart_expr = lExprLogicalCartProd(qg_plan->getPlanExpr(),
//                                                       hop_plan->getPlanExpr());
//                 qg_plan->getSchema()->appendSchema(hop_plan->getSchema());
//                 qg_plan->addBinaryParentOp(cart_expr, hop_plan);
//             }
//             else {
//                 qg_plan = hop_plan;
//             }
//             GPOS_ASSERT(qg_plan != nullptr);
//         }
//         // if no edge, this is single node scan case
//         if (qg->getQueryNodes().size() == 1) {
//             auto node = (NodeOrRelExpression *)qg->getQueryNodes()[0].get();
//             string node_name = node->getUniqueName();
//             if (outer_plan->getSchema()->isNodeBound(node_name)) {
//                 D_ASSERT(
//                     false);  // Not sure about the logic here. different from SQL
//             }
//             LogicalPlan *nodescan_plan = lPlanNodeOrRelExpr(
//                 (NodeOrRelExpression *)qg->getQueryNodes()[0].get());
//             if (qg_plan == nullptr) {
//                 qg_plan = nodescan_plan;
//             }
//             else {
//                 // cartprod
//                 auto cart_expr = lExprLogicalCartProd(
//                     qg_plan->getPlanExpr(), nodescan_plan->getPlanExpr());
//                 qg_plan->getSchema()->appendSchema(nodescan_plan->getSchema());
//                 qg_plan->addBinaryParentOp(cart_expr, nodescan_plan);
//             }
//         }
//     }
//     GPOS_ASSERT(qg_plan != nullptr);

//     return qg_plan;
// }

// LogicalPlan *Planner::lPlanSelection(const expression_vector &predicates,
//                                      LogicalPlan *prev_plan)
// {

//     CMemoryPool *mp = this->memory_pool;
//     // the predicates are in CNF form

//     CExpressionArray *cnf_exprs = GPOS_NEW(mp) CExpressionArray(mp);
//     for (auto &pred : predicates) {
//         cnf_exprs->Append(lExprScalarExpression(pred.get(), prev_plan));
//     }

//     // orca supports N-ary AND
//     CExpression *pred_expr;
//     if (cnf_exprs->Size() > 1) {
//         pred_expr = CUtils::PexprScalarBoolOp(
//             mp, CScalarBoolOp::EBoolOperator::EboolopAnd, cnf_exprs);
//     }
//     else {
//         pred_expr = cnf_exprs->operator[](0);
//     }

//     CExpression *selection_expr =
//         CUtils::PexprLogicalSelect(mp, prev_plan->getPlanExpr(), pred_expr);
//     prev_plan->addUnaryParentOp(selection_expr);

//     // schema is never changed
//     return prev_plan;
// }

// LogicalPlan *Planner::lPlanProjection(const expression_vector &expressions,
//                                       LogicalPlan *prev_plan)
// {

//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

//     CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
//     CColRefArray *colrefs = GPOS_NEW(mp) CColRefArray(mp);
//     LogicalSchema new_schema;

//     for (auto &proj_expr_ptr : expressions) {
//         vector<CExpression *> generated_exprs;
//         vector<CColRef *> generated_colrefs;
//         kuzu::binder::Expression *proj_expr = proj_expr_ptr.get();
//         if (proj_expr->expressionType !=
//             kuzu::common::ExpressionType::VARIABLE) {
//             // Handle pruned case for DSI (TODO: Adopt more good design)
//             // For example. MATCH (n) WHERE n.type = "A" RETURN n
//             // n should not contain properties from the removed tables
//             if (proj_expr->expressionType == PROPERTY) {
//                 auto prop_key_id = ((PropertyExpression *)proj_expr)->getPropertyID();
//                 if (std::find(pruned_key_ids.begin(), pruned_key_ids.end(), prop_key_id) != pruned_key_ids.end()) {
//                     continue;
//                 }
//             }

//             // Process expr
//             CExpression *expr = lExprScalarExpression(proj_expr, prev_plan);
//             D_ASSERT(expr->Pop()->FScalar());
//             string col_name = proj_expr->hasAlias()
//                                   ? proj_expr->getAlias()
//                                   : proj_expr->getUniqueName();
//             if (CUtils::FScalarIdent(expr)) {
//                 // reuse colref
//                 CColRef *orig_colref = col_factory->LookupColRef(
//                     ((CScalarIdent *)(expr->Pop()))->Pcr()->Id());
//                 orig_colref->MarkAsUsed();  // TODO correctness check @jhha
//                 // also mark the previous one
//                 CColRef *prev_colref = col_factory->LookupColRef(
//                     ((CScalarIdent *)(expr->Pop()))->Pcr()->PrevId());
//                 prev_colref->MarkAsUsed();  // TODO correctness check @jhha
//                 generated_colrefs.push_back(orig_colref);
//                 if (proj_expr->expressionType ==
//                     kuzu::common::ExpressionType::PROPERTY) {
//                     // considered as property only when users can still access as node property.
//                     // otherwise considered as general column
//                     PropertyExpression *prop_expr =
//                         (PropertyExpression *)proj_expr;
//                     if (prev_plan->getSchema()->isNodeBound(
//                             prop_expr->getVariableName())) {
//                         new_schema.appendNodeProperty(
//                             prop_expr->getVariableName(),
//                             prop_expr->getPropertyID(), orig_colref);
//                     }
//                     else {
//                         new_schema.appendEdgeProperty(
//                             prop_expr->getVariableName(),
//                             prop_expr->getPropertyID(), orig_colref);
//                     }
//                 }
//                 else {
//                     new_schema.appendColumn(col_name, generated_colrefs.back());
//                 }
//             }
//             else {
//                 // get new colref
//                 CScalar *scalar_op = (CScalar *)(expr->Pop());
//                 std::wstring w_col_name = L"";
//                 w_col_name.assign(col_name.begin(), col_name.end());
//                 const CWStringConst col_name_str(w_col_name.c_str());
//                 CName col_cname(&col_name_str);
//                 CColRef *new_colref = col_factory->PcrCreate(
//                     lGetMDAccessor()->RetrieveType(scalar_op->MdidType()),
//                     scalar_op->TypeModifier(), col_cname);
//                 new_colref->MarkAsUsed();  // TODO correctness check @jhha
//                 generated_colrefs.push_back(new_colref);
//                 new_schema.appendColumn(col_name, generated_colrefs.back());
//             }
//             generated_exprs.push_back(expr);
//             // add new property
//         }
//         else {
//             // Handle kuzu::common::ExpressionType::VARIABLE here.
//             kuzu::binder::NodeOrRelExpression *var_expr =
//                 (kuzu::binder::NodeOrRelExpression *)(proj_expr);
//             auto var_colrefs = prev_plan->getSchema()->getAllColRefsOfKey(
//                 var_expr->getUniqueName());
//             for (auto &colref : var_colrefs) {
//                 generated_colrefs.push_back(colref);
//                 generated_exprs.push_back(lExprScalarPropertyExpr(
//                     var_expr->getUniqueName(),
//                     prev_plan->getSchema()->getPropertyNameOfColRef(
//                         var_expr->getUniqueName(), colref),
//                     prev_plan));
//                 // TODO aliasing???
//             }
//             if (var_expr->getDataType().typeID == DataTypeID::NODE) {
//                 new_schema.copyNodeFrom(prev_plan->getSchema(),
//                                         var_expr->getUniqueName());
//             }
//             else {  // rel
//                 new_schema.copyEdgeFrom(prev_plan->getSchema(),
//                                         var_expr->getUniqueName());
//             }
//         }
//         D_ASSERT(generated_exprs.size() > 0 &&
//                  generated_exprs.size() == generated_colrefs.size());
//         for (int expr_idx = 0; expr_idx < generated_exprs.size(); expr_idx++) {
//             CExpression *scalar_proj_elem = GPOS_NEW(mp) CExpression(
//                 mp,
//                 GPOS_NEW(mp)
//                     CScalarProjectElement(mp, generated_colrefs[expr_idx]),
//                 generated_exprs[expr_idx]);
//             proj_array->Append(scalar_proj_elem);
//         }
//     }

//     // Our columnar projection
//     CExpression *pexprPrjList = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
//     CExpression *proj_expr =
//         GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp),
//                                  prev_plan->getPlanExpr(), pexprPrjList);

//     prev_plan->addUnaryParentOp(proj_expr);
//     prev_plan->setSchema(move(new_schema));

//     return prev_plan;
// }

// LogicalPlan *Planner::lPlanGroupBy(const expression_vector &expressions,
//                                    LogicalPlan *prev_plan)
// {
//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

//     CExpressionArray *agg_columns = GPOS_NEW(mp) CExpressionArray(mp);
//     CColRefArray *key_columns = GPOS_NEW(mp) CColRefArray(mp);
//     agg_columns->AddRef();
//     key_columns->AddRef();

//     LogicalSchema new_schema;

//     for (auto &proj_expr_ptr : expressions) {
//         kuzu::binder::Expression *proj_expr = proj_expr_ptr.get();
//         string col_name = proj_expr->getUniqueName();
//         string col_name_print = proj_expr->hasAlias()
//                                     ? proj_expr->getAlias()
//                                     : proj_expr->getUniqueName();

//         if (proj_expr->hasAggregationExpression() && !proj_expr->is_processed) {
//             // AGG COLUMNS
//             CExpression *expr = lExprScalarExpression(proj_expr, prev_plan);

//             // get new colref and add to schema
//             CScalar *scalar_op = (CScalar *)(expr->Pop());
//             std::wstring w_col_name = L"";
//             w_col_name.assign(col_name_print.begin(), col_name_print.end());
//             const CWStringConst col_name_str(w_col_name.c_str());
//             CName col_cname(&col_name_str);
//             /**
// 			 * Basically, this code is wrong.
// 			 * We should create CColRef based on 'our' storage info.
// 			 * This code only uses Orca info, which ends error.
// 			 * Just do bind, and found return type.
// 			*/
//             CColRef *new_colref = col_factory->PcrCreate(
//                 lGetMDAccessor()->RetrieveType(scalar_op->MdidType()),
//                 scalar_op->TypeModifier(), col_cname);      // col_name_print
//             new_schema.appendColumn(col_name, new_colref);  // col_name

//             // add to agg_columns
//             auto *proj_elem = GPOS_NEW(mp) CExpression(
//                 mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref),
//                 expr);  // one proj_elem refers to one aggregated value ()
//             agg_columns->Append(proj_elem);
//             proj_expr->is_processed = true;
//         }
//         else {
//             // KEY COLUMNS
//             if (proj_expr->expressionType ==
//                 kuzu::common::ExpressionType::PROPERTY) {
//                 CExpression *expr = lExprScalarExpression(proj_expr, prev_plan);
//                 // add original colref to schema
//                 CColRef *orig_colref = col_factory->LookupColRef(
//                     ((CScalarIdent *)(expr->Pop()))->Pcr()->Id());
//                 if (proj_expr->expressionType ==
//                         kuzu::common::ExpressionType::PROPERTY &&
//                     !proj_expr->hasAlias()) {
//                     // considered as property only when users can still access as node property.
//                     // otherwise considered as general column
//                     PropertyExpression *prop_expr =
//                         (PropertyExpression *)proj_expr;
//                     if (prev_plan->getSchema()->isNodeBound(
//                             prop_expr->getVariableName())) {
//                         new_schema.appendNodeProperty(
//                             prop_expr->getVariableName(),
//                             prop_expr->getPropertyID(), orig_colref);
//                     }
//                     else {
//                         new_schema.appendEdgeProperty(
//                             prop_expr->getVariableName(),
//                             prop_expr->getPropertyID(), orig_colref);
//                     }
//                 }
//                 else {
//                     // handle as column
//                     new_schema.appendColumn(col_name, orig_colref);
//                 }
//                 // add to key_columns
//                 key_columns->Append(orig_colref);
//             }
//             else if (proj_expr->expressionType ==
//                      kuzu::common::ExpressionType::VARIABLE) {
//                 // e.g. WITH person, AGG(...), ...
//                 kuzu::binder::NodeOrRelExpression *var_expr =
//                     (kuzu::binder::NodeOrRelExpression *)(proj_expr);
//                 auto property_colrefs =
//                     std::move(prev_plan->getSchema()->getAllColRefsOfKey(
//                         proj_expr->getUniqueName()));
//                 for (auto &col : property_colrefs) {
//                     // consider all columns as key columns TODO this is inefficient
//                     key_columns->Append(col);
//                 }

//                 if (var_expr->getDataType().typeID == DataTypeID::NODE) {
//                     new_schema.copyNodeFrom(prev_plan->getSchema(),
//                                             var_expr->getUniqueName());
//                 }
//                 else {  // rel
//                     new_schema.copyEdgeFrom(prev_plan->getSchema(),
//                                             var_expr->getUniqueName());
//                 }
//             }
//             else if (proj_expr->expressionType ==
//                          kuzu::common::ExpressionType::FUNCTION ||
//                      proj_expr->expressionType ==
//                          kuzu::common::ExpressionType::AGGREGATE_FUNCTION) {
//                 // e.g. substring(), ...
//                 vector<CColRef *> property_columns;
//                 property_columns =
//                     std::move(prev_plan->getSchema()->getAllColRefsOfKey(
//                         proj_expr->getRawName()));
//                 if (property_columns.size() == 0) {
//                     property_columns =
//                         std::move(prev_plan->getSchema()->getAllColRefsOfKey(
//                             proj_expr->getUniqueName()));
//                 }
//                 if (property_columns.size() == 0) {
//                     CExpression *expr =
//                         lExprScalarExpression(proj_expr, prev_plan);
//                     string col_name = proj_expr->hasAlias()
//                                           ? proj_expr->getAlias()
//                                           : proj_expr->getUniqueName();
//                     // get new colref
//                     CScalar *scalar_op = (CScalar *)(expr->Pop());
//                     std::wstring w_col_name = L"";
//                     w_col_name.assign(col_name.begin(), col_name.end());
//                     const CWStringConst col_name_str(w_col_name.c_str());
//                     CName col_cname(&col_name_str);
//                     CColRef *new_colref = col_factory->PcrCreate(
//                         lGetMDAccessor()->RetrieveType(scalar_op->MdidType()),
//                         scalar_op->TypeModifier(), col_cname);
//                     key_columns->Append(new_colref);
//                     // generated_colrefs.push_back(new_colref);
//                     new_schema.appendColumn(col_name, new_colref);
//                 }
//                 else {
//                     for (auto &col : property_columns) {
//                         // consider all columns as key columns
//                         // TODO this is inefficient
//                         key_columns->Append(col);
//                         if (proj_expr->hasAlias()) {
//                             new_schema.appendColumn(col_name, col);
//                         }
//                         else {
//                             if (prev_plan->getSchema()->isNodeBound(
//                                     proj_expr->getUniqueName())) {
//                                 // consider as node
//                                 new_schema.appendNodeProperty(
//                                     proj_expr->getUniqueName(),
//                                     prev_plan->getSchema()
//                                         ->getPropertyNameOfColRef(
//                                             proj_expr->getUniqueName(), col),
//                                     col);
//                             }
//                             else {
//                                 // considera as edge
//                                 new_schema.appendEdgeProperty(
//                                     proj_expr->getUniqueName(),
//                                     prev_plan->getSchema()
//                                         ->getPropertyNameOfColRef(
//                                             proj_expr->getUniqueName(), col),
//                                     col);
//                             }
//                         }
//                     }
//                 }
//             }
//             else {
//                 D_ASSERT(false);  // not implemented yet
//             }
//         }
//     }

//     CExpression *pexprList = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), agg_columns);
//     CExpression *agg_expr = CUtils::PexprLogicalGbAggGlobal(
//         mp, key_columns, prev_plan->getPlanExpr(), pexprList);

//     prev_plan->addUnaryParentOp(agg_expr);
//     prev_plan->setSchema(move(new_schema));

//     return prev_plan;
// }

// LogicalPlan *Planner::lPlanOrderBy(const expression_vector &orderby_exprs,
//                                    const vector<bool> sort_orders,
//                                    LogicalPlan *prev_plan)
// {
//     CMemoryPool *mp = this->memory_pool;

//     D_ASSERT(orderby_exprs.size() == sort_orders.size());

//     vector<CColRef *> sort_colrefs;
//     for (auto &orderby_expr : orderby_exprs) {
//         auto &orderby_expr_type = orderby_expr.get()->expressionType;
//         if (orderby_expr_type == kuzu::common::ExpressionType::PROPERTY) {
//             // first depth projection = simple projection
//             PropertyExpression *prop_expr =
//                 (PropertyExpression *)(orderby_expr.get());
//             string k1 = prop_expr->getVariableName();
//             uint64_t k2 = prop_expr->getPropertyID();
//             CColRef *key_colref =
//                 prev_plan->getSchema()->getColRefOfKey(k1, k2);
//             // fallback to alias
//             if (key_colref == NULL && prop_expr->hasAlias()) {
//                 k1 = prop_expr->getAlias();
//                 k2 = std::numeric_limits<uint64_t>::max();
//                 key_colref = prev_plan->getSchema()->getColRefOfKey(k1, k2);
//             }
//             D_ASSERT(key_colref != NULL);
//             sort_colrefs.push_back(key_colref);
//         }
//         else {
//             CColRef *key_colref = prev_plan->getSchema()->getColRefOfKey(
//                 orderby_expr->getUniqueName(), std::numeric_limits<uint64_t>::max());
//             // fallback to alias
//             if (key_colref == NULL && orderby_expr->hasAlias()) {
//                 key_colref = prev_plan->getSchema()->getColRefOfKey(
//                     orderby_expr->getAlias(), std::numeric_limits<uint64_t>::max());
//             }
//             D_ASSERT(key_colref != NULL);
//             sort_colrefs.push_back(key_colref);
//         }
//     }

//     COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);

//     for (uint64_t i = 0; i < sort_colrefs.size(); i++) {
//         CColRef *colref = sort_colrefs[i];

//         IMDType::ECmpType sort_type =
//             sort_orders[i] == true ? IMDType::EcmptL
//                                    : IMDType::EcmptG;  // TODO not sure...
//         auto x = colref->RetrieveType();
//         CMDIdGPDB *x_id = CMDIdGPDB::CastMdid(x->MDId());
//         IMDId *mdid = colref->RetrieveType()->GetMdidForCmpType(sort_type);
//         pos->Append(mdid, colref, COrderSpec::EntLast);
//     }

//     CLogicalLimit *popLimit = GPOS_NEW(mp)
//         CLogicalLimit(mp, pos, true /* fGlobal */, false /* fHasCount */,
//                       true /*fTopLimitUnderDML*/);
//     CExpression *pexprLimitOffset =
//         CUtils::PexprScalarConstInt8(mp, 0 /*ulOffSet*/);
//     CExpression *pexprLimitCount =
//         CUtils::PexprScalarConstInt8(mp, 0 /*count*/, true /*is_null*/);

//     CExpression *plan_orderby_expr =
//         GPOS_NEW(mp) CExpression(mp, popLimit, prev_plan->getPlanExpr(),
//                                  pexprLimitOffset, pexprLimitCount);

//     prev_plan->addUnaryParentOp(plan_orderby_expr);  // TODO ternary op?..
//     return prev_plan;
// }

// LogicalPlan *Planner::lPlanDistinct(const expression_vector &expressions,
//                                     CColRefArray *colrefs,
//                                     LogicalPlan *prev_plan)
// {
//     // TODO bug..
//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
//     CColRefArray *key_columns = GPOS_NEW(mp) CColRefArray(mp);
//     CColRefArray *minimal_key_columns = GPOS_NEW(mp) CColRefArray(mp);
//     key_columns->AddRef();
//     minimal_key_columns->AddRef();

//     for (auto &proj_expr_ptr : expressions) {
//         kuzu::binder::Expression *proj_expr = proj_expr_ptr.get();
//         string col_name = proj_expr->getUniqueName();
//         string col_name_print = proj_expr->hasAlias()
//                                     ? proj_expr->getAlias()
//                                     : proj_expr->getUniqueName();
//         if (proj_expr->hasAggregationExpression()) {
//             D_ASSERT(false);  // TODO not implemented yet
//         }
//         else {
//             if (proj_expr->expressionType ==
//                 kuzu::common::ExpressionType::PROPERTY) {
//                 proj_expr->is_processed = true;
//                 CExpression *expr = lExprScalarExpression(proj_expr, prev_plan);
//                 // add original colref to schema
//                 CColRef *orig_colref = col_factory->LookupColRef(
//                     ((CScalarIdent *)(expr->Pop()))->Pcr()->Id());
//                 // add to key_columns
//                 key_columns->Append(orig_colref);
//                 minimal_key_columns->Append(orig_colref);
//             }
//             else if (proj_expr->expressionType ==
//                      kuzu::common::ExpressionType::VARIABLE) {
//                 proj_expr->is_processed = true;
//                 kuzu::binder::NodeOrRelExpression *var_expr =
//                     (kuzu::binder::NodeOrRelExpression *)(proj_expr);
//                 auto property_colrefs =
//                     std::move(prev_plan->getSchema()->getAllColRefsOfKey(
//                         proj_expr->getUniqueName()));
//                 minimal_key_columns->Append(property_colrefs[0]);
//                 for (auto &col : property_colrefs) {
//                     key_columns->Append(col);
//                 }
//             }
//             else if (proj_expr->expressionType ==
//                      kuzu::common::ExpressionType::FUNCTION) {
//                 D_ASSERT(false);  // TODO not implemented yet
//             }
//             else {
//                 D_ASSERT(false);  // TODO not implemented yet
//             }
//         }
//     }

//     // CLogicalGbAgg *pop_gbagg =
//     // 	GPOS_NEW(mp) CLogicalGbAgg(
//     // 		mp, key_columns, minimal_key_columns, COperator::EgbaggtypeGlobal /*egbaggtype*/);
//     CLogicalGbAgg *pop_gbagg = GPOS_NEW(mp) CLogicalGbAgg(
//         mp, key_columns, COperator::EgbaggtypeGlobal /*egbaggtype*/);
//     CExpression *gbagg_expr = GPOS_NEW(mp) CExpression(
//         mp, pop_gbagg, prev_plan->getPlanExpr(),
//         GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp)));
//     // colrefs->AddRef();

//     prev_plan->addUnaryParentOp(gbagg_expr);

//     return prev_plan;
// }

// LogicalPlan *Planner::lPlanPathGet(RelExpression *edge_expr)
// {
//     CMemoryPool *mp = this->memory_pool;

//     auto table_oids = edge_expr->getTableIDs();

//     duckdb::Catalog &catalog = context->db->GetCatalog();
//     duckdb::GraphCatalogEntry *graph_catalog_entry =
//         (duckdb::GraphCatalogEntry *)catalog.GetEntry(
//             *context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
//             DEFAULT_GRAPH);

//     // generate three columns, based on the first table
//     CColRefArray *cols;
//     LogicalSchema schema;
//     auto edge_name = edge_expr->getUniqueName();
//     auto edge_name_expr = edge_expr->getUniqueName();

//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
//     CColRefArray *path_output_cols = GPOS_NEW(mp) CColRefArray(mp);

//     // tabledescs for underlying table
//     CTableDescriptorArray *path_table_descs =
//         GPOS_NEW(mp) CTableDescriptorArray(mp);
//     path_table_descs->AddRef();
//     for (auto &obj_id : table_oids) {
//         path_table_descs->Append(
//             lCreateTableDescForRel(lGenRelMdid(obj_id), edge_name));
//     }
//     D_ASSERT(path_table_descs->Size() == table_oids.size());

//     CColumnDescriptorArray *pdrgpcoldesc =
//         path_table_descs->operator[](0)
//             ->Pdrgpcoldesc();  // TODO need to change for Union All case
//     IMDId *mdid_table = path_table_descs->operator[](0)
//                             ->MDId();  // TODO need to change for Union All case
//     auto &prop_exprs = edge_expr->getPropertyExpressions();
//     D_ASSERT(pdrgpcoldesc->Size() >= prop_exprs.size());
//     for (int colidx = 0; colidx < prop_exprs.size(); colidx++) {
//         auto &_prop_expr = prop_exprs[colidx];
//         PropertyExpression *expr =
//             static_cast<PropertyExpression *>(_prop_expr.get());

//         // if property name in _id, _sid, _tid
//         property_id_t propertyID = expr->getPropertyID();
//         string property_name = graph_catalog_entry->GetPropertyName(*context, propertyID);
//         if (property_name == "_id" ||
//             property_name == "_sid" ||
//             property_name == "_tid") {
//             gpmd::IMDId *col_type_imdid =
//                 lGetRelMd(table_oids[0])
//                     ->GetMdCol(expr->getPropertyID(table_oids[0]))
//                     ->MdidType();
//             gpos::INT col_type_modifier =
//                 lGetRelMd(table_oids[0])
//                     ->GetMdCol(expr->getPropertyID(table_oids[0]))
//                     ->TypeModifier();

//             // std::string property_name = expr->getPropertyName();   
//             std::wstring property_print_name = L"";
//             property_print_name.assign(property_name.begin(),
//                                        property_name.end());
//             const CWStringConst col_name_str(property_print_name.c_str());
//             CName col_name(&col_name_str);

//             CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[colidx];
//             // generate colref
//             // CColRef *new_colref = col_factory->PcrCreate(
//             // 	lGetMDAccessor()->RetrieveType(col_type_imdid), col_type_modifier, col_name);
//             CColRef *new_colref = col_factory->PcrCreate(
//                 pcoldesc, col_name, 0, false /* mark_as_used */,
//                 mdid_table);  // TODO ulOpSourceId?
//             path_output_cols->Append(new_colref);

//             // add to schema
//             schema.appendEdgeProperty(edge_name, propertyID, new_colref);
//         }
//     }
//     D_ASSERT(schema.getNumPropertiesOfKey(edge_name) == 3);
//     D_ASSERT(path_output_cols->Size() == 3);

//     // generate get expression
//     std::wstring w_alias = L"";
//     w_alias.assign(edge_name.begin(), edge_name.end());
//     const CWStringConst path_name_str(w_alias.c_str());

//     CExpression *path_get_expr = NULL;
//     CLogicalPathGet *pop = GPOS_NEW(mp) CLogicalPathGet(
//         mp, GPOS_NEW(mp) CName(mp, CName(&path_name_str)), path_table_descs,
//         path_output_cols, (gpos::INT)edge_expr->getLowerBound(),
//         (gpos::INT)edge_expr->getUpperBound());
//     path_get_expr = GPOS_NEW(mp) CExpression(mp, pop);

//     path_output_cols->AddRef();

//     LogicalPlan *plan = new LogicalPlan(path_get_expr, schema);
//     GPOS_ASSERT(!plan->getSchema()->isEmpty());
//     return plan;
// }

// LogicalPlan *Planner::lPlanSkipOrLimit(BoundProjectionBody *proj_body,
//                                        LogicalPlan *prev_plan)
// {
//     CMemoryPool *mp = this->memory_pool;
//     COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
//     bool hasCount = proj_body->hasLimit();
//     CLogicalLimit *popLimit = GPOS_NEW(mp)
//         CLogicalLimit(mp, pos, true /* fGlobal */, hasCount /* fHasCount */,
//                       true /*fTopLimitUnderDML*/);
//     CExpression *pexprLimitOffset, *pexprLimitCount;

//     if (proj_body->hasSkip()) {
//         pexprLimitOffset = CUtils::PexprScalarConstInt8(
//             mp, proj_body->getSkipNumber() /*ulOffSet*/);
//     }
//     else {
//         pexprLimitOffset = CUtils::PexprScalarConstInt8(mp, 0 /*ulOffSet*/);
//     }

//     if (proj_body->hasLimit()) {
//         pexprLimitCount = CUtils::PexprScalarConstInt8(
//             mp, proj_body->getLimitNumber() /*count*/, false /*is_null*/);
//     }
//     else {
//         pexprLimitCount =
//             CUtils::PexprScalarConstInt8(mp, 0 /*count*/, true /*is_null*/);
//     }

//     CExpression *plan_orderby_expr =
//         GPOS_NEW(mp) CExpression(mp, popLimit, prev_plan->getPlanExpr(),
//                                  pexprLimitOffset, pexprLimitCount);

//     prev_plan->addUnaryParentOp(plan_orderby_expr);  // TODO ternary op?..
//     return prev_plan;
// }

// LogicalPlan *Planner::lPlanNodeOrRelExpr(NodeOrRelExpression *node_rel_expr)
// {
//     CMemoryPool *mp = this->memory_pool;
//     auto &table_oids = node_rel_expr->getTableIDs();
//     auto &prop_exprs = node_rel_expr->getPropertyExpressions();
//     auto &num_tables_per_part = node_rel_expr->getNumTablesPerPartition();
//     GPOS_ASSERT(table_oids.size() >= 1);
//     GPOS_ASSERT(prop_exprs.size() >= 1);
//     GPOS_ASSERT(num_tables_per_part.size() >= 1);

//     std::vector<uint64_t> repr_oids;
//     CExpressionArray *coalesced_gets = GPOS_NEW(mp) CExpressionArray(mp);
//     for (auto i = 0; i < num_tables_per_part.size(); i++) {
//         std::vector<uint64_t> table_oids_in_part(
//             table_oids.begin() + std::accumulate(num_tables_per_part.begin(), num_tables_per_part.begin() + i, 0),
//             table_oids.begin() + std::accumulate(num_tables_per_part.begin(), num_tables_per_part.begin() + i + 1, 0)
//         );

//         uint64_t repr_oid;
//         coalesced_gets->Append(lGetCoalescedGet(node_rel_expr, prop_exprs, table_oids_in_part, repr_oid));
//         repr_oids.push_back(repr_oid);
//     }

//     // Create UNION All of coalesced gets
//     map<uint64_t, map<uint64_t, uint64_t>> schema_proj_mapping;
//     vector<int> used_col_idx;
//     lBuildSchemaProjectionMapping(repr_oids, node_rel_expr, prop_exprs,
//                                   schema_proj_mapping, used_col_idx);

//     auto planned_expr = std::move(lExprLogicalGetNodeOrEdge(
//         node_rel_expr->resolveName(), repr_oids, nullptr, used_col_idx,
//         &schema_proj_mapping, true, node_rel_expr->isWholeNodeRequired()));
//     CExpression *plan_expr = planned_expr.first;
//     D_ASSERT(used_col_idx.size() == planned_expr.second->Size());

//     LogicalSchema schema;
//     lGenerateNodeOrEdgeSchema(node_rel_expr, prop_exprs, node_rel_expr->isNode(), used_col_idx,
//                               planned_expr.second, schema);

//     LogicalPlan *plan = new LogicalPlan(plan_expr, schema);
//     GPOS_ASSERT(!plan->getSchema()->isEmpty());
//     return plan;
// }

// CExpression *Planner::lGetCoalescedGet(NodeOrRelExpression *node_rel_expr,
//                                        const expression_vector &prop_exprs,
//                                        std::vector<uint64_t> &table_oids,
//                                        uint64_t &repr_oid)
// {
//     D_ASSERT(table_oids.size() > 0);
//     if (table_oids.size() == 1) {
//         repr_oid = table_oids[0];
//         return lExprLogicalGet(repr_oid, node_rel_expr->resolveName(), false,
//                                nullptr, node_rel_expr->isWholeNodeRequired());
//     }
//     else {
//         std::vector<uint64_t> property_location;
//         std::vector<uint64_t> prop_key_ids;
//         lGetUsedPropertyIDs(node_rel_expr, prop_exprs, prop_key_ids);
//         context->db->GetCatalogWrapper().CoalesceTablesOids(
//             *context, prop_key_ids, table_oids, provider, repr_oid,
//             property_location);
//         lUpdatePropExprsForNewTable(node_rel_expr, prop_exprs, repr_oid,
//                                     property_location);
//         return lExprLogicalGet(repr_oid, node_rel_expr->resolveName(), true,
//                                &table_oids,
//                                node_rel_expr->isWholeNodeRequired());
//     }
// }

// std::pair<CExpression *, CColRefArray *> Planner::lExprLogicalGetNodeOrEdge(
//     string name, vector<uint64_t> &relation_oids,
//     std::vector<std::vector<uint64_t>> *table_oids_in_groups,
//     vector<int> &used_col_idx,
//     map<uint64_t, map<uint64_t, uint64_t>> *schema_proj_mapping,
//     bool insert_projection, bool whole_node_required)
// {
//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

//     CExpression *union_plan = nullptr;
//     const bool do_schema_mapping = insert_projection;
//     GPOS_ASSERT(relation_oids.size() > 0);

//     // generate type infos to the projected schema
//     vector<pair<gpmd::IMDId *, gpos::INT>>
//         union_schema_types;  // mdid type and type modifier for both types
//     CColRefArray *union_output_array = GPOS_NEW(mp) CColRefArray(mp);

//     for (int i = 0; i < used_col_idx.size(); i++) {
//         int col_idx = used_col_idx[i];
//         // foreach mappings
//         uint64_t valid_oid;
//         uint64_t valid_cid = std::numeric_limits<uint64_t>::max();

//         for (auto &oid : relation_oids) {
//             uint64_t idx_to_try =
//                 (*schema_proj_mapping)[oid].find(col_idx)->second;
//             if (idx_to_try != std::numeric_limits<uint64_t>::max()) {
//                 valid_oid = oid;
//                 valid_cid = idx_to_try;
//                 break;
//             }
//         }
//         GPOS_ASSERT(valid_cid != std::numeric_limits<uint64_t>::max());
//         // extract info and maintain vector of column type infos
//         auto *mdcol = lGetRelMd(valid_oid)->GetMdCol(valid_cid);
//         gpmd::IMDId *col_type_imdid = mdcol->MdidType();
//         gpos::INT col_type_modifier = mdcol->TypeModifier();
//         union_schema_types.push_back(
//             make_pair(col_type_imdid, col_type_modifier));
//     }

//     CColRefArray *idx0_output_array;
//     CColRef2dArray *pdrgdrgpcrInput;
//     CExpressionArray *pdrgpexpr;
//     if (relation_oids.size() > 1) {
//         pdrgdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);
//         pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
//     }
//     for (int idx = 0; idx < relation_oids.size(); idx++) {
//         auto &oid = relation_oids[idx];

//         CExpression *expr;
//         const gpos::ULONG num_cols = lGetRelMd(oid)->ColumnCount();

//         GPOS_ASSERT(num_cols != 0);
//         expr = lExprLogicalGet(oid, name, table_oids_in_groups != nullptr,
//                                table_oids_in_groups == nullptr
//                                    ? nullptr
//                                    : &(*table_oids_in_groups)[idx],
//                                whole_node_required);

//         // conform schema if necessary
//         CColRefArray *output_array;
//         vector<uint64_t> project_col_ids;
//         if (do_schema_mapping) {
//             auto &mapping = (*schema_proj_mapping)[oid];
//             for (int i = 0; i < used_col_idx.size(); i++) {
//                 int proj_col_idx = used_col_idx[i];
//                 project_col_ids.push_back(mapping.find(proj_col_idx)->second);
//             }
//             GPOS_ASSERT(project_col_ids.size() > 0);
//             auto proj_result = lExprScalarAddSchemaConformProject(
//                 expr, project_col_ids, &union_schema_types);
//             expr = proj_result.first;
//             output_array = proj_result.second;
//         }
//         else {
//             // the output of logicalGet is always sorted, thus it is ok to use DeriveOutputColumns() here.
//             output_array = expr->DeriveOutputColumns()->Pdrgpcr(mp);
//         }

//         /* Single table might not have the identical column mapping with original table. Thus projection is required */
//         if (relation_oids.size() == 1) {
//             // REL
//             union_plan = expr;
//             idx0_output_array = output_array;
//         }
//         else {
//             // REL/UNION + REL
//             // As the result of Union ALL keeps the colrefs of LHS, we always set lhs array as idx0_output_array
//             if (idx == 0) {
//                 idx0_output_array = output_array;
//                 pdrgdrgpcrInput->Append(output_array);
//             }
//             else {
//                 pdrgdrgpcrInput->Append(output_array);
//             }
//             pdrgpexpr->Append(expr);
//         }
//     }
//     // union_output_array->AddRef();

//     if (relation_oids.size() > 1) {
//         union_plan = GPOS_NEW(mp) CExpression(
//             mp,
//             GPOS_NEW(mp)
//                 CLogicalUnionAll(mp, idx0_output_array, pdrgdrgpcrInput),
//             pdrgpexpr);
//     }

//     return make_pair(union_plan, idx0_output_array);
// }

// void Planner::lBuildSchemaProjectionMapping(
//     std::vector<uint64_t> &table_oids, NodeOrRelExpression *node_expr,
//     const expression_vector &prop_exprs,
//     map<uint64_t, map<uint64_t, uint64_t>> &schema_proj_mapping,
//     vector<int> &used_col_idx, bool is_dsi)
// {
//     for (auto &t_oid : table_oids) {
//         schema_proj_mapping.insert({t_oid, map<uint64_t, uint64_t>()});
//     }
//     GPOS_ASSERT(schema_proj_mapping.size() == table_oids.size());

//     // these properties include system columns (e.g. _id)
//     int tmp = 0;
//     used_col_idx.reserve(prop_exprs.size());
//     for (int col_idx = 0; col_idx < prop_exprs.size(); col_idx++) {
//         if (!node_expr->isUsedColumn(col_idx))
//             continue;
//         used_col_idx.push_back(col_idx);
//         auto &_prop_expr = prop_exprs[col_idx];
//         PropertyExpression *expr =
//             static_cast<PropertyExpression *>(_prop_expr.get());
//         for (auto &t_oid : table_oids) {
//             if (expr->isBelongToTable(t_oid)) {
//                 // table has property
//                 schema_proj_mapping.find(t_oid)->second.insert(
//                     {(uint64_t)col_idx,
//                      (uint64_t)(expr->getPropertyID(t_oid))});
//             }
//             else {
//                 // need to be projected as null column
//                 schema_proj_mapping.find(t_oid)->second.insert(
//                     {(uint64_t)col_idx, std::numeric_limits<uint64_t>::max()});
//             }
//         }
//     }
// }

// void Planner::lGenerateNodeOrEdgeSchema(NodeOrRelExpression *node_expr,
//                                         const expression_vector &prop_exprs,
//                                         bool is_node, vector<int> &used_col_idx,
//                                         CColRefArray *prop_colrefs,
//                                         LogicalSchema &schema)
// {
//     auto node_name = node_expr->getUniqueName();
//     for (int i = 0; i < used_col_idx.size(); i++) {
//         int col_idx = used_col_idx[i];
//         auto &_prop_expr = prop_exprs[col_idx];
//         PropertyExpression *expr =
//             static_cast<PropertyExpression *>(_prop_expr.get());
//         uint64_t propertyID = expr->getPropertyID();
//         if (is_node) {
//             schema.appendNodeProperty(node_name, propertyID,
//                                       prop_colrefs->operator[](i));
//         }
//         else {
//             schema.appendEdgeProperty(node_name, propertyID,
//                                       prop_colrefs->operator[](i));
//         }
//     }
//     GPOS_ASSERT(schema.getNumPropertiesOfKey(node_name) == used_col_idx.size());
// }

// CExpression *Planner::lExprLogicalGet(
//     uint64_t obj_id, string rel_name, bool is_instance,
//     std::vector<uint64_t> *table_oids_in_group, bool whole_node_required,
//     string alias)
// {
//     CMemoryPool *mp = this->memory_pool;

//     if (alias == "") {
//         alias = rel_name;
//     }

//     CTableDescriptor *ptabdesc =
//         lCreateTableDescForRel(lGenRelMdid(obj_id), rel_name);
//     ptabdesc->SetInstanceDescriptor(is_instance);

//     if (is_instance) {
//         GPOS_ASSERT(table_oids_in_group != nullptr);
//         for (ULONG ul = 0; ul < table_oids_in_group->size(); ul++) {
//             ptabdesc->AddTableInTheGroup(
//                 lGenRelMdid(table_oids_in_group->at(ul)));
//         }
//     }

//     std::wstring w_alias = L"";
//     w_alias.assign(alias.begin(), alias.end());
//     CWStringConst strAlias(w_alias.c_str());

//     CLogicalGet *pop = GPOS_NEW(mp)
//         CLogicalGet(mp, GPOS_NEW(mp) CName(mp, CName(&strAlias)), ptabdesc);

//     CExpression *scan_expr = GPOS_NEW(mp) CExpression(mp, pop);
//     CColRefArray *arr = pop->PdrgpcrOutput();
//     ULONG node_id = -1; // colref id of _id
//     if (whole_node_required) {
//         CColRef *pid_ref = (*arr)[0];
//         node_id = pid_ref->Id();
//     }
//     for (ULONG ul = 0; ul < arr->Size(); ul++) {
//         CColRef *ref = (*arr)[ul];
//         ref->MarkAsUnknown();
//         ref->SetNodeId(node_id);
//     }
//     return scan_expr;
// }

// CExpression *Planner::lExprLogicalUnionAllWithMapping(CExpression *lhs,
//                                                       CColRefArray *lhs_mapping,
//                                                       CExpression *rhs,
//                                                       CColRefArray *rhs_mapping)
// {
//     GPOS_ASSERT(rhs_mapping != nullptr);  // must be binary

//     CMemoryPool *mp = this->memory_pool;

//     CColRefArray *pdrgpcrOutput = GPOS_NEW(mp) CColRefArray(mp);
//     CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);

//     // output columns of the union
//     pdrgpcrOutput->AppendArray(lhs_mapping);
//     pdrgpcrOutput->AddRef();
//     pdrgdrgpcrInput->Append(lhs_mapping);
//     pdrgdrgpcrInput->Append(rhs_mapping);

//     // Binary Union ALL
//     return GPOS_NEW(mp) CExpression(
//         mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgdrgpcrInput),
//         lhs, rhs);
// }

// /*
//  * CExpression* returns result of projected schema.
// */
// std::pair<CExpression *, CColRefArray *>
// Planner::lExprScalarAddSchemaConformProject(
//     CExpression *relation, vector<uint64_t> &col_ids_to_project,
//     vector<pair<gpmd::IMDId *, gpos::INT>> *target_schema_types)
// {
//     // col_ids_to_project may include std::numeric_limits<uint64_t>::max(),
//     // which indicates null projection
//     CMemoryPool *mp = this->memory_pool;

//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
//     CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
//     CColRefArray *output_col_array = GPOS_NEW(mp) CColRefArray(mp);
//     output_col_array->AddRef();

//     uint64_t target_col_id = 0;
//     for (auto col_id : col_ids_to_project) {
//         CExpression *scalar_proj_elem;
//         if (col_id == std::numeric_limits<uint64_t>::max()) {
//             GPOS_ASSERT(target_schema_types != nullptr);
//             // project null column
//             auto &type_info = (*target_schema_types)[target_col_id];
//             CExpression *null_expr = CUtils::PexprScalarConstNull(
//                 mp, lGetMDAccessor()->RetrieveType(type_info.first),
//                 type_info.second);
//             const CWStringConst col_name_str(GPOS_WSZ_LIT("const_null"));
//             CName col_name(&col_name_str);
//             CColRef *new_colref = col_factory->PcrCreate(
//                 lGetMDAccessor()->RetrieveType(type_info.first),
//                 type_info.second, col_name);
//             scalar_proj_elem = GPOS_NEW(mp) CExpression(
//                 mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref),
//                 null_expr);

//             proj_array->Append(scalar_proj_elem);
//             output_col_array->Append(new_colref);
//         }
//         else {
//             // project non-null column
//             // the output of logicalGet is always sorted, thus it is ok to use DeriveOutputColumns() here.
//             CColRef *colref =
//                 relation->DeriveOutputColumns()->Pdrgpcr(mp)->operator[](
//                     col_id);
//             CColRef *new_colref;

//             // TODO 240116 tslee - this logic will not work when we have index for properties
//             if ((std::wcsstr(colref->Name().Pstr()->GetBuffer(), L"._id") !=
//                  0) ||
//                 (std::wcsstr(colref->Name().Pstr()->GetBuffer(), L"._sid") !=
//                  0) ||
//                 (std::wcsstr(colref->Name().Pstr()->GetBuffer(), L"._tid") !=
//                  0)) {
//                 CExpression *ident_expr = GPOS_NEW(mp)
//                     CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
//                 scalar_proj_elem = GPOS_NEW(mp) CExpression(
//                     mp, GPOS_NEW(mp) CScalarProjectElement(mp, colref),
//                     ident_expr);  // ident element do not assign new colref id

//                 // if we do not mark as used in this step, these columns never be marked as used
//                 // -> it makes indexonlyscan possible which will not occur
//                 colref->MarkAsUsed();
//                 proj_array->Append(scalar_proj_elem);
//                 output_col_array->Append(colref);
//             }
//             else {
//                 new_colref = col_factory->PcrCopy(colref);
//                 new_colref->SetPrevId(colref->Id());
                
//                 CExpression *ident_expr = GPOS_NEW(mp)
//                     CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref));
//                 scalar_proj_elem = GPOS_NEW(mp) CExpression(
//                     mp, GPOS_NEW(mp) CScalarProjectElement(mp, new_colref),
//                     ident_expr);  // ident element do not assign new colref id

//                 proj_array->Append(scalar_proj_elem);
//                 output_col_array->Append(new_colref);
//             }
//         }
//         target_col_id++;
//     }

//     // Our columnar projection
//     CExpression *pexprPrjList = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);
//     CExpression *proj_expr = GPOS_NEW(mp) CExpression(
//         mp, GPOS_NEW(mp) CLogicalProjectColumnar(mp), relation, pexprPrjList);

//     return make_pair(proj_expr, output_col_array);
// }

// CExpression *Planner::lExprLogicalJoin(CExpression *lhs, CExpression *rhs,
//                                        CColRef *lhs_colref, CColRef *rhs_colref,
//                                        gpopt::COperator::EOperatorId join_op,
//                                        CExpression *additional_join_pred)
// {

//     CMemoryPool *mp = this->memory_pool;

//     CColRef *pcrLeft = lhs_colref;
//     CColRef *pcrRight = rhs_colref;

//     lhs->AddRef();
//     rhs->AddRef();

//     CExpression *pexprEquality =
//         CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
//     if (additional_join_pred == nullptr) {
//         pexprEquality = CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
//     }
//     else {
//         CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
//         pdrgpexprChildren->Append(additional_join_pred);
//         pdrgpexprChildren->Append(
//             CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight));
//         pdrgpexprChildren->AddRef();
//         pexprEquality = CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EboolopAnd,
//                                                   pdrgpexprChildren);
//     }
//     pexprEquality->AddRef();

//     CExpression *join_result;
//     switch (join_op) {
//         case gpopt::COperator::EOperatorId::EopLogicalInnerJoin: {
//             join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
//                 mp, lhs, rhs, pexprEquality);
//             break;
//         }
//         case gpopt::COperator::EOperatorId::EopLogicalRightOuterJoin: {
//             join_result = CUtils::PexprLogicalJoin<CLogicalRightOuterJoin>(
//                 mp, lhs, rhs, pexprEquality);
//             break;
//         }
//         case gpopt::COperator::EOperatorId::EopLogicalLeftOuterJoin: {
//             join_result = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(
//                 mp, lhs, rhs, pexprEquality);
//             break;
//         }
//         default:
//             D_ASSERT(false);
//     }
//     D_ASSERT(join_result != NULL);

//     return join_result;
// }

// LogicalPlan *Planner::lPlanShortestPath(QueryGraph *qg, NodeExpression *lhs,
//                                         RelExpression *edge,
//                                         NodeExpression *rhs,
//                                         LogicalPlan *prev_plan)
// {
//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

//     LogicalSchema new_schema;
//     new_schema.appendSchema(prev_plan->getSchema());

//     string lhs_name = lhs->getUniqueName();
//     string rhs_name = rhs->getUniqueName();

//     CColRef *lhs_col_ref =
//         prev_plan->getSchema()->getColRefOfKey(lhs_name, 0);
//     CColRef *rhs_col_ref =
//         prev_plan->getSchema()->getColRefOfKey(rhs_name, 0);
//     CColRef *prev_lhs_col_ref =
//         col_factory->LookupColRef(lhs_col_ref->PrevId());
//     CColRef *prev_rhs_col_ref =
//         col_factory->LookupColRef(rhs_col_ref->PrevId());

//     // Marking
//     lhs_col_ref->MarkAsUsed();
//     rhs_col_ref->MarkAsUsed();
//     prev_lhs_col_ref->MarkAsUsed();
//     prev_rhs_col_ref->MarkAsUsed();

//     // Create projection expression List
//     auto path_name = qg->getQueryPath()->getUniqueName();
//     auto path_col_ref = lCreateColRefFromName(
//         path_name, lGetMDAccessor()->RetrieveType(&CMDIdGPDB::m_mdid_s62_path));
//     prev_plan->getSchema()->appendColumn(path_name, path_col_ref);

//     // Create project element
//     CExpressionArray *ident_array = GPOS_NEW(mp) CExpressionArray(mp);
//     ident_array->Append(GPOS_NEW(mp) CExpression(
//         mp, GPOS_NEW(mp) CScalarIdent(mp, lhs_col_ref)));
//     ident_array->Append(GPOS_NEW(mp) CExpression(
//         mp, GPOS_NEW(mp) CScalarIdent(mp, rhs_col_ref)));
//     CExpression *pexpr_value_list = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarValuesList(mp), ident_array);
//     auto *proj_elem = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectElement(mp, path_col_ref),
//                     pexpr_value_list);

//     // Create project List
//     CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
//     proj_array->Append(proj_elem);
//     CExpression *pexprPrjList = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);

//     // Create descriptor array
//     auto table_oids = edge->getTableIDs();
//     auto edge_name = edge->getUniqueName();
//     CTableDescriptorArray *path_table_descs =
//         lGetTableDescriptorArrayFromOids(edge_name, table_oids);

//     // Create pathname
//     std::wstring w_alias = L"";
//     w_alias.assign(edge_name.begin(), edge_name.end());
//     const CWStringConst path_name_str(w_alias.c_str());
//     const CName c_path_name(&path_name_str);

//     // Create shortest path
//     CExpression *shortestpath_expr = GPOS_NEW(mp) CExpression(
//         mp,
//         GPOS_NEW(mp) CLogicalShortestPath(
//             mp, &c_path_name, path_table_descs, lhs_col_ref, rhs_col_ref,
//             edge->getLowerBound(), edge->getUpperBound()),
//         prev_plan->getPlanExpr(), pexprPrjList);

//     // Add new operator
//     prev_plan->addUnaryParentOp(shortestpath_expr);
//     new_schema.appendColumn(path_name, path_col_ref);
//     prev_plan->setSchema(move(new_schema));
//     return prev_plan;
// }

// // merge with lPlanShortestPath
// LogicalPlan *Planner::lPlanAllShortestPath(QueryGraph *qg, NodeExpression *lhs,
//                                         RelExpression *edge,
//                                         NodeExpression *rhs,
//                                         LogicalPlan *prev_plan)
// {
//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

//     LogicalSchema new_schema;
//     new_schema.appendSchema(prev_plan->getSchema());

//     string lhs_name = lhs->getUniqueName();
//     string rhs_name = rhs->getUniqueName();

//     CColRef *lhs_col_ref =
//         prev_plan->getSchema()->getColRefOfKey(lhs_name, 0);
//     CColRef *rhs_col_ref =
//         prev_plan->getSchema()->getColRefOfKey(rhs_name, 0);
//     CColRef *prev_lhs_col_ref =
//         col_factory->LookupColRef(lhs_col_ref->PrevId());
//     CColRef *prev_rhs_col_ref =
//         col_factory->LookupColRef(rhs_col_ref->PrevId());

//     // Marking
//     lhs_col_ref->MarkAsUsed();
//     rhs_col_ref->MarkAsUsed();
//     prev_lhs_col_ref->MarkAsUsed();
//     prev_rhs_col_ref->MarkAsUsed();

//     // Create projection expression List
//     auto path_name = qg->getQueryPath()->getUniqueName();
//     auto path_col_ref = lCreateColRefFromName(
//         path_name, lGetMDAccessor()->RetrieveType(&CMDIdGPDB::m_mdid_s62_path));
//     prev_plan->getSchema()->appendColumn(path_name, path_col_ref);

//     // Create project element
//     CExpressionArray *ident_array = GPOS_NEW(mp) CExpressionArray(mp);
//     ident_array->Append(GPOS_NEW(mp) CExpression(
//         mp, GPOS_NEW(mp) CScalarIdent(mp, lhs_col_ref)));
//     ident_array->Append(GPOS_NEW(mp) CExpression(
//         mp, GPOS_NEW(mp) CScalarIdent(mp, rhs_col_ref)));
//     CExpression *pexpr_value_list = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarValuesList(mp), ident_array);
//     auto *proj_elem = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectElement(mp, path_col_ref),
//                     pexpr_value_list);

//     // Create project List
//     CExpressionArray *proj_array = GPOS_NEW(mp) CExpressionArray(mp);
//     proj_array->Append(proj_elem);
//     CExpression *pexprPrjList = GPOS_NEW(mp)
//         CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), proj_array);

//     // Create descriptor array
//     auto table_oids = edge->getTableIDs();
//     auto edge_name = edge->getUniqueName();
//     CTableDescriptorArray *path_table_descs =
//         lGetTableDescriptorArrayFromOids(edge_name, table_oids);

//     // Create pathname
//     std::wstring w_alias = L"";
//     w_alias.assign(edge_name.begin(), edge_name.end());
//     const CWStringConst path_name_str(w_alias.c_str());
//     const CName c_path_name(&path_name_str);

//     // Create shortest path
//     CExpression *allshortestpath_expr = GPOS_NEW(mp) CExpression(
//         mp,
//         GPOS_NEW(mp) CLogicalAllShortestPath(
//             mp, &c_path_name, path_table_descs, lhs_col_ref, rhs_col_ref,
//             edge->getLowerBound(), edge->getUpperBound()),
//         prev_plan->getPlanExpr(), pexprPrjList);

//     // Add new operator
//     prev_plan->addUnaryParentOp(allshortestpath_expr);
//     new_schema.appendColumn(path_name, path_col_ref);
//     prev_plan->setSchema(move(new_schema));
//     return prev_plan;
// }


// CExpression *Planner::lExprLogicalPathJoin(
//     CExpression *lhs, CExpression *rhs, CColRef *lhs_colref,
//     CColRef *rhs_colref, int32_t lower_bound, int32_t upper_bound,
//     gpopt::COperator::EOperatorId join_op)
// {

//     CMemoryPool *mp = this->memory_pool;

//     D_ASSERT(join_op == gpopt::COperator::EOperatorId::EopLogicalInnerJoin);

//     CColRef *pcrLeft = lhs_colref;
//     CColRef *pcrRight = rhs_colref;

//     lhs->AddRef();
//     rhs->AddRef();

//     CExpression *pexprEquality =
//         CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight);
//     pexprEquality->AddRef();

//     auto join_result = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
//         mp, lhs, rhs, pexprEquality);

//     return join_result;
// }

// CExpression *Planner::lExprLogicalShortestPathJoin(
//     CExpression *lhs, CExpression *rhs, CColRef *lhs_colref,
//     CColRef *rhs_colref, gpopt::COperator::EOperatorId join_op)
// {
//     return lExprLogicalPathJoin(lhs, rhs, lhs_colref, rhs_colref, 1, -1,
//                                 join_op);
// }

// CExpression *Planner::lExprLogicalCartProd(CExpression *lhs, CExpression *rhs)
// {
//     /* Perform cartesian product = inner join on predicate true	*/

//     CMemoryPool *mp = this->memory_pool;

//     CExpression *pexprTrue = CUtils::PexprScalarConstBool(mp, true, false);
//     auto prod_result =
//         CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, lhs, rhs, pexprTrue);
//     GPOS_ASSERT(CUtils::FCrossJoin(prod_result));
//     return prod_result;
// }

// CTableDescriptor *Planner::lCreateTableDescForRel(CMDIdGPDB *rel_mdid,
//                                                   std::string rel_name)
// {

//     CMemoryPool *mp = this->memory_pool;

//     const CWStringConst *table_name_cwstring =
//         lGetMDAccessor()->RetrieveRel(rel_mdid)->Mdname().GetMDName();
//     wstring table_name_ws(table_name_cwstring->GetBuffer());
//     string table_name(table_name_ws.begin(), table_name_ws.end());
//     std::wstring w_print_name = L"";
//     w_print_name.assign(table_name.begin(), table_name.end());
//     CWStringConst strName(w_print_name.c_str());
//     CTableDescriptor *ptabdesc =
//         lCreateTableDesc(mp,
//                          rel_mdid,         // 6.objid.0.0
//                          CName(&strName),  // debug purpose table string
//                          rel_name);

//     ptabdesc->AddRef();
//     return ptabdesc;
// }

// CTableDescriptor *Planner::lCreateTableDesc(CMemoryPool *mp, IMDId *mdid,
//                                             const CName &nameTable,
//                                             string rel_name,
//                                             gpos::BOOL fPartitioned)
// {
//     CTableDescriptor *ptabdesc = lTabdescPlainWithColNameFormat(
//         mp, mdid,
//         GPOS_WSZ_LIT("column_%04d"),  // format notused
//         nameTable, rel_name,
//         false /* is_nullable */);  // TODO retrieve isnullable from the storage!

//     return ptabdesc;
// }

// CTableDescriptor *Planner::lTabdescPlainWithColNameFormat(
//     CMemoryPool *mp, IMDId *mdid, const WCHAR *wszColNameFormat,
//     const CName &nameTable, string rel_name,
//     gpos::BOOL is_nullable  // define nullable columns
// )
// {

//     CWStringDynamic *str_name = GPOS_NEW(mp) CWStringDynamic(mp);
//     CTableDescriptor *ptabdesc = GPOS_NEW(mp) CTableDescriptor(
//         mp, mdid, nameTable,
//         false,  // convert_hash_to_random
//         IMDRelation::EreldistrMasterOnly, IMDRelation::ErelstorageHeap,
//         0  // ulExecuteAsUser
//     );

//     auto num_cols = lGetMDAccessor()->RetrieveRel(mdid)->ColumnCount();
//     for (ULONG i = 0; i < num_cols; i++) {
//         const IMDColumn *md_col =
//             lGetMDAccessor()->RetrieveRel(mdid)->GetMdCol(i);
//         IMDId *type_id = md_col->MdidType();
//         const IMDType *pmdtype = lGetMDAccessor()->RetrieveType(type_id);
//         INT type_modifier = md_col->TypeModifier();

//         wstring col_name_ws(md_col->Mdname().GetMDName()->GetBuffer());
//         string col_name(col_name_ws.begin(), col_name_ws.end());
//         if (rel_name != "") {
//             col_name = rel_name + "." + col_name;
//         }
//         std::wstring col_print_name = L"";
//         col_print_name.assign(col_name.begin(), col_name.end());
//         CWStringConst strName(col_print_name.c_str());

//         CName colname(&strName);
//         INT attno = md_col->AttrNum();
//         CColumnDescriptor *pcoldesc = GPOS_NEW(mp) CColumnDescriptor(
//             mp, pmdtype, type_modifier, colname, attno, is_nullable);
//         pcoldesc->SetPropId(md_col->PropId());
//         ptabdesc->AddColumn(pcoldesc);
//     }

//     GPOS_DELETE(str_name);
//     return ptabdesc;
// }

// CColRef *Planner::lCreateColRefFromName(std::string &name,
//                                         const IMDType *mdid_type)
// {
//     std::wstring w_name = L"";
//     w_name.assign(name.begin(), name.end());
//     const CWStringConst col_name_str(w_name.c_str());
//     CName col_cname(&col_name_str);
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
//     CColRef *path_col_ref = col_factory->PcrCreate(mdid_type, 0, col_cname);
//     path_col_ref->MarkAsUsed();
//     return path_col_ref;
// }

// CTableDescriptorArray *Planner::lGetTableDescriptorArrayFromOids(
//     string &unique_name, vector<uint64_t> &oids)
// {
//     CMemoryPool *mp = this->memory_pool;
//     CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
//     CTableDescriptorArray *path_table_descs =
//         GPOS_NEW(mp) CTableDescriptorArray(mp);
//     path_table_descs->AddRef();
//     for (auto &obj_id : oids) {
//         path_table_descs->Append(
//             lCreateTableDescForRel(lGenRelMdid(obj_id), unique_name));
//     }
//     return path_table_descs;
// }

// // @jhha: this pruning is not complete. It cannot fully capture complex operator tree
// // Instead, for implementation efficency, we used simple algorithm (see expresison binder)
// // Later, we need to implement more complex pruning algorithm (UNION for OR filter, INTERSECT for AND filter)
// void Planner::lPruneUnnecessaryGraphlets(
//     std::vector<uint64_t> &table_oids, NodeOrRelExpression *node_expr,
//     const expression_vector &prop_exprs,
//     std::vector<uint64_t> &final_pruned_table_oids)
// {
//     std::unordered_set<uint64_t> union_pruned_table_oids;

//     // Process each OR group
//     auto orGroupIDs = node_expr->getORGroupIDs();
//     if (orGroupIDs.size() == 0) {
//         final_pruned_table_oids.assign(table_oids.begin(), table_oids.end());
//         std::sort(final_pruned_table_oids.begin(), final_pruned_table_oids.end());
//         return;
//     }
//     for (auto group_idx: orGroupIDs) {
//         std::unordered_map<uint64_t, int> necessary_table_oids_map;
//         std::vector<uint64_t> group_pruned_table_oids;

//         for (auto table_oid : table_oids) {
//             necessary_table_oids_map[table_oid] = 0;
//         }

//         int num_filter_cols = 0;
//         for (int col_idx = 0; col_idx < prop_exprs.size(); ++col_idx) {
//             if (!node_expr->isUsedForFilterColumn(col_idx, group_idx))
//                 continue;

//             auto &_prop_expr = prop_exprs[col_idx];
//             PropertyExpression *expr =
//                 static_cast<PropertyExpression *>(_prop_expr.get());
//             num_filter_cols++;
//             auto *propIdPerTable = expr->getPropertyIDPerTable();

//             for (auto &it : *propIdPerTable) {
//                 if (necessary_table_oids_map.find(it.first) !=
//                     necessary_table_oids_map.end()) {
//                     necessary_table_oids_map[it.first]++;
//                 } else {
//                     D_ASSERT(false);
//                 }
//             }
//         }

//         for (auto &it : necessary_table_oids_map) {
//             if (it.second == num_filter_cols) {
//                 group_pruned_table_oids.push_back(it.first);
//             }
//         }

//         std::sort(group_pruned_table_oids.begin(), group_pruned_table_oids.end());
//         union_pruned_table_oids.insert(group_pruned_table_oids.begin(), group_pruned_table_oids.end());
//     }

//     // Convert union set to final pruned table OIDs
//     final_pruned_table_oids.assign(union_pruned_table_oids.begin(), union_pruned_table_oids.end());
//     std::sort(final_pruned_table_oids.begin(), final_pruned_table_oids.end());
// }

// // prune useless columns (e.g., MATCH (n) WHERE n.C_ACCTBAL > 100 RETURN n
// // query should not return columns from the tables not having C_ACCTBAL)
// void Planner::lPruneUnnecessaryColumns(NodeOrRelExpression *node_expr,
//                                 const expression_vector &prop_exprs,
//                                 std::vector<uint64_t> &pruned_table_oids)
// {
//     for (int col_idx = 0; col_idx < prop_exprs.size(); col_idx++) {
//         auto &_prop_expr = prop_exprs[col_idx];
//         PropertyExpression *expr =
//             static_cast<PropertyExpression *>(_prop_expr.get());
//         auto *propIdPerTable = expr->getPropertyIDPerTable();

//         bool is_used = false;
//         for (auto &it : *propIdPerTable) {
//             if (std::find(pruned_table_oids.begin(), pruned_table_oids.end(),
//                           it.first) != pruned_table_oids.end()) {
//                 is_used = true;
//                 break;
//             }
//         }
//         if (!is_used) {
//             node_expr->setUnusedColumn(col_idx);
//             pruned_key_ids.push_back(expr->getPropertyID());
//         }
//     }
//     return;
// }

// void Planner::lGetUsedPropertyIDs(
//     NodeOrRelExpression *node_rel_expr, const expression_vector &prop_exprs,
//         std::vector<uint64_t>& prop_key_ids, bool exclude_pid
// )
// {
//     for (int col_idx = 0; col_idx < prop_exprs.size(); col_idx++) {
//         if (!node_rel_expr->isUsedColumn(col_idx))
//             continue;
//         PropertyExpression *expr =
//             (PropertyExpression *)(prop_exprs[col_idx].get());
//         if (col_idx != 0) {  // exclude _id column
//             prop_key_ids.push_back(expr->getPropertyID());
//         }
//     }
// }

// void Planner::lUpdatePropExprsForNewTable(
// 		NodeOrRelExpression *node_rel_expr, const expression_vector &prop_exprs,
// 		uint64_t oid, std::vector<uint64_t>& property_location
// )
// {
//     int col_idx_except_id_col = 0;
//     for (int col_idx = 0; col_idx < prop_exprs.size(); col_idx++) {
//         if (!node_rel_expr->isUsedColumn(col_idx))
//             continue;
//         PropertyExpression *expr =
//             (PropertyExpression *)(prop_exprs[col_idx].get());
//         if (col_idx != 0) {  // exclude _id column
//             if (property_location[col_idx_except_id_col] !=
//                 std::numeric_limits<uint64_t>::max()) {
//                 expr->addPropertyID(oid,
//                                     property_location[col_idx_except_id_col] +
//                                         1);
//             }
//             col_idx_except_id_col++;
//         }
//         else {
//             expr->addPropertyID(oid, 0);
//         }
//     }
// }

}  // namespace s62