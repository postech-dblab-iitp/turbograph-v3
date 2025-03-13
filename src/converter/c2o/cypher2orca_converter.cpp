#include "converter/c2o/cypher2orca_converter.hpp"

namespace s62 {

LogicalPlan *Cypher2OrcaConverter::Convert(BoundStatement * boundStatement) {
    GPOS_ASSERT(boundStatement != nullptr);
    auto &regularQuery = *((BoundRegularQuery *)(boundStatement));

    // TODO need union between single queries
    GPOS_ASSERT(regularQuery.getNumSingleQueries() == 1);
    return ConvertSingleQuery(*regularQuery.getSingleQuery(0));
}

LogicalPlan *Cypher2OrcaConverter::ConvertSingleQuery(const NormalizedSingleQuery &singleQuery) {
    LogicalPlan *plan = nullptr;

    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        plan = ConvertQueryPart(*singleQuery.getQueryPart(i), plan);
    }

    return plan;
}

LogicalPlan *Cypher2OrcaConverter::ConvertQueryPart(const NormalizedQueryPart &queryPart, LogicalPlan *input_plan) {
    LogicalPlan* plan = input_plan;

    // Process reading clauses
    for (size_t i = 0; i < queryPart.getNumReadingClause(); ++i) {
        plan = ConvertReadingClause(queryPart.getReadingClause(i), plan);
    }

    GPOS_ASSERT(queryPart.getNumUpdatingClause() == 0);

    if (queryPart.hasProjectionBody()) {
        plan = ConvertProjectionBody(queryPart.getProjectionBody(), plan);
    }

    return plan;
}

LogicalPlan *Cypher2OrcaConverter::ConvertReadingClause(const BoundReadingClause *readingClause, LogicalPlan *input_plan) {
    switch (readingClause->getClauseType()) {
    case ClauseType::MATCH:
        return ConvertMatchClause((BoundMatchClause *)readingClause, input_plan);
    case ClauseType::UNWIND:
        return ConvertUnwindClause((BoundUnwindClause *)readingClause, input_plan);
    default:
        GPOS_ASSERT(false);
        return nullptr;
    }
}

LogicalPlan *Cypher2OrcaConverter::ConvertProjectionBody(const BoundProjectionBody *projBody, LogicalPlan *input_plan) {


    // if (queryPart.hasProjectionBodyPredicate()) {
    //     // WITH ... WHERE ...
    //     // Need to check semantics on whether filter or project should be planned first on behalf of other.
    //     // maybe filter first?
    //     cur_plan = lPlanSelection(
    //         queryPart.getProjectionBodyPredicate().get()->splitOnAND(),
    //         cur_plan);
    //     // appendFilter(queryPart.getProjectionBodyPredicate(), *plan);
    // }

    return nullptr;
}


LogicalPlan *Cypher2OrcaConverter::ConvertMatchClause(const BoundMatchClause *matchClause, 
                                        LogicalPlan *input_plan)
{
    auto queryGraphCollection = matchClause->getQueryGraphCollection();

    LogicalPlan *plan;
    if (!matchClause->getIsOptional()) {
        // plan = lPlanRegularMatch(*queryGraphCollection, input_plan);
    }
    else {
        // plan = lPlanRegularOptionalMatch(*queryGraphCollection, input_plan);
    }

    if (matchClause->hasWhereExpression()) {
        plan = ConvertSelection(matchClause->getWhereExpression()->splitOnAND(), plan);
    }

    return plan;
}

LogicalPlan *Cypher2OrcaConverter::ConvertUnwindClause(const BoundUnwindClause *unwindClause,
                                        LogicalPlan *input_plan)
{
    GPOS_ASSERT(false);
    return nullptr;
}


LogicalPlan *Cypher2OrcaConverter::ConvertSelection(const expression_vector &predicates, 
                                                    LogicalPlan *input_plan)
{

}


/**************************
 * Expression Converters *
**************************/



}