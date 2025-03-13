#pragma once

#include "converter/c2o/logical_plan.hpp"

#include "gpos/base.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpopt/base/CQueryContext.h"

#include "kuzu/binder/binder.h"
#include "kuzu/binder/bound_statement.h"
#include "kuzu/binder/query/reading_clause/bound_reading_clause.h"
#include "kuzu/binder/query/reading_clause/bound_match_clause.h"

using namespace kuzu::binder;

namespace s62 {

// Converts Cypher parse tree → Orca logical plan
class Cypher2OrcaConverter {

public:
    Cypher2OrcaConverter(CMemoryPool *_mp): mp(_mp) {};
    ~Cypher2OrcaConverter() = default;

    LogicalPlan *Convert(BoundStatement * boundStatement);
 
private:

    LogicalPlan *ConvertSingleQuery(const NormalizedSingleQuery &singleQuery);
    LogicalPlan *ConvertQueryPart(const NormalizedQueryPart &queryPart, LogicalPlan *input_plan);
    LogicalPlan *ConvertReadingClause(const BoundReadingClause *readingClause, LogicalPlan *input_plan);
    LogicalPlan *ConvertMatchClause(const BoundMatchClause *readingClause, LogicalPlan *input_plan);
    LogicalPlan *ConvertUnwindClause(const BoundUnwindClause *unwindClause, LogicalPlan *input_plan);
    LogicalPlan *ConvertProjectionBody(const BoundProjectionBody *projBody, LogicalPlan *input_plan);
    LogicalPlan *ConvertSelection(const expression_vector &predicates, LogicalPlan *input_plan);

private:
    CMemoryPool *mp;

};

}