#include "planner/expression/bound_pattern_element_expression.hpp"


namespace duckdb {

BoundPatternElementExpression::BoundPatternElementExpression(LogicalType type, std::string variableName)
		: Expression(ExpressionType::INVALID, ExpressionClass::BOUND_PATTERN_ELEMENT, type),
		  variableName(variableName)
{
}

string BoundPatternElementExpression::ToString() const {
    throw NotImplementedException("BoundPatternElementExpression::ToString");
}

bool BoundPatternElementExpression::Equals(const BaseExpression *other) const {
    if (!Expression::Equals(other)) {
        return false;
    }
    auto other_ = (BoundPatternElementExpression *)other;
    if (variableName != other_->variableName) {
        return false;
    }
    if (!std::equal(partitionOIDs.begin(), partitionOIDs.end(), other_->partitionOIDs.begin(), other_->partitionOIDs.end())) {
        return false;
    }
    if (!std::equal(graphletOIDs.begin(), graphletOIDs.end(), other_->graphletOIDs.begin(), other_->graphletOIDs.end())) {
        return false;
    }
    return true;
}

hash_t BoundPatternElementExpression::Hash() const {
    hash_t result = Expression::Hash();
    result = CombineHash(result, duckdb::Hash(variableName));
    for (auto &partitionOID : partitionOIDs) {
        result = CombineHash(result, duckdb::Hash(partitionOID));
    }
    for (auto &graphletOID : graphletOIDs) {
        result = CombineHash(result, duckdb::Hash(graphletOID));
    }
    return result;
}

unique_ptr<Expression> BoundPatternElementExpression::Copy() {
    auto copy = make_unique<BoundPatternElementExpression>(return_type, variableName);
    copy->CopyProperties(*this);
    copy->partitionOIDs = partitionOIDs;
    copy->graphletOIDs = graphletOIDs;
    copy->propertyNameToIdx = propertyNameToIdx;
    for (auto &propertyExpr : propertyExprs) {
        copy->propertyExprs.push_back(propertyExpr->Copy());
    }
    copy->labelExpression = labelExpression->Copy();
    copy->propertyDataExprs = propertyDataExprs;
    return move(copy);
}

void BoundPatternElementExpression::addPartitionOIDs(const vector<CatalogObjectID>& partitionOIDsToAdd) {
    for (auto& oid : partitionOIDsToAdd) {
        partitionOIDs.push_back(oid);
    }
}

void BoundPatternElementExpression::addGraphletOIDs(const vector<CatalogObjectID>& graphletOIDsToAdd, const vector<size_t>& numTablesPerPartitionToAdd) {
    for (auto i = 0; i < graphletOIDsToAdd.size(); i++) {
        graphletOIDs.push_back(graphletOIDsToAdd[i]);
    }

    for (auto i = 0; i < numTablesPerPartitionToAdd.size(); i++) {
        numTablesPerPartition.push_back(numTablesPerPartitionToAdd[i]);
    }
}


void BoundPatternElementExpression::addPartitionOIDs(const vector<CatalogObjectID>& partitionOIDsToAdd) {
    for (auto& oid : partitionOIDsToAdd) {
        partitionOIDs.push_back(oid);
    }
}

void BoundPatternElementExpression::addGraphletOIDs(const vector<CatalogObjectID>& graphletOIDsToAdd) {
    for (auto& oid : graphletOIDsToAdd) {
        graphletOIDs.push_back(oid);
    }
}

void BoundPatternElementExpression::addPropertyExpression(PropertyKeyID propertyKeyID, std::shared_ptr<Expression> property) {
    D_ASSERT(propertyKeyIDToIdx.find(propertyKeyID) == propertyKeyIDToIdx.end());
    propertyKeyIDToIdx.insert({propertyKeyID, propertyExprs.size()});
    propertyExprs.push_back(property);
}

void BoundPatternElementExpression::getPropertyExpression(PropertyKeyID propertyKeyID) {
    D_ASSERT(propertyKeyIDToIdx.find(propertyKeyID) != propertyKeyIDToIdx.end());
    return propertyExprs[propertyKeyIDToIdx.at(propertyKeyID)];
}

bool BoundPatternElementExpression::hasPropertyExpression(PropertyKeyID propertyKeyID) const {
    return propertyKeyIDToIdx.find(propertyKeyID) != propertyKeyIDToIdx.end();
}

void BoundPatternElementExpression::setUnusedColumn(idx_t col_idx) {
    D_ASSERT(used_columns.size() > col_idx);
    used_columns[col_idx] = false;
}

bool BoundPatternElementExpression::isUsedColumn(uint64_t col_idx) {
    D_ASSERT(used_columns.size() > col_idx);
    return used_columns[col_idx];
}

void BoundPatternElementExpression::setUsedForFilterColumn(PropertyKeyID propertyKeyID, idx_t group_idx) {
    if (used_for_filter_columns_per_OR.find(group_idx) == used_for_filter_columns_per_OR.end()) {
        used_for_filter_columns_per_OR[group_idx] = vector<bool>(used_columns.size(), false);
    }
    auto col_idx = propertyKeyIDToIdx.at(propertyKeyID);
    auto &used_for_filter_columns = used_for_filter_columns_per_OR[group_idx];
    used_columns[col_idx] = true;
    used_for_filter_columns[col_idx] = true;
}

bool BoundPatternElementExpression::isUsedForFilterColumn(PropertyKeyID propertyKeyID, idx_t group_idx) const {
    if (used_for_filter_columns_per_OR.find(group_idx) == used_for_filter_columns_per_OR.end()) {
        return false;
    }
    auto &used_for_filter_columns = used_for_filter_columns_per_OR[group_idx];
    D_ASSERT(used_for_filter_columns.size() > col_idx);
    return used_for_filter_columns[col_idx];
}

vector<idx_t> BoundPatternElementExpression::getORGroupIDs() {
    vector<idx_t> result;
    for (auto &pair : used_for_filter_columns_per_OR) {
        result.push_back(pair.first);
    }
    return result;
}

} // namespace duckdb