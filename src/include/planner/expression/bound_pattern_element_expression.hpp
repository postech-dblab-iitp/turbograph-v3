#pragma once

#include "common/types/value.hpp"
#include "planner/expression.hpp"
#include "common/case_insensitive_map.hpp"

namespace duckdb {

//TOOD: (jhha) We need PatternElementExpression for transformer
class BoundPatternElementExpression : public Expression {
public:
	BoundPatternElementExpression(LogicalType type, std::string variableName)
		: Expression(ExpressionType::INVALID, ExpressionClass::BOUND_PATTERN_ELEMENT, type),
		  variableName(variableName) {
	}

public:
	// Override functions
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;
	hash_t Hash() const override;

	unique_ptr<Expression> Copy() override;

	// Member functions
	inline std::string getVariableName() const { return variableName; }
	inline vector<CatalogObjectID> getPartitionOIDs() const { return partitionOIDs; }
	inline vector<CatalogObjectID> getGraphletOIDs() const { return graphletOIDs; }
	inline vector<size_t> getNumTablesPerPartition() const { return numTablesPerPartition; }

	void addPartitionOIDs(const vector<CatalogObjectID>& partitionOIDsToAdd);
	void addGraphletOIDs(const vector<CatalogObjectID>& graphletOIDsToAdd, const vector<size_t>& numTablesPerPartitionToAdd);

    inline bool isEmpty() const { return partitionOIDs.empty(); }
    inline bool isMultiLabeled() const { return partitionOIDs.size() > 1; }

	void addPropertyExpression(PropertyKeyID propertyKeyID, std::shared_ptr<Expression> property);
	void getPropertyExpression(PropertyKeyID propertyKeyID);
	bool hasPropertyExpression(PropertyKeyID propertyKeyID) const;

	// S62-specific functions
    void setUnusedColumn(idx_t col_idx);
    bool isUsedColumn(uint64_t col_idx);
	void setUsedForFilterColumn(PropertyKeyID propertyKeyID, idx_t group_idx = 0);
	bool isUsedForFilterColumn(PropertyKeyID propertyKeyID, idx_t group_idx = 0) const;
	vector<idx_t> getORGroupIDs();

protected:

    std::string variableName;
    // A pattern may bind to multiple tables.
    vector<CatalogObjectID> partitionOIDs;
    vector<CatalogObjectID> graphletOIDs;
	vector<size_t> numTablesPerPartition;
    // Index over propertyExprs on property name.
	std::unordered_map<PropertyKeyID, idx_t> propertyKeyIDToIdx;
    // Property expressions with order.
    std::vector<std::shared_ptr<Expression>> propertyExprs;
    // Property data expressions specified by user in the form of "{propertyName : data}"
    case_insensitive_map_t<std::shared_ptr<Expression>> propertyDataExprs;
	// Column pruning
	std::vector<bool> column_used;
    std::unordered_map<idx_t, vector<bool>> used_for_filter_columns_per_OR;
};
} // namespace duckdb