#pragma once

#include <unordered_map>

#include "expression.h"

namespace kuzu {
namespace binder {

class NodeOrRelExpression : public Expression {
public:
    NodeOrRelExpression(
        DataTypeID dataTypeID, const string& uniqueName, vector<table_id_t> partitionIDs, vector<table_id_t> tableIDs)
        : Expression{VARIABLE, dataTypeID, uniqueName}, partitionIDs{std::move(partitionIDs)}, tableIDs{std::move(tableIDs)} {}
    virtual ~NodeOrRelExpression() = default;

    inline void addTableIDs(const vector<table_id_t>& tableIDsToAdd) {
        auto tableIDsMap = unordered_set<table_id_t>(tableIDs.begin(), tableIDs.end());
        for (auto tableID : tableIDsToAdd) {
            if (!(tableIDsMap.find(tableID) != tableIDsMap.end())) {
                tableIDs.push_back(tableID);
            }
        }
    }
    inline void pushBackTableIDs(const vector<table_id_t>& tableIDsToAdd) {
        for (auto i = 0; i < tableIDsToAdd.size(); i++) {
            tableIDs.push_back(tableIDsToAdd[i]);
        }
    }
    void setUnivTableID(table_id_t univTableID) {
        this->univTableID = univTableID;
    }
    inline bool isMultiLabeled() const { return tableIDs.size() > 1; }
    inline vector<table_id_t> &getPartitionIDs() { return partitionIDs; }
    inline vector<table_id_t> &getTableIDs() { return tableIDs; }
    table_id_t getUnivTableID() { return univTableID; }
    inline table_id_t getSingleTableID() const {
        assert(tableIDs.size() == 1);
        return tableIDs[0];
    }

    inline void addPropertyExpression(const string propertyName, unique_ptr<Expression> property) {
        assert(!(propertyNameToIdx.find(propertyName) != propertyNameToIdx.end()));
        propertyNameToIdx.insert({propertyName, properties.size()});
        properties.push_back(std::move(property));
    }
    inline bool hasPropertyExpression(const string& propertyName) const {
        return propertyNameToIdx.find(propertyName) != propertyNameToIdx.end();
    }
    inline shared_ptr<Expression> getPropertyExpression(const string& propertyName) const {
        assert(propertyNameToIdx.find(propertyName) != propertyNameToIdx.end());
        return properties[propertyNameToIdx.at(propertyName)]->copy();
    }
    inline const vector<unique_ptr<Expression>>& getPropertyExpressions() const {
        return properties;
    }

    bool isSchemainfoBound() {
        return schema_info_bound;
    }

    void setSchemainfoBound(bool schema_info_bound_) {
        schema_info_bound = schema_info_bound_;
    }

protected:
    vector<table_id_t> partitionIDs;
    vector<table_id_t> tableIDs;
    table_id_t univTableID;
    unordered_map<std::string, size_t> propertyNameToIdx;
    // TODO maintain map<tid, vector<size_t> projectionListPerTid
    vector<unique_ptr<Expression>> properties;
    bool schema_info_bound = false;
};

} // namespace binder
} // namespace kuzu
