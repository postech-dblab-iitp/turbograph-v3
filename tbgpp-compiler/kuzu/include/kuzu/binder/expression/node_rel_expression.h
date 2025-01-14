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

    inline void addPropertyExpression(uint64_t propertyKeyID, unique_ptr<Expression> property) {
        assert(!(propertyKeyIDToIdx.find(propertyKeyID) != propertyKeyIDToIdx.end()));
        propertyKeyIDToIdx.insert({propertyKeyID, properties.size()});
        properties.push_back(std::move(property));
        used_columns.push_back(false);
    }

    inline bool hasPropertyExpression(uint64_t propertyKeyID) const {
        return propertyKeyIDToIdx.find(propertyKeyID) != propertyKeyIDToIdx.end();
    }

    inline shared_ptr<Expression> getPropertyExpression(uint64_t propertyKeyID) {
        assert(propertyKeyIDToIdx.find(propertyKeyID) != propertyKeyIDToIdx.end());
        used_columns[propertyKeyIDToIdx.at(propertyKeyID)] = true;
        // return properties[propertyNameToIdx.at(propertyName)]->copy();
        return properties[propertyKeyIDToIdx.at(propertyKeyID)];
    }

    inline const vector<shared_ptr<Expression>> &getPropertyExpressions() const {
        return properties;
    }

    bool isSchemainfoBound() {
        return schema_info_bound;
    }

    void setSchemainfoBound(bool schema_info_bound_) {
        schema_info_bound = schema_info_bound_;
    }

    bool isDSITarget() {
        return dsi_target;
    }

    void setDSITarget() {
        dsi_target = true;
    }

    void markAllColumnsAsUsed() {
        for (auto i = 0; i < used_columns.size(); i++) {
            used_columns[i] = true;
        }
        is_whold_node_required = true;
    }

    void setUnusedColumn(uint64_t col_idx) {
        assert(used_columns.size() > col_idx);
        used_columns[col_idx] = false;
    }

    bool isUsedColumn(uint64_t col_idx) {
        assert(used_columns.size() > col_idx);
        return used_columns[col_idx];
    }

    void setUsedForFilterColumn(uint64_t propertyKeyID, uint64_t group_idx = 0) {
        if (used_for_filter_columns_per_OR.find(group_idx) == used_for_filter_columns_per_OR.end()) {
            used_for_filter_columns_per_OR[group_idx] = vector<bool>(used_columns.size(), false);
        }
        auto col_idx = propertyKeyIDToIdx.at(propertyKeyID);
        auto &used_for_filter_columns = used_for_filter_columns_per_OR[group_idx];
        used_columns[col_idx] = true;
        used_for_filter_columns[col_idx] = true;
    }

    bool isUsedForFilterColumn(uint64_t col_idx, uint64_t group_idx = 0) {
        if (used_for_filter_columns_per_OR.find(group_idx) == used_for_filter_columns_per_OR.end()) {
            return false;
        }
        auto &used_for_filter_columns = used_for_filter_columns_per_OR[group_idx];
        assert(used_for_filter_columns.size() > col_idx);
        return used_for_filter_columns[col_idx];
    }

    vector<uint64_t> getORGroupIDs() {
        vector<uint64_t> group_ids;
        for (auto &it : used_for_filter_columns_per_OR) {
            group_ids.push_back(it.first);
        }
        return std::move(group_ids);
    }

    bool isWholeNodeRequired() {
        return is_whold_node_required;
    }

protected:
    vector<table_id_t> partitionIDs;
    vector<table_id_t> tableIDs;
    table_id_t univTableID;
    unordered_map<uint64_t, size_t> propertyKeyIDToIdx;
    // TODO maintain map<tid, vector<size_t> projectionListPerTid
    // vector<unique_ptr<Expression>> properties;
    vector<shared_ptr<Expression>> properties;
    vector<bool> used_columns;
    unordered_map<int, vector<bool>> used_for_filter_columns_per_OR;
    bool schema_info_bound = false;
    bool dsi_target = false;
    bool is_whold_node_required = false;
};

} // namespace binder
} // namespace kuzu
