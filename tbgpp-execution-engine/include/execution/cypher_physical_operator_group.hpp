#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

class CypherPhysicalOperatorGroup { // 2-dim group
public:
    CypherPhysicalOperatorGroup() = default;
    ~CypherPhysicalOperatorGroup() = default;

    CypherPhysicalOperatorGroup(CypherPhysicalOperator *op) {
        this->op = op;
    }

    void PushBack(vector<CypherPhysicalOperatorGroup *> &child) {
        childs.push_back(child);
    }

    void SetOp(CypherPhysicalOperator *op) {
        this->op = op;
    }

    CypherPhysicalOperator *GetOp() {
        return op;
    }

    bool IsSingleton() const {
        return op != nullptr;
    }

    vector<vector<CypherPhysicalOperatorGroup *>> childs;
    CypherPhysicalOperator *op = nullptr;
};


class CypherPhysicalOperatorGroups {
    
public:
    CypherPhysicalOperatorGroups() = default;

    void push_back(CypherPhysicalOperator *op) {
        groups.push_back(new CypherPhysicalOperatorGroup(op));
    }

    void push_back(CypherPhysicalOperatorGroup *group) {
        groups.push_back(group);
    }

    size_t size() {
        return groups.size();
    }

    CypherPhysicalOperatorGroup *operator[](idx_t idx) {
        return groups[idx];
    }

    CypherPhysicalOperator* back() {
        if (groups.back()->IsSingleton()) {
            return groups.back()->GetOp();
        }
        else {
            throw NotImplementedException("back() is not allowed for grouped operators");
            return nullptr;
        }
    }
    
    vector<CypherPhysicalOperatorGroup *> &GetGroups() {
        return groups;
    }
  
    vector<CypherPhysicalOperatorGroup *> groups;
};


}