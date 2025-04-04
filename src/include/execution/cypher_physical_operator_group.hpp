#pragma once

#include "execution/physical_operator/cypher_physical_operator.hpp"

namespace duckdb {

// Big assumption: in vector, 2-dim group is always the last one

// Represents group of pipelines (each pipline can also be super-pipeline)
class CypherPhysicalOperatorGroup {
public:
    CypherPhysicalOperatorGroup() = default;
    
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

    CypherPhysicalOperator *GetIdxOperator(int idx) {
        if (IsSingleton()) {
            return op;
        }
        else {
            auto &repr_child = childs[child_idx];
            size_t acc_size = 0;
            for (auto &child : repr_child) {
                if (acc_size + child->GetSize() > idx) {
                    return child->GetIdxOperator(idx - acc_size);
                }
                acc_size += child->GetSize();
            }
            throw NotImplementedException("Invalid Idx for CypherPhysicalOperatorGroup GetOp");
            return nullptr;
        }
    }

    bool IsSingleton() const {
        return op != nullptr;
    }

    void IncrementChildIdx() {
        child_idx++;
    }

    bool AdvanceChild() {
        if (IsSingleton()) return false;
        if (IsChildSingletonVector(child_idx)) {
            if (!IsLastChild()) {
                IncrementChildIdx();
                return true;
            }
            else {
                return false;
            }
        }
        else {
            auto &current_child = childs[child_idx];
            auto advanced = current_child[0]->AdvanceChild();
            if (!advanced) {
                if (!IsLastChild()) {
                    IncrementChildIdx();
                    return true;
                }
                else {
                    return false;
                }
            }
            else {
                return true;
            }
        }
    }

    bool IsLastChild() {
        if (IsSingleton()) return true;
        return child_idx == childs.size() - 1;
    }

    bool IsChildSingletonVector(int child_idx) {
        if (IsSingleton()) return true;
        auto &child = childs[child_idx];
        bool is_singleton_vector = true;
        for (auto group : child) {
            is_singleton_vector &= group->IsSingleton();
        }
        return is_singleton_vector;
    }

    size_t GetSize() {
        if (IsSingleton()) {
            return 1;
        }
        else {
            auto &cur_child = childs[child_idx];
            size_t size = 0;
            for (auto &cur_child_child : cur_child) {
                size += cur_child_child->GetSize();
            }
            return size;
        }
    }

    vector<vector<CypherPhysicalOperatorGroup *>> childs;
    CypherPhysicalOperator *op = nullptr;
    int child_idx = 0;
};

// Data structure for handling recursive super-pipeline
// For example
// [op1, op2, [(op3, op4) | (op3-2, op4-2)]]
// This is represented as two CypherPhysicalOperatorGroup

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
        vector<size_t> group_sizes;
        size_t total_size = 0;
        for (auto &group : groups) {
            total_size += group->GetSize();
        }
        return total_size;
    }

    CypherPhysicalOperator *operator[](idx_t idx) {
        return GetIdxOperator(idx);
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

    bool AdvanceGroup() {
        return groups[0]->AdvanceChild();
    }

    CypherPhysicalOperator* GetIdxOperator(int idx) const {
        size_t acc_size = 0;
        for (auto &group : groups) {
            size_t size = group->GetSize();
            if (acc_size + size > idx) {
                return group->GetIdxOperator(idx - acc_size);
            }
            acc_size += size;
        }
        return nullptr;
    }

    bool IsSinkSingleton() const {
        return groups.back()->IsSingleton();
    }

    vector<CypherPhysicalOperatorGroup *> groups;
};


}