#pragma once

#include "common/typedefs.hpp"
#include "parser/cypher_statement.hpp"
#include "parser/query/single_query.hpp"

namespace duckdb {

class RegularQuery : public CypherStatement {
    static constexpr StatementType type_ = StatementType::SELECT_STATEMENT;

public:
    explicit RegularQuery(std::unique_ptr<SingleQuery> singleQuery) : CypherStatement{type_} {
        singleQueries.push_back(std::move(singleQuery));
    }

    void addSingleQuery(std::unique_ptr<SingleQuery> singleQuery, bool isUnionAllQuery) {
        singleQueries.push_back(std::move(singleQuery));
        isUnionAll.push_back(isUnionAllQuery);
    }

    idx_t getNumSingleQueries() const { return singleQueries.size(); }

    const SingleQuery* getSingleQuery(idx_t singleQueryIdx) const {
        return singleQueries[singleQueryIdx].get();
    }

    std::vector<bool> getIsUnionAll() const { return isUnionAll; }

    string ToString() const override {
        std::stringstream ss;
        for (size_t i = 0; i < singleQueries.size(); ++i) {
            if (i > 0) {
                ss << (isUnionAll[i - 1] ? " UNION ALL " : " UNION ");
            }
            ss << singleQueries[i]->ToString();
        }
        return ss.str();
    }

	unique_ptr<CypherStatement> Copy() const override {
        throw InternalException("Copy not supported for this type of CypherStatement");
    }

    bool Equals(const CypherStatement *other) const {
        // compare singleQueries and isUnionAll
        if (other->type != type) {
            return false;
        }
        auto otherRegularQuery = (RegularQuery *)other;
        if (singleQueries.size() != otherRegularQuery->singleQueries.size()) {
            return false;
        }
        for (size_t i = 0; i < singleQueries.size(); ++i) {
            if (singleQueries[i] != otherRegularQuery->singleQueries[i]) {
                return false;
            }
        }
        for (size_t i = 0; i < isUnionAll.size(); ++i) {
            if (isUnionAll[i] != otherRegularQuery->isUnionAll[i]) {
                return false;
            }
        }
        return true;
    }

private:
    std::vector<std::unique_ptr<SingleQuery>> singleQueries;
    std::vector<bool> isUnionAll;
};

} 
