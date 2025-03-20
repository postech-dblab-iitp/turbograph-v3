#pragma once

#include <optional>

#include "parser/query/query_part.hpp"
#include "parser/query/return_with_clause/return_clause.hpp"

namespace duckdb {

class SingleQuery {
public:
    SingleQuery() = default;

    inline void addQueryPart(std::unique_ptr<QueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline const QueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    inline uint32_t getNumUpdatingClauses() const { return updatingClauses.size(); }
    inline UpdatingClause* getUpdatingClause(uint32_t idx) const {
        return updatingClauses[idx].get();
    }
    inline void addUpdatingClause(std::unique_ptr<UpdatingClause> updatingClause) {
        updatingClauses.push_back(std::move(updatingClause));
    }

    inline uint32_t getNumReadingClauses() const { return readingClauses.size(); }
    inline ReadingClause* getReadingClause(uint32_t idx) const { return readingClauses[idx].get(); }
    inline void addReadingClause(std::unique_ptr<ReadingClause> readingClause) {
        readingClauses.push_back(std::move(readingClause));
    }

    inline void setReturnClause(std::unique_ptr<ReturnClause> returnClause) {
        this->returnClause = std::move(returnClause);
    }
    inline bool hasReturnClause() const { return returnClause != nullptr; }
    inline ReturnClause* getReturnClause() const { return returnClause.get(); }

    string ToString() const {
        std::stringstream ss;

        for (size_t i = 0; i < queryParts.size(); ++i) {
            ss << queryParts[i]->ToString();
            if (i < queryParts.size() - 1) {
                ss << " ";
            }
        }
        
        if (queryParts.size() > 0) {
            ss << " ";
        }

        // Append reading clauses
        for (size_t i = 0; i < readingClauses.size(); ++i) {
            ss << readingClauses[i]->ToString();
            if (i < readingClauses.size() - 1) {
                ss << " ";
            }
        }

        // Append updating clauses
        for (size_t i = 0; i < updatingClauses.size(); ++i) {
            if (i > 0 || !readingClauses.empty()) {
                ss << " ";
            }
            ss << updatingClauses[i]->ToString();
        }

        // Append return clause if present
        if (returnClause) {
            if (!queryParts.empty() || !readingClauses.empty() || !updatingClauses.empty()) {
                ss << " ";
            }
            ss << returnClause->ToString();
        }

        return ss.str();
    }

private:
    std::vector<std::unique_ptr<QueryPart>> queryParts;
    std::vector<std::unique_ptr<ReadingClause>> readingClauses;
    std::vector<std::unique_ptr<UpdatingClause>> updatingClauses;
    std::unique_ptr<ReturnClause> returnClause;
};

}