#pragma once

#include "parser/query/reading_clause/reading_clause.hpp"
#include "parser/query/return_with_clause/with_clause.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"

namespace duckdb {

class QueryPart {
public:
    explicit QueryPart(std::unique_ptr<WithClause> withClause) : withClause{std::move(withClause)} {}

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

    inline const WithClause* getWithClause() const { return withClause.get(); }

    string ToString() const {
        std::stringstream ss;

        // Append reading clauses
        for (size_t i = 0; i < readingClauses.size(); ++i) {
            ss << readingClauses[i]->ToString();
            if (i < readingClauses.size() - 1) {
                ss << " ";
            }
        }

        // Append updating clauses
        if (!updatingClauses.empty()) {
            if (!readingClauses.empty()) {
                ss << " ";
            }
            for (size_t i = 0; i < updatingClauses.size(); ++i) {
                ss << updatingClauses[i]->ToString();
                if (i < updatingClauses.size() - 1) {
                    ss << " ";
                }
            }
        }

        // Append WITH clause if present
        if (withClause) {
            if (!readingClauses.empty() || !updatingClauses.empty()) {
                ss << " ";
            }
            ss << withClause->ToString();
        }

        return ss.str();
    }

private:
    std::vector<std::unique_ptr<ReadingClause>> readingClauses;
    std::vector<std::unique_ptr<UpdatingClause>> updatingClauses;
    std::unique_ptr<WithClause> withClause;
};

}