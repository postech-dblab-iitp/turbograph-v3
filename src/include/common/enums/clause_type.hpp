#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

enum class ClauseType : uint8_t {
    // updating clause
    SET = 0,
    DELETE_ = 1, // winnt.h defines DELETE as a macro, so we use DELETE_ instead of DELETE.
    INSERT = 2,
    MERGE = 3,

    // reading clause
    MATCH = 10,
    UNWIND = 11,
    IN_QUERY_CALL = 12,
    TABLE_FUNCTION_CALL = 13,
    GDS_CALL = 14,
    LOAD_FROM = 15,
};

enum class MatchClauseType : uint8_t {
    MATCH = 0,
    OPTIONAL_MATCH = 1,
};

inline std::string ClauseTypeToString(ClauseType clauseType) {
    switch (clauseType) {
        case ClauseType::SET: return "SET";
        case ClauseType::DELETE_: return "DELETE";
        case ClauseType::INSERT: return "INSERT";
        case ClauseType::MERGE: return "MERGE";
        case ClauseType::MATCH: return "MATCH";
        case ClauseType::UNWIND: return "UNWIND";
        case ClauseType::IN_QUERY_CALL: return "IN QUERY CALL";
        case ClauseType::TABLE_FUNCTION_CALL: return "TABLE FUNCTION CALL";
        case ClauseType::GDS_CALL: return "GDS CALL";
        case ClauseType::LOAD_FROM: return "LOAD FROM";
        default: return "UNKNOWN_CLAUSE";
    }
}

}
