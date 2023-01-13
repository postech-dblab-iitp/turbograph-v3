#pragma once

#include <cstdint>

namespace kuzu {
namespace common {

enum class StatementType : uint8_t {
    QUERY = 0,
    CREATE_NODE_CLAUSE = 1,
    CREATE_REL_CLAUSE = 2,
    COPY_CSV = 3,
    DROP_TABLE = 4,
};

class StatementTypeUtils {
public:
    static bool isDDL(StatementType statementType) {
        return statementType == StatementType::CREATE_NODE_CLAUSE ||
               statementType == StatementType::CREATE_REL_CLAUSE ||
               statementType == StatementType::DROP_TABLE;
    }

    static bool isCopyCSV(StatementType statementType) {
        return statementType == StatementType::COPY_CSV;
    }

    static bool isDDLOrCopyCSV(StatementType statementType) {
        return isDDL(statementType) || isCopyCSV(statementType);
    }
};

} // namespace common
} // namespace kuzu
