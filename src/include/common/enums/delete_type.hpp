#pragma once

#include <cstdint>

namespace duckdb {

enum class DeleteNodeType : uint8_t {
    DELETE = 0,
    DETACH_DELETE = 1,
};

}
