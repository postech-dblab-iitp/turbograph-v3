#pragma once

#include <cstdint>

enum class ReturnStatus : uint8_t {
    OK,
    DONE,
    FAIL,
    ON_GOING,
};