
#pragma once

#include <string>

namespace duckdb {

class BoundPattern {
public:
    BoundPattern() = default;

    std::string getUniqueName() const {
        return "";
    }
};

}