#pragma once
#include <string>

namespace duckdb {

struct YieldVariable {
    std::string name;
    std::string alias;

    YieldVariable(std::string name, std::string alias)
        : name{std::move(name)}, alias{std::move(alias)} {}
    bool hasAlias() const { return alias != ""; }
    std::string ToString() const {
        return hasAlias() ? name + " AS " + alias : name;
    }

};

}