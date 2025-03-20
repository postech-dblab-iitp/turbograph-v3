#pragma once

#include <string>

#include "parser/parsed_expression.hpp"
#include "common/copy_constructors.hpp"
#include "common/typedefs.hpp"

namespace duckdb {

class Pattern {
public:
    Pattern(std::string name, 
        std::vector<ParsedPropertyKeyValue> propertyKeyVals)
        : variableName{std::move(name)}, propertyKeyVals{std::move(propertyKeyVals)} {}

    virtual ~Pattern() = default;

    inline std::string getVariableName() const { return variableName; }

    inline const std::vector<ParsedPropertyKeyValue>& getPropertyKeyVals() const {
        return propertyKeyVals;
    }

    virtual std::string ToString() const {
        std::stringstream ss;
        ss << variableName;

        if (!propertyKeyVals.empty()) {
            ss << " {";
            for (size_t i = 0; i < propertyKeyVals.size(); ++i) {
                auto &keyValPair = propertyKeyVals[i];
                ss << keyValPair.first << ":";
                ss << keyValPair.second->ToString();
                if (i < propertyKeyVals.size() - 1) {
                    ss << ", ";
                }
            }
            ss << "}";
        }

        return ss.str();
    }

protected:
    std::string variableName;
    std::vector<ParsedPropertyKeyValue> propertyKeyVals;
};

}