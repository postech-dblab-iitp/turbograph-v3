
#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <stdexcept>

namespace duckdb {

class CypherPreparedStatement {
private:
    std::string originalQuery;
    std::vector<std::string> params;
    std::vector<size_t> paramPositions;
    int paramCount = 0;

    // Utility function to replace the first occurrence of a substring from a specific position
    std::string replaceFromPosition(const std::string& source, const std::string& from, const std::string& to, size_t position) {
        std::string result = source;
        size_t pos = result.find(from, position);
        if (pos != std::string::npos) {
            result.replace(pos, from.length(), to);
        }
        return result;
    }

public:
    CypherPreparedStatement(const std::string& query) : originalQuery(query) {
        size_t pos = 0;
        bool insideString = false;
        while (pos < originalQuery.length()) {
            if (originalQuery[pos] == '\'') {
                insideString = !insideString;
            } else if (originalQuery[pos] == '?' && !insideString) {
                params.push_back("");
                paramPositions.push_back(pos);
            }
            pos++;
        }
    }

    void setParam(int index, const std::string& value) {
        if (index > 0 && index <= params.size()) {
            params[index - 1] = "'" + value + "'";
            paramCount++;
        } else {
            throw std::out_of_range("Index out of bounds for string, index " + std::to_string(index));
        }
    }

    void setParam(int index, int value) {
        if (index > 0 && index <= params.size()) {
            params[index - 1] = std::to_string(value);
            paramCount++;
        } else {
            throw std::out_of_range("Index out of bounds for integer, index " + std::to_string(index));
        }
    }

    void setParam(int index, double value) {
        if (index > 0 && index <= params.size()) {
            params[index - 1] = std::to_string(value);
            paramCount++;
        } else {
            throw std::out_of_range("Index out of bounds for double, index " + std::to_string(index));
        }
    }

    void setParam(int index, bool value) {
        if (index > 0 && index <= params.size()) {
            params[index - 1] = value ? "true" : "false";
            paramCount++;
        } else {
            throw std::out_of_range("Index out of bounds for boolean, index " + std::to_string(index));
        }
    }

    std::string getQuery() {
        if (paramCount != params.size()) {
            std::cerr << "Not all parameters have been set!" << std::endl;
            return "";
        }

        std::string result = originalQuery;
        for (size_t i = 0; i < params.size(); ++i) {
            result = replaceFromPosition(result, "?", params[i], paramPositions[i]);
        }
        return result;
    }
};
}
