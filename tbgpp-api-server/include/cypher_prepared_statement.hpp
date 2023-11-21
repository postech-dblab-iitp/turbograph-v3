
#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <regex>
#include <stdexcept>

namespace duckdb {

class CypherPreparedStatement {
public:
    std::string originalQuery;
    std::map<std::string, std::string> params; // Stores param names and their values
    std::vector<std::string> paramOrder; // Stores the order of param names for indexed access
    int boundParams = 0;

    CypherPreparedStatement(const std::string& query) : originalQuery(query) {
        std::regex paramRegex("\\$[a-zA-Z_][a-zA-Z0-9_]*"); // Regex to match named parameters like $name
        auto words_begin = std::sregex_iterator(query.begin(), query.end(), paramRegex);
        auto words_end = std::sregex_iterator();

        for (std::sregex_iterator i = words_begin; i != words_end; ++i) {
            std::smatch match = *i;
            std::string matchStr = match.str();
            params[matchStr] = ""; // Initialize with empty strings
            paramOrder.push_back(matchStr); // Store the order of parameter names
        }
    }

    size_t getNumParams() {
        return params.size();
    }

    void setParam(const std::string& paramName, const std::string& value) {
        if (params.find(paramName) != params.end()) {
            params[paramName] = "'" + value + "'";
            boundParams++;
        } else {
            throw std::out_of_range("Parameter name not found: " + paramName);
        }
    }

    void setParam(int index, const std::string& value) {
        if (index >= 0 && index < paramOrder.size()) {
            setParam(paramOrder[index], value);
        } else {
            throw std::out_of_range("Parameter index out of bounds: " + std::to_string(index));
        }
    }

    // void setParam(int index, const std::string& value) {
    //     if (index > 0 && index <= params.size()) {
    //         params[index - 1] = "'" + value + "'";
    //         boundParams++;
    //     } else {
    //         throw std::out_of_range("Index out of bounds for string, index " + std::to_string(index));
    //     }
    // }

    // void setParam(int index, int value) {
    //     if (index > 0 && index <= params.size()) {
    //         params[index - 1] = std::to_string(value);
    //         boundParams++;
    //     } else {
    //         throw std::out_of_range("Index out of bounds for integer, index " + std::to_string(index));
    //     }
    // }

    // void setParam(int index, double value) {
    //     if (index > 0 && index <= params.size()) {
    //         params[index - 1] = std::to_string(value);
    //         boundParams++;
    //     } else {
    //         throw std::out_of_range("Index out of bounds for double, index " + std::to_string(index));
    //     }
    // }

    // void setParam(int index, bool value) {
    //     if (index > 0 && index <= params.size()) {
    //         params[index - 1] = value ? "true" : "false";
    //         boundParams++;
    //     } else {
    //         throw std::out_of_range("Index out of bounds for boolean, index " + std::to_string(index));
    //     }
    // }

    std::string getQuery() {
        if (boundParams != params.size()) {
            std::cerr << "Not all parameters have been set!" << std::endl;
            return "";
        }

        std::string result = originalQuery;
        for (const auto& pair : params) {
            size_t pos = result.find(pair.first);
            while (pos != std::string::npos) {
                result.replace(pos, pair.first.length(), pair.second);
                pos = result.find(pair.first, pos + pair.second.length());
            }
        }
        return result;
    }
};
}
