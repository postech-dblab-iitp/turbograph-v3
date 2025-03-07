
#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <regex>
#include <stdexcept>
#include "common/types/value.hpp"
#include "common/types/data_chunk.hpp"

namespace duckdb {

class CypherPreparedStatement {
public:
    std::string originalQuery;
    std::map<std::string, std::string> params; // Stores param names and their values
    std::vector<std::string> paramOrder; // Stores the order of param names for indexed access
	std::vector<DataChunk*> queryResults;

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

    bool checkParamsAllSet() {
        for (const auto& pair : params) {
            if (pair.second == "") {
                return false;
            }
        }
        return true;
    }

    size_t getNumParams() {
        return params.size();
    }

    std::string getBoundQuery() {
        D_ASSERT(checkParamsAllSet());

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

    bool bindValue(int index, Value& value) {
        if (index >= 0 && index < paramOrder.size()) {
            auto type = value.type();
            auto type_id = type.id();
            
            if (type_id == LogicalTypeId::BOOLEAN) {
                params[paramOrder[index]] = value.GetValue<bool>() ? "true" : "false";
            }
            else if (type_id == LogicalTypeId::VARCHAR) {
                params[paramOrder[index]] = "'" + value.GetValue<string>() + "'";
            } 
            else if (type_id == LogicalTypeId::DATE) {
                params[paramOrder[index]] = "date('" + value.GetValue<string>() + "')";
            }
            else {
                params[paramOrder[index]] = value.ToString();
            }

            return true;
        } else {
            return false;
        }
    }

    void copyResults(vector<shared_ptr<DataChunk>>& results) {
        for (size_t i = 0; i < results.size(); i++) {
            queryResults.push_back(new DataChunk());
        }
        for (size_t i = 0; i < results.size(); i++) {
            queryResults[i]->Move(*results[i]);
        }
    }

    size_t getNumRows() {
        size_t num_total_tuples = 0;
        for (auto &it : queryResults) num_total_tuples += it->size();
        return num_total_tuples;
    }
};
}
