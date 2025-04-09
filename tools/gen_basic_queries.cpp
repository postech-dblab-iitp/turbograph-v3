#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <random>
#include <algorithm>
#include <optional>
#include <limits>
#include <cstdlib>

#include "simdjson.h"
#include "common/logger.hpp"
#include "nlohmann/json.hpp"

using namespace simdjson;

// -----------------------------------------------------------------------
// Hard-coded values controlling query generation
// -----------------------------------------------------------------------
static const bool FULL_SCAN = false; // Generate full scan query
static const int NUM_PROJ  = 0;  // Number of projection queries
static const int NUM_EQUI  = 0;  // Number of equality filter queries
static const int NUM_RANGE = 0;  // Number of range filter queries
static const int NUM_AGG   = 0;  // Number of aggregation queries
static const int NUM_JOINS = 5;  // Number of join queries
static const int NUM_COLUMNS_PER_JOIN = 5; // Number of columns to select in a join query
static const bool AGG_AFTER_JOIN = true;

// -----------------------------------------------------------------------
// Data structures to collect per-property statistics
// -----------------------------------------------------------------------
struct NumericStats {
    double min_value = std::numeric_limits<double>::max();
    double max_value = std::numeric_limits<double>::lowest();
    // A small random sample of unique values
    std::vector<double> sample_values;
};

struct StringStats {
    // A small random sample of unique values
    std::vector<std::string> sample_values;
};

struct PropertyStats {
    bool is_numeric = false;
    // If numeric
    NumericStats numeric_stats;
    // If string
    StringStats string_stats;
};

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------
static const size_t MAX_SAMPLE_SIZE = 100; // how many random distinct values to keep

// Add a numeric value to the numeric stats (update min, max, keep sample).
void updateNumericStats(PropertyStats& ps, double val) {
    NumericStats& ns = ps.numeric_stats;
    if(val < ns.min_value) ns.min_value = val;
    if(val > ns.max_value) ns.max_value = val;

    // Very naive "random reservoir" approach for sampling
    if (ns.sample_values.size() < MAX_SAMPLE_SIZE) {
        ns.sample_values.push_back(val);
    } else {
        static thread_local std::mt19937_64 rng(std::random_device{}());
        std::uniform_int_distribution<size_t> dist(0, std::numeric_limits<size_t>::max());
        size_t randomIndex = dist(rng) % (ns.sample_values.size() + 1);
        if (randomIndex < MAX_SAMPLE_SIZE) {
            ns.sample_values[randomIndex] = val;
        }
    }
}

// Add a string value to the string stats (keep sample).
void updateStringStats(PropertyStats& ps, const std::string& val) {
    // if we already have it, no need to add (basic check)
    auto& sampleVals = ps.string_stats.sample_values;
    if (std::find(sampleVals.begin(), sampleVals.end(), val) != sampleVals.end()) {
        return;
    }

    if(sampleVals.size() < MAX_SAMPLE_SIZE) {
        sampleVals.push_back(val);
    } else {
        static thread_local std::mt19937_64 rng(std::random_device{}());
        std::uniform_int_distribution<size_t> dist(0, std::numeric_limits<size_t>::max());
        size_t randomIndex = dist(rng) % (sampleVals.size() + 1);
        if(randomIndex < MAX_SAMPLE_SIZE) {
            sampleVals[randomIndex] = val;
        }
    }
}

// Decide if the simdjson on-demand value is numeric
bool isLikelyNumeric(ondemand::json_type type) {
    return (type == ondemand::json_type::number);
}

// Wrap property name with ``
std::string wrapPropertyName(const std::string& propName) {
    return "`" + propName + "`";
}

// -----------------------------------------------------------------------
// Write/read property stats to/from a JSON file
// -----------------------------------------------------------------------

// Convert our stats structure into a JSON representation using nlohmann::json
nlohmann::json statsToJson(const std::unordered_map<std::string, PropertyStats>& statsMap) {
    nlohmann::json j;
    for(const auto& kv : statsMap) {
        const auto& propName = kv.first;
        const auto& propStats = kv.second;

        nlohmann::json propJson;
        propJson["is_numeric"] = propStats.is_numeric;

        if(propStats.is_numeric) {
            propJson["numeric_stats"]["min"] = propStats.numeric_stats.min_value;
            propJson["numeric_stats"]["max"] = propStats.numeric_stats.max_value;
            // store sample values
            propJson["numeric_stats"]["sample"] = nlohmann::json::array();
            for(auto& v : propStats.numeric_stats.sample_values) {
                propJson["numeric_stats"]["sample"].push_back(v);
            }
        } else {
            // store sample values
            propJson["string_stats"]["sample"] = nlohmann::json::array();
            for(auto& s : propStats.string_stats.sample_values) {
                propJson["string_stats"]["sample"].push_back(s);
            }
        }
        j[propName] = propJson;
    }
    return j;
}

std::unordered_map<std::string, PropertyStats> jsonToStats(const nlohmann::json& j) {
    std::unordered_map<std::string, PropertyStats> statsMap;
    for(auto it = j.begin(); it != j.end(); ++it) {
        PropertyStats ps;
        auto propName = it.key();
        auto val = it.value();
        ps.is_numeric = val["is_numeric"].get<bool>();
        if(ps.is_numeric) {
            ps.numeric_stats.min_value = val["numeric_stats"]["min"].get<double>();
            ps.numeric_stats.max_value = val["numeric_stats"]["max"].get<double>();

            auto sampleArr = val["numeric_stats"]["sample"];
            for(auto& sv : sampleArr) {
                ps.numeric_stats.sample_values.push_back(sv.get<double>());
            }
        } else {
            auto sampleArr = val["string_stats"]["sample"];
            for(auto& sv : sampleArr) {
                ps.string_stats.sample_values.push_back(sv.get<std::string>());
            }
        }
        statsMap[propName] = ps;
    }
    return statsMap;
}

void DumpStatsFile(std::unordered_map<std::string, PropertyStats> &propertyStats, std::optional<std::string> &statsFile) {
    std::ofstream ofs(*statsFile);
    auto j = statsToJson(propertyStats);
    ofs << j.dump(2) << std::endl;
}

void ParseJsonlFile(const std::string &inputJsonlPath,
                      std::unordered_map<std::string, PropertyStats> &propertyStats) {
    ondemand::parser parser;
    padded_string json = padded_string::load(inputJsonlPath);
    ondemand::document_stream docs = parser.iterate_many(json);

    for (auto doc : docs) {
        // For example, we expect something like:
        // {
        //   "labels": ["NODE"],
        //   "properties": { "id": 9, "uri": "http://..." }
        // }
        //
        // 1) Try to get "properties"
        ondemand::value props_val = doc["properties"];
        if (props_val.type() != ondemand::json_type::object) {
            // If there's no "properties" object, skip
            continue;
        }

        // 2) Iterate over the properties object
        for (auto field : props_val.get_object()) {
            // 'unescaped_key()' gives the raw key without JSON escaping
            std::string key = std::string(std::string_view(field.unescaped_key()));

            // If propertyStats does not exist yet, create it
            if (propertyStats.find(key) == propertyStats.end()) {
                PropertyStats ps;
                ps.is_numeric = isLikelyNumeric(field.value().type());
                // Initialize numeric stats if needed
                // e.g. ps.numeric_stats.min_value = ...
                propertyStats[key] = ps;
            }

            PropertyStats &ps = propertyStats[key];

            // Check type with on-demand
            if (ps.is_numeric) {
                // Already flagged numeric, see if this value is actually numeric
                if (field.value().type() == ondemand::json_type::number) {
                    // Figure out if it's integer/floating
                    ondemand::number_type num_type = field.value().get_number_type();
                    double valNum = 0;
                    if (num_type == ondemand::number_type::signed_integer) {
                        valNum = double(int64_t(field.value().get_int64().value()));
                    } else if (num_type == ondemand::number_type::unsigned_integer) {
                        valNum = double(uint64_t(field.value().get_uint64().value()));
                    } else { // floating_point_number
                        valNum = double(field.value().get_double().value());
                    }
                    updateNumericStats(ps, valNum);
                } else {
                    // We found a non-numeric for a property we thought was numeric
                    // => forcibly switch to string
                    ps.is_numeric = false;
                    std::string s = std::string(std::string_view(field.value().get_string()));
                    updateStringStats(ps, s);
                }
            } else {
                if (field.value().type() == ondemand::json_type::string) {
                    std::string s = std::string(std::string_view(field.value().get_string()));
                    updateStringStats(ps, s);
                } else {
                    spdlog::warn("[ParseJsonlFile] Unexpected type for property {}", key);
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// Program entry point
// -----------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] 
                  << " <input_jsonl_file> <output_folder> [stats_file]\n";
        return 1;
    }
    SetupLogger();

    std::string inputJsonlPath = argv[1];
    std::string outputFolder   = argv[2];
    std::optional<std::string> statsFile;
    if (argc >= 4) {
        statsFile = argv[3];
    }

    spdlog::info("[Main] Input JSONL file: {}", inputJsonlPath);
    spdlog::info("[Main] Output folder: {}", outputFolder);
    if(statsFile) {
        spdlog::info("[Main] Stats file: {}", *statsFile);
    }

    // Attempt to load existing stats if user provided a stats file
    bool loadedStats = false;
    std::unordered_map<std::string, PropertyStats> propertyStats;

    if(statsFile) {
        spdlog::info("[Main] Attempting to load stats from {}", *statsFile);
        // Try read the stats
        std::ifstream ifs(*statsFile);
        if(ifs.good()) {
            try {
                nlohmann::json j;
                ifs >> j;
                propertyStats = jsonToStats(j);
                loadedStats = true;
                spdlog::info("[Main] Successfully loaded stats from {}", *statsFile);
            } catch(...) {
                spdlog::error("[Main] Failed to load stats from {}", *statsFile);
                loadedStats = false;
            }
        }
        else {
            spdlog::warn("[Main] Failed to load stats from {}", *statsFile);
            loadedStats = false;
        }
    }


    // If we cannot load stats, we parse the JSONL file and build them
    if(!loadedStats) {
        spdlog::info("[Main] Parsing JSONL file to build stats");
        ParseJsonlFile(inputJsonlPath, propertyStats);
        DumpStatsFile(propertyStats, statsFile);
        spdlog::info("[Main] Successfully built stats");
    }

    // We separate property names into numeric vs. string
    std::vector<std::string> numericProps;
    std::vector<std::string> stringProps;
    for(const auto& kv : propertyStats) {
        if(kv.second.is_numeric) {
            numericProps.push_back(kv.first);
        } else {
            stringProps.push_back(kv.first);
        }
    }

    // Prepare random engine for queries
    static thread_local std::mt19937_64 rng(std::random_device{}());

    // Helper function to pick a random item from a vector
    auto pickRandom = [&](auto& vec) -> typename std::decay<decltype(vec)>::type::value_type {
        std::uniform_int_distribution<size_t> dist(0, vec.size()-1);
        return vec[dist(rng)];
    };

    // Aggregation variants
    std::vector<std::string> aggregators = {"MIN", "MAX", "SUM"};

    // ------------------------------------------------
    // Now let's generate queries
    // ------------------------------------------------
    std::vector<std::string> queries;

    // 1) Full scan query
    {
        if (FULL_SCAN) {
            std::string q = "MATCH (n) RETURN n";
            queries.push_back(q);
        }
    }

    // 2) Projection queries
    //    Just pick random properties from all properties (or pick from numeric + string).
    //    For demonstration, we'll do it from all property names, if present.
    {
        // Combine numericProps + stringProps
        std::vector<std::string> allProps = numericProps;
        allProps.insert(allProps.end(), stringProps.begin(), stringProps.end());

        if(!allProps.empty()) {
            for(int i=0; i<NUM_PROJ; i++){
                auto col = pickRandom(allProps);
                std::string q = "MATCH (n) RETURN n." + wrapPropertyName(col);
                queries.push_back(q);
            }
        }
    }

    // 3) Equality filter queries
    //    - For numeric props, do "WHERE n.prop = value"
    //    - For string props, do "WHERE n.prop = 'value'"
    //    We pick randomly from numeric or string props, if available.
    {
        if(!numericProps.empty() || !stringProps.empty()) {
            for(int i=0; i<NUM_EQUI; i++) {
                bool pickNumeric = false;
                if(numericProps.empty()) {
                    pickNumeric = false;
                } else if (stringProps.empty()) {
                    pickNumeric = true;
                } else {
                    // random choice
                    std::uniform_int_distribution<int> dist(0,1);
                    pickNumeric = (dist(rng) == 1);
                }

                if(pickNumeric) {
                    auto prop = pickRandom(numericProps);
                    auto& ns = propertyStats[prop].numeric_stats;
                    // pick random sample if available
                    if(!ns.sample_values.empty()) {
                        auto val = pickRandom(ns.sample_values);
                        long long valInt = (long long)val;
                        // build query
                        std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " = " + std::to_string(valInt) + " RETURN n." + wrapPropertyName(prop);
                        queries.push_back(q);
                    } else {
                        // fallback
                        std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " = 0 RETURN n." + wrapPropertyName(prop);
                        queries.push_back(q);
                    }
                } else {
                    // string
                    auto prop = pickRandom(stringProps);
                    auto& ss = propertyStats[prop].string_stats;
                    if(!ss.sample_values.empty()) {
                        auto val = pickRandom(ss.sample_values);
                        // escape single quotes if needed
                        // (For simplicity, do naive replacement of `'` with `\'`)
                        std::string safeVal;
                        safeVal.reserve(val.size());
                        for(char c : val) {
                            if(c == '\'') safeVal += "\\'";
                            else safeVal += c;
                        }
                        std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " = '" + safeVal + "' RETURN n." + wrapPropertyName(prop);
                        queries.push_back(q);
                    } else {
                        std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " = '' RETURN n." + wrapPropertyName(prop);
                        queries.push_back(q);
                    }
                }
            }
        }
    }

    // 4) Range filter queries
    //    - For numeric props: e.g. "WHERE n.prop > X AND n.prop < Y"
    //    - For string props: e.g. "WHERE n.prop CONTAINS 'str'"
    {
        // numeric range
        int numericRangeCount = 0;
        if(!numericProps.empty()) {
            for(int i=0; i<NUM_RANGE; i++){
                // pick a random numeric prop
                auto prop = pickRandom(numericProps);
                auto& ns = propertyStats[prop].numeric_stats;
                if(ns.min_value < ns.max_value) {
                    // pick random within [min, max], to generate "WHERE n.prop > X AND n.prop < Y"
                    std::uniform_real_distribution<double> dist(ns.min_value, ns.max_value);
                    double v1 = dist(rng);
                    double v2 = dist(rng);
                    if(v1 > v2) std::swap(v1, v2);
                    // round them for nice output
                    long long i1 = (long long)v1;
                    long long i2 = (long long)v2;
                    std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " > " + std::to_string(i1) +
                                    " AND n." + wrapPropertyName(prop) + " < " + std::to_string(i2) +
                                    " RETURN n." + wrapPropertyName(prop);
                    queries.push_back(q);
                    numericRangeCount++;
                } else {
                    // fallback
                    std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " > 10 AND n." + wrapPropertyName(prop) + " < 100 RETURN n." + wrapPropertyName(prop);
                    queries.push_back(q);
                    numericRangeCount++;
                }
            }
        }

        // If we still need range queries and have string props, do "CONTAINS" style
        // We'll put them in the same loop (just as an example). If we want to keep them separate,
        // we can break them out. Here, let's do a maximum of NUM_RANGE total (some numeric, some string).
        int totalRangeNeeded = NUM_RANGE;
        int stringRangeCount = totalRangeNeeded - numericRangeCount;
        if(stringRangeCount < 0) stringRangeCount = 0;
        if(!stringProps.empty()) {
            for(int i=0; i<stringRangeCount; i++){
                // pick a random string prop
                auto prop = pickRandom(stringProps);
                auto& ss = propertyStats[prop].string_stats;
                if(!ss.sample_values.empty()) {
                    auto val = pickRandom(ss.sample_values);
                    // We want to pick a substring => let's do a naive substring from the first few chars
                    // Or we can just do 'val' as is
                    // to demonstrate "CONTAINS"
                    if(val.size() > 5) {
                        val = val.substr(0, 5); 
                    }
                    // escape
                    std::string safeVal;
                    safeVal.reserve(val.size());
                    for(char c : val) {
                        if(c == '\'') safeVal += "\\'";
                        else safeVal += c;
                    }
                    std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " CONTAINS '" + safeVal + "' RETURN n." + wrapPropertyName(prop);
                    queries.push_back(q);
                } else {
                    // fallback
                    std::string q = "MATCH (n) WHERE n." + wrapPropertyName(prop) + " CONTAINS '' RETURN n." + wrapPropertyName(prop);
                    queries.push_back(q);
                }
            }
        }
    }

    // 5) Aggregation queries
    //    - "MATCH (n) RETURN MIN(n.prop)"
    //    - "MATCH (n) RETURN MAX(n.prop)"
    //    - "MATCH (n) RETURN SUM(n.prop)"
    {
        if(!numericProps.empty()) {
            for(int i=0; i<NUM_AGG; i++){
                auto prop = pickRandom(numericProps);
                auto agg = pickRandom(aggregators);
                std::string q = "MATCH (n) RETURN " + agg + "(n." + wrapPropertyName(prop) + ")";
                queries.push_back(q);
            }
        } else {
            // If no numeric property, fallback aggregator is on "id" or do nothing
            // For demonstration, let's do 2 aggregator queries on 'id'
            for(int i=0; i<NUM_AGG; i++){
                auto agg = pickRandom(aggregators);
                std::string q = "MATCH (n) RETURN " + agg + "(n.id)";
                queries.push_back(q);
            }
        }
    }

    // 6) Join queries
    //    - Use edge: http://www.w3.org/2002/07/owl#sameAs
    //    - Generate paths with 1 to NUM_JOINS hops
    //    - Each node returns NUM_COLUMNS_PER_JOIN properties (if available)
    {
        std::string redirectEdge = "TEN_PRCNT";

        // Combine all properties for selection
        std::vector<std::string> allProps = numericProps;
        allProps.insert(allProps.end(), stringProps.begin(), stringProps.end());

        if (!allProps.empty()) {
            for (int hop = 1; hop <= NUM_JOINS; hop++) {
                std::string match = "MATCH ";
                std::string returnClause = "RETURN \n";
                std::vector<std::string> nodeAliases;

                // Build the MATCH chain
                for (int i = 0; i <= hop; ++i) {
                    nodeAliases.push_back("n" + std::to_string(i));
                }

                for (int i = 0; i < hop; ++i) {
                    match += "(" + nodeAliases[i] + ")";
                    match += "-[:`" + redirectEdge + "`]->";
                }
                match += "(" + nodeAliases[hop] + ")";

                // Build RETURN clause
                std::vector<std::string> projectedProps;

                for (const auto& alias : nodeAliases) {
                    std::vector<std::string> selectedProps;
                    std::sample(
                        allProps.begin(), allProps.end(),
                        std::back_inserter(selectedProps),
                        std::min((int)allProps.size(), NUM_COLUMNS_PER_JOIN),
                        rng
                    );

                    for (const auto& prop : selectedProps) {
                        std::string expr = alias + "." + wrapPropertyName(prop);
                        projectedProps.push_back(expr);
                    }
                }

                if (AGG_AFTER_JOIN) {
                    int numAgg = projectedProps.size();
                    for (size_t i = 0; i < projectedProps.size(); ++i) {
                        if (i > 0) returnClause += ", \n";
                        if ((int)i < numAgg)
                            returnClause += "COUNT(" + projectedProps[i] + ")";
                        else
                            returnClause += projectedProps[i];
                    }
                } else {
                    for (size_t i = 0; i < projectedProps.size(); ++i) {
                        if (i > 0) returnClause += ", \n";
                        returnClause += projectedProps[i];
                    }
                }

                queries.push_back(match + "\n" + returnClause);
            }
        }
    }

    // ------------------------------------------------
    // Write each query to its own file: q{number}.cql
    // ------------------------------------------------
    int queryIndex = 1;
    for (auto &q : queries) {
        std::string outFile = outputFolder + "/q" + std::to_string(queryIndex) + ".cql";
        std::ofstream ofsQuery(outFile);
        if (!ofsQuery.good()) {
            spdlog::error("Failed to open output file: {}", outFile);
            return 1;
        }
        ofsQuery << q << ";\n";
        ofsQuery.close();
        ++queryIndex;
    }

    spdlog::info("Successfully generated {} queries in folder: {}", queries.size(), outputFolder);
}
