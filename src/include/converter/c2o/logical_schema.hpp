#pragma once

#include <cassert>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <limits>
#include <algorithm>

#include "gpos/base.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"

#include "common/assert.hpp"

using namespace std;
using namespace gpopt;
using namespace gpos;

namespace s62 {

class LogicalSchema {

   public:
    LogicalSchema()
    {
        bound_nodes = set<string>();
        bound_edges = set<string>();
    }
    ~LogicalSchema() {}

    void copyNodeFrom(LogicalSchema *old_schema, std::string node_name)
    {
        D_ASSERT(old_schema->bound_nodes.find(node_name) !=
                 old_schema->bound_nodes.end());
        for (auto &sch : old_schema->schema) {
            if (node_name == std::get<0>(sch)) {
                appendNodeProperty(node_name, std::get<1>(sch),
                                   std::get<2>(sch));
            }
        }
    }

    void copyEdgeFrom(LogicalSchema *old_schema, std::string edge_name)
    {
        D_ASSERT(old_schema->bound_edges.find(edge_name) !=
                 old_schema->bound_edges.end());
        for (auto &sch : old_schema->schema) {
            if (edge_name == std::get<0>(sch)) {
                appendEdgeProperty(edge_name, std::get<1>(sch),
                                   std::get<2>(sch));
            }
        }
    }

    void appendSchema(LogicalSchema *sch1)
    {
        // TODO ASSERT key does not collapse
        // schema
        auto &sch = *sch1;
        for (auto &prop : sch.schema) {
            this->schema.push_back(prop);
        }
        // bound variables
        for (auto &it : sch.bound_nodes) {
            this->bound_nodes.insert(it);
        }
        for (auto &it : sch.bound_edges) {
            this->bound_edges.insert(it);
        }
    }

    void appendNodeProperty(string k1, uint64_t k2, CColRef *colref)
    {
        D_ASSERT(colref != NULL);
        appendKey(k1, k2, colref, true, false);
    }
    void appendEdgeProperty(string k1, uint64_t k2, CColRef *colref)
    {
        D_ASSERT(colref != NULL);
        appendKey(k1, k2, colref, false, true);
    }
    void appendColumn(string k1, CColRef *colref)
    {
        D_ASSERT(colref != NULL);
        schema.push_back(
            make_tuple(k1, std::numeric_limits<uint64_t>::max(), colref));
    }

    uint64_t getNumPropertiesOfKey(string k1)
    {
        uint64_t cnt = 0;
        for (auto &col : schema) {
            if (std::get<0>(col) == k1) {
                cnt++;
            }
        }
        return cnt;
    }

    CColRef *getColRefOfKey(string k1, uint64_t k2)
    {
        bool found = false;
        CColRef *found_colref = NULL;
        for (int idx = 0; idx < schema.size(); idx++) {
            auto &col = schema[idx];
            if (std::get<0>(col) == k1 && std::get<1>(col) == k2) {
                found = true;
                found_colref = std::get<2>(col);
                break;
            }
        }

        // if not found, and k1 like "x.prop" and k2 == "", then try normailzing k1 and k2
        if (!found) {
            if (k1.find(".") != std::string::npos &&
                k2 == std::numeric_limits<uint64_t>::max()) {
                auto k1_temp = k1;
                int pos = k1_temp.find(".");
                k1 = k1_temp.substr(0, pos);
                // Check if the substring after '.' is numeric before conversion
                auto numericPart = k1_temp.substr(pos + 1, std::string::npos);
                if (!numericPart.empty() &&
                    std::all_of(numericPart.begin(), numericPart.end(),
                                 ::isdigit)) {
                    k2 = std::stoul(k1_temp.substr(pos + 1, std::string::npos));
                    for (int idx = 0; idx < schema.size(); idx++) {
                        auto &col = schema[idx];
                        if (std::get<0>(col) == k1 && std::get<1>(col) == k2) {
                            found = true;
                            found_colref = std::get<2>(col);
                            break;
                        }
                    }
                }
            }
        }
        
        // if still not found, try k1 = k1+"."+k2, k2 = ""
        if (!found) {
            k1 = k1 + "." + std::to_string(k2);
            k2 = std::numeric_limits<uint64_t>::max();
            for (int idx = 0; idx < schema.size(); idx++) {
                auto &col = schema[idx];
                if (std::get<0>(col) == k1 && std::get<1>(col) == k2) {
                    found = true;
                    found_colref = std::get<2>(col);
                    break;
                }
            }
        }
        if (found_colref == NULL) {
            return NULL;
        }
        D_ASSERT(found == true);
        // in order to change colref from unsed to used
        found_colref->MarkAsUsed();
        // projection code
        CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
        CColRef *prev_colref =
            col_factory->LookupColRef(found_colref->PrevId());
        prev_colref->MarkAsUsed();
        return found_colref;
    }

    vector<CColRef *> getAllColRefsOfKey(string k1)
    {
        vector<CColRef *> result;
        for (int idx = 0; idx < schema.size(); idx++) {
            auto &col = schema[idx];
            if (std::get<0>(col) == k1) {
                // in order to change colref from unsed to used
                std::get<2>(col)->MarkAsUsed();
                result.push_back(std::get<2>(col));
            }
        }
        return result;
    }

    CColRef *getColRefofIndex(int i) { return std::get<2>(schema[i]); }

	// not name, id
    uint64_t getPropertyNameOfColRef(string k1, const CColRef *colref)
    {
        for (int idx = 0; idx < schema.size(); idx++) {
            auto &col = schema[idx];
            if (std::get<0>(col) == k1 && std::get<2>(col) == colref) {
                D_ASSERT(std::get<1>(col) != std::numeric_limits<uint64_t>::max());
                return std::get<1>(col);
            }
        }
        D_ASSERT(false);
    }

    void getOutputNames(vector<string> &result)
    {
        for (int idx = 0; idx < schema.size(); idx++) {
            auto &col = schema[idx];
            if (std::get<1>(col) != std::numeric_limits<uint64_t>::max()) {
                result.push_back(std::get<0>(col) + "." + std::to_string(std::get<1>(col)));
            }
            else {
                result.push_back(std::get<0>(col));
            }
        }
    }

    bool isNodeBound(string k1)
    {
        return bound_nodes.size() > 0 &&
               (bound_nodes.find(k1) != bound_nodes.end());
    }
    bool isEdgeBound(string k1)
    {
        return bound_edges.size() > 0 &&
               (bound_edges.find(k1) != bound_edges.end());
    }
    uint64_t size() { return schema.size(); }

    void clear()
    {
        schema.clear();
        bound_edges.clear();
        bound_nodes.clear();
    }
    bool isEmpty()
    {
        return schema.size() == 0 && bound_edges.size() == 0 &&
               bound_nodes.size() == 0;
    }
    std::string toString()
    {
        std::string output = "SCHEMA => \n";
        for (int idx = 0; idx < schema.size(); idx++) {
            auto &sch = schema[idx];
            output += " - [" + std::to_string(idx) + "]" + std::get<0>(sch);
            if (std::get<1>(sch) != std::numeric_limits<uint64_t>::max()) {
                output += "." + std::to_string(std::get<1>(sch));
            }
            output += "\n";
        }
        return output;
    }
    void getOutputColumns(vector<CColRef *> &output)
    {
        D_ASSERT(output.size() == 0);
        for (auto &sch : schema) {
            output.push_back(std::get<2>(sch));
        }
    }

   private:
    /* Append to the last column of the schema */
    void appendKey(string &k1, uint64_t &k2, CColRef *colref, bool is_node,
                   bool is_edge)
    {
        D_ASSERT(k2 != std::numeric_limits<uint64_t>::max());
        D_ASSERT(!(is_node && is_edge));
        if (is_node) {
            bound_nodes.insert(k1);
        }
        else if (is_edge) {
            bound_edges.insert(k1);
        }
        else {
            D_ASSERT(k2 != 0);
        }  // property with _id indicates _node or _id

        schema.push_back(make_tuple(k1, k2, colref));
    }

    int getIdxOfColRef(CColRef *colref)
    {
        for (int i = 0; i < schema.size(); i++) {
            auto &col = schema[i];
            if (std::get<2>(col) == colref) {
                return i;
            }
        }
        D_ASSERT(false);
    }

    vector<tuple<string, uint64_t, CColRef *>> &getSchema() { return schema; }

   private:
    // vector<tuple<string, string, CColRef*>> schema;
    vector<tuple<string, uint64_t, CColRef *>> schema;
    set<string> bound_nodes;
    set<string> bound_edges;
};

}  // namespace s62