#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <tuple>
#include <fstream>
#include <cmath>
#include <iomanip>
#include <algorithm>
#include <chrono>
#include <list>
#include <numeric>
using namespace std;

typedef unordered_set<uint32_t> SchemaKeyIDs;
typedef vector<uint32_t> SchemaKeyIDsVec;

struct TupleGroup {
    int64_t tuple_group_id;
    SchemaKeyIDs schema_key_ids;
};

typedef vector<TupleGroup> TupleGroups;

const int64_t INVALID_TUPLE_GROUP_ID = -1;

inline bool isSchemaFound(int64_t group_id) {
    return group_id != INVALID_TUPLE_GROUP_ID;
}

// Hash table implementation using separate chaining
class SchemaHashTable {
private:
    std::vector<TupleGroups> table;
    int size;

public:
    SchemaHashTable() : size(1) {
        table.resize(size);
    }

    SchemaHashTable(int tableSize) : size(tableSize) {
        table.resize(size);
    }

    uint64_t hashFunction(SchemaKeyIDs &schema_key_ids) {
        uint64_t hashValue = std::accumulate(schema_key_ids.begin(), schema_key_ids.end(), 0);
        hashValue %= size;
        return hashValue;
    }

    uint64_t hashFunction(SchemaKeyIDsVec &schema_key_ids) {
        uint64_t hashValue = std::accumulate(schema_key_ids.begin(), schema_key_ids.end(), 0);
        hashValue %= size;
        return hashValue;
    }

    void resize(int tableSize) {
        size = tableSize;
        table.resize(tableSize);
    }

    void find(SchemaKeyIDs &schema_key_ids, int64_t &tuple_group_id) {
        uint64_t hashValue = hashFunction(schema_key_ids);
        for (auto& tupleGroup : table[hashValue]) {
            if (tupleGroup.schema_key_ids == schema_key_ids) {
                tuple_group_id = tupleGroup.tuple_group_id;
                return;
            }
        }
        tuple_group_id = INVALID_TUPLE_GROUP_ID;
        return;
    }

    void find(SchemaKeyIDsVec &schema_key_ids, int64_t &tuple_group_id) {
        uint64_t hashValue = hashFunction(schema_key_ids);
        bool found = false;
        for (auto& tupleGroup : table[hashValue]) {
            if (tupleGroup.schema_key_ids.size() != schema_key_ids.size()) {
                continue;
            } else {
                found = true;
                for (int64_t i = 0; i < schema_key_ids.size(); i++) {
                    if (tupleGroup.schema_key_ids.find(schema_key_ids[i]) == tupleGroup.schema_key_ids.end()) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    tuple_group_id = tupleGroup.tuple_group_id;
                    return;
                } else {
                    found = false;
                    continue;
                }
            }
        }
        tuple_group_id = INVALID_TUPLE_GROUP_ID;
        return;
    }

    void insert(SchemaKeyIDs &schema_key_ids, int group_id) {
        int64_t tuple_group_id = INVALID_TUPLE_GROUP_ID;
        if (tuple_group_id != INVALID_TUPLE_GROUP_ID) { 
            return; 
        }
        else { 
            uint64_t hashValue = hashFunction(schema_key_ids);
            table[hashValue].push_back({group_id, schema_key_ids}); 
        }
    }

    void insert(SchemaKeyIDsVec &schema_key_ids, int group_id) {
        int64_t tuple_group_id = INVALID_TUPLE_GROUP_ID;
        if (tuple_group_id != INVALID_TUPLE_GROUP_ID) { 
            return; 
        } else { 
            uint64_t hashValue = hashFunction(schema_key_ids);
            std::unordered_set<uint32_t> tmp_set;
            std::copy(schema_key_ids.begin(), schema_key_ids.end(),
                      std::inserter(tmp_set, tmp_set.end()));
            table[hashValue].push_back({group_id, tmp_set}); 
        }
    }

    // Getters
    int getSize() const {
        return size;
    }

    TupleGroups& getSlot(int index) {
        return table[index];
    }
};
