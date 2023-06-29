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

typedef vector<int64_t> SchemaKeyIDs;

struct TupleGroup {
    int64_t tuple_group_id;
    SchemaKeyIDs& schema_key_ids;
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

    void find(SchemaKeyIDs& schema_key_ids, int64_t& tuple_group_id) {
        int hashValue = std::accumulate(schema_key_ids.begin(), schema_key_ids.end(), 0);
        hashValue %= size;
        for (auto& tupleGroup : table[hashValue]) {
            if (tupleGroup.schema_key_ids == schema_key_ids) {
                tuple_group_id = tupleGroup.tuple_group_id;
                return;
            }
        }
        tuple_group_id = INVALID_TUPLE_GROUP_ID;
        return;
    }

    void insert(SchemaKeyIDs& schema_key_ids, int group_id) {
        int hashValue = std::accumulate(schema_key_ids.begin(), schema_key_ids.end(), 0);
        hashValue %= size;
        int64_t tuple_group_id = INVALID_TUPLE_GROUP_ID;
        find(schema_key_ids, tuple_group_id);
        if (tuple_group_id != INVALID_TUPLE_GROUP_ID) { return; }
        else { table[hashValue].push_back({group_id, schema_key_ids}); }
    }

    // Getters
    int getSize() const {
        return size;
    }

    TupleGroups& getSlot(int index) {
        return table[index];
    }
};
