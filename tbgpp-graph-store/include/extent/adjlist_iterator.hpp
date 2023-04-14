#ifndef ADJLIST_ITERATOR_H
#define ADJLIST_ITERATOR_H

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"

#include "common/types.hpp"
#include "common/vector_size.hpp"

#include "extent/extent_iterator.hpp"

#include <limits>

namespace duckdb {

class Value;
class DataChunk;
class LogicalType;
class ClientContext;

class AdjacencyListIterator {
public:
    AdjacencyListIterator() {
        ext_it = std::make_shared<ExtentIterator>();
        eid_to_bufptr_idx_map = std::make_shared<unordered_map<ExtentID, int>>();
    }
    ~AdjacencyListIterator() {}
    AdjacencyListIterator(std::shared_ptr<ExtentIterator> share_ext_it, std::shared_ptr<unordered_map<ExtentID, int>> share_eid_to_bufptr_idx_map) {
        ext_it = share_ext_it;
        eid_to_bufptr_idx_map = share_eid_to_bufptr_idx_map;
    }
    bool Initialize(ClientContext &context, int adjColIdx, ExtentID target_eid, LogicalType adjlist_type = LogicalType::FORWARD_ADJLIST);
    void Initialize(ClientContext &context, int adjColIdx, DataChunk &input, idx_t srcColIdx, LogicalType adjlist_type = LogicalType::FORWARD_ADJLIST);
    void getAdjListRange(uint64_t vid, uint64_t *start_idx, uint64_t *end_idx);
    void getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t *&start_ptr, uint64_t *&end_ptr, bool is_initialized);

private:
    bool is_initialized = false;
    std::shared_ptr<ExtentIterator> ext_it;
    ExtentID cur_eid = std::numeric_limits<ExtentID>::max();
    std::shared_ptr<unordered_map<ExtentID, int>> eid_to_bufptr_idx_map;
    data_ptr_t cur_adj_list;
};

class DFSIterator {
public:
    DFSIterator() {
        ext_it = std::make_shared<ExtentIterator>();
        eid_to_bufptr_idx_map = std::make_shared<unordered_map<ExtentID, int>>();
    }
    ~DFSIterator() {}

    void initialize(ClientContext &context, uint64_t src_id, uint64_t adj_col_idx);
    bool getNextEdge(ClientContext &context, int lv, uint64_t &tgt, uint64_t &edge);
    void reduceLevel();

private:
    void changeLevel(ClientContext &context, bool traverse_child, uint64_t src_id = 0);
    void initializeDSForNewLv(int new_lv);

    int current_lv = 0;
    int max_lv = 0;
    int adjColIdx;

    vector<AdjacencyListIterator *> adjlist_iter_per_level;
    vector<idx_t *> cur_adjlist_ptr_per_level;
    vector<std::pair<uint64_t *, uint64_t *>> cur_start_end_offsets_per_level;
    vector<idx_t> cursor_per_level;
    vector<ExtentIterator *> ext_it_per_level;
    std::shared_ptr<ExtentIterator> ext_it = nullptr;
    std::shared_ptr<unordered_map<ExtentID, int>> eid_to_bufptr_idx_map;
};

} // namespace duckdb

#endif
