#ifndef ADJLIST_ITERATOR_H
#define ADJLIST_ITERATOR_H

#include "common/common.hpp"
#include "common/vector.hpp"
#include "common/unordered_map.hpp"

#include "common/types.hpp"
#include "common/vector_size.hpp"

#include "extent/extent_iterator.hpp"
#include <queue>
#include <unordered_map>

#include <limits>

namespace s62 {

class Value;
class DataChunk;
class LogicalType;
class ClientContext;

#define BufPtrAdjIdxPair std::pair<idx_t, data_ptr_t>
const BufPtrAdjIdxPair INVALID_PTR_ADJ_IDX_PAIR = std::make_pair<idx_t, data_ptr_t>(-2, nullptr);

class AdjacencyListIterator {
public:
    AdjacencyListIterator() {
        ext_it = std::make_shared<ExtentIterator>();
        eid_to_bufptr_idx_map = std::make_shared<vector<BufPtrAdjIdxPair>>();
        eid_to_bufptr_idx_map->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);
    }
    ~AdjacencyListIterator() {}
    AdjacencyListIterator(std::shared_ptr<ExtentIterator> share_ext_it, std::shared_ptr<vector<BufPtrAdjIdxPair>> share_eid_to_bufptr_idx_map) {
        ext_it = share_ext_it;
        eid_to_bufptr_idx_map = share_eid_to_bufptr_idx_map;
    }
    bool Initialize(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd);
    void getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t **start_ptr, uint64_t **end_ptr, bool is_initialized);
    int requestNewAdjList(ClientContext &context, int adjColIdx, ExtentID target_eid, bool is_fwd);

private:
    bool is_initialized = false;
    std::shared_ptr<ExtentIterator> ext_it;
    ExtentID cur_eid = std::numeric_limits<ExtentID>::max();
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map;
    data_ptr_t cur_adj_list;
    idx_t *adjListBase;
    vector<LogicalType> fwd_types { LogicalType::FORWARD_ADJLIST };
    vector<LogicalType> bwd_types { LogicalType::BACKWARD_ADJLIST };
};

class DFSIterator {
public:
    DFSIterator() {
        ext_it = std::make_shared<ExtentIterator>();
        eid_to_bufptr_idx_map = std::make_shared<vector<BufPtrAdjIdxPair>>();
        eid_to_bufptr_idx_map->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);
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
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map;
};

typedef uint64_t NodeID;
typedef uint64_t EdgeID;
typedef uint64_t Level;

class ShortestPathIterator {
public:
    ShortestPathIterator();
    ~ShortestPathIterator();

    void initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx, Level lower_bound, Level upper_bound);
    bool getShortestPath(ClientContext &context, std::vector<EdgeID>& edges, std::vector<NodeID>& nodes);

private:
    uint64_t srcId, tgtId;
    uint64_t adjColIdx;
    Level lowerBound;
    Level upperBound;
    std::unordered_map<NodeID, std::pair<NodeID, EdgeID>> predecessor;  // Node ID mapped to a pair of its predecessor and the connecting edge ID
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator;
    std::shared_ptr<ExtentIterator> ext_it = nullptr;
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map;

    bool enqueueNeighbors(ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue);
};

class ShortestPathAdvancedIterator {
public:
    ShortestPathAdvancedIterator();
    ~ShortestPathAdvancedIterator();

    void initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_forward, uint64_t adj_col_idx_backward, Level lower_bound, Level upper_bound);
    bool getShortestPath(ClientContext &context, std::vector<EdgeID>& edges, std::vector<NodeID>& nodes);

private:
    struct NodeIDHasher {
        std::size_t operator()(const NodeID& k) const {
            return GET_SEQNO_FROM_PHYSICAL_ID(k);
        }
    };

private:
    const NodeID INVALID_NODE_ID = -1;
    NodeID meeting_point;
    NodeID src_id, tgt_id;
    uint64_t adj_col_idx_fwd;
    uint64_t adj_col_idx_bwd;
    Level lower_bound;
    Level upper_bound;
    std::unordered_map<NodeID, std::pair<NodeID, EdgeID>, NodeIDHasher> predecessor_forward;
    std::unordered_map<NodeID, std::pair<NodeID, EdgeID>, NodeIDHasher> predecessor_backward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_forward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_backward;
    std::shared_ptr<ExtentIterator> ext_it_forward = nullptr;
    std::shared_ptr<ExtentIterator> ext_it_backward = nullptr;
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map_forward;
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map_backward;

    bool biDirectionalSearch(ClientContext &context);
    bool enqueueNeighbors(ClientContext &context, NodeID current_node, Level node_level, std::queue<std::pair<NodeID, Level>>& queue, bool is_forward);
};

class AllShortestPathIterator {
public:
    AllShortestPathIterator();
    ~AllShortestPathIterator();

    void initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx_forward, uint64_t adj_col_idx_backward, Level lower_bound, Level upper_bound);
    bool getAllShortestPaths(ClientContext &context, std::vector<std::vector<EdgeID>> &all_edges, std::vector<std::vector<NodeID>> &all_nodes);

private:
    struct NodeIDHasher {
        std::size_t operator()(const NodeID &k) const {
            return GET_SEQNO_FROM_PHYSICAL_ID(k);
        }
    };

    const NodeID INVALID_NODE_ID = -1;
    const NodeID INVALID_EDGE_ID = -1;
    std::unordered_set<NodeID> meeting_points;
    NodeID src_id, tgt_id;
    uint64_t adj_col_idx_fwd;
    uint64_t adj_col_idx_bwd;
    Level lower_bound;
    Level upper_bound;

    std::unordered_map<NodeID, std::vector<std::pair<NodeID, EdgeID>>, NodeIDHasher> predecessor_forward;
    std::unordered_map<NodeID, std::vector<std::pair<NodeID, EdgeID>>, NodeIDHasher> predecessor_backward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_forward;
    std::shared_ptr<AdjacencyListIterator> adjlist_iterator_backward;
    std::shared_ptr<ExtentIterator> ext_it_forward = nullptr;
    std::shared_ptr<ExtentIterator> ext_it_backward = nullptr;
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map_forward;
    std::shared_ptr<vector<BufPtrAdjIdxPair>> eid_to_bufptr_idx_map_backward;

    std::vector<std::vector<NodeID>> constructPaths();

    bool biDirectionalSearch(ClientContext &context);
    bool enqueueNeighbors(ClientContext &context, NodeID current_node, Level node_level, std::queue<std::pair<NodeID, Level>> &queue, bool is_forward);
};


} // namespace s62

#endif
