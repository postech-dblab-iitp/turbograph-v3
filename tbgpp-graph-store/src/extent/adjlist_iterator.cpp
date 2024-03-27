#include "extent/adjlist_iterator.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "cache/disk_aio/TypeDef.hpp"
#include "cache/chunk_cache_manager.h"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "common/types/data_chunk.hpp"
#include "extent/compression/compression_function.hpp"

#include "icecream.hpp" 

namespace duckdb {

bool AdjacencyListIterator::Initialize(ClientContext &context, int adjColIdx, ExtentID target_eid, LogicalType adjlist_type) {
    if (is_initialized && target_eid == cur_eid) return true;

    vector<LogicalType> target_types { adjlist_type };
	vector<idx_t> target_idxs { (idx_t)adjColIdx };
    
    cur_eid = target_eid;
    is_initialized = true;
    auto target_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eid);
    if (target_eid_seqno > eid_to_bufptr_idx_map->size()) {
        eid_to_bufptr_idx_map->resize(target_eid_seqno + 1, INVALID_PTR_ADJ_IDX_PAIR);
    }
    
    if (!ext_it ->IsInitialized()) {
        ext_it->Initialize(context, target_types, target_idxs, target_eid); // TODO
        (*eid_to_bufptr_idx_map)[target_eid_seqno] = std::make_pair<idx_t, data_ptr_t>(0, nullptr);
        return false;
    } else {
        auto pair = (*eid_to_bufptr_idx_map)[target_eid_seqno];
        if (pair != INVALID_PTR_ADJ_IDX_PAIR) {
            if (pair.first != -1) {
                // Find! Nothing to do
                return true;
            } else {
                // Evicted extent
                ExtentID evicted_eid;
                int bufptr_idx = ext_it->RequestNewIO(context, target_types, target_idxs, target_eid, evicted_eid);
                pair.first = bufptr_idx;
                pair.second = nullptr;
            }
        }
        else {
            // Fail to find
            ExtentID evicted_eid;
            int bufptr_idx = ext_it->RequestNewIO(context, target_types, target_idxs, target_eid, evicted_eid);
            (*eid_to_bufptr_idx_map)[target_eid_seqno] = std::make_pair<idx_t, data_ptr_t>(bufptr_idx, nullptr);
            return false;
        }
    }
    
    return true;
}

void AdjacencyListIterator::Initialize(ClientContext &context, int adjColIdx, DataChunk &input, idx_t srcColIdx, LogicalType adjlist_type) {
    D_ASSERT(false);
    // vector<ExtentID> target_eids;
    // uint64_t *vids = (uint64_t *)input.data[srcColIdx].GetData();
    // ExtentID prev_eid = input.size() == 0 ? 0 : (UBigIntValue::Get(input.GetValue(nodeColIdx, 0)) >> 32);
    // for (size_t i = 0; i < input.size(); i++) {
    //     uint64_t vid = vids[i];
	// 	ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	// 	target_eids.push_back(target_eid);
    // }
    // ExtentID target_eid = vid >> 32;

    // if (is_initialized && target_eid == cur_eid) return;
    // icecream::ic.enable(); IC(); IC(target_eid); icecream::ic.disable();

    // vector<LogicalType> target_types { adjlist_type };
	// vector<idx_t> target_idxs { (idx_t)adjColIdx };
    
    // ext_it = new ExtentIterator();
    // ext_it->Initialize(context, nullptr, target_types, target_idxs, target_eid);
    // // icecream::ic.enable(); IC(); icecream::ic.disable();
    // cur_eid = target_eid;
    // is_initialized = true;
}

void AdjacencyListIterator::getAdjListRange(uint64_t vid, uint64_t *start_idx, uint64_t *end_idx) {
    D_ASSERT(false);
    // idx_t target_seqno = vid & 0x00000000FFFFFFFF;
    // data_ptr_t adj_list;
    // ext_it->GetExtent(adj_list);
    // idx_t *adjListBase = (idx_t *)adj_list;
    // *start_idx = target_seqno == 0 ? STORAGE_STANDARD_VECTOR_SIZE : adjListBase[target_seqno - 1];
    // *end_idx = adjListBase[target_seqno];
}

void AdjacencyListIterator::getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t **start_ptr, uint64_t **end_ptr, bool is_initialized) {
    idx_t target_seqno = GET_SEQNO_FROM_PHYSICAL_ID(vid);
    auto target_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eid);
    
    D_ASSERT((*eid_to_bufptr_idx_map)[target_eid_seqno].first != -1);
    auto &bufptr_adjidx_pair = (*eid_to_bufptr_idx_map)[target_eid_seqno];
    if (!bufptr_adjidx_pair.second) {
        ext_it->GetExtent(cur_adj_list, bufptr_adjidx_pair.first, is_initialized);
        bufptr_adjidx_pair.second = cur_adj_list;
        adjListBase = (idx_t *)cur_adj_list;
    } else {
        adjListBase = (idx_t *)bufptr_adjidx_pair.second;
    }
    *start_ptr = adjListBase + (target_seqno == 0 ? STORAGE_STANDARD_VECTOR_SIZE : adjListBase[target_seqno - 1]);
    *end_ptr = adjListBase + adjListBase[target_seqno];
}

void DFSIterator::initialize(ClientContext &context, uint64_t src_id, uint64_t adj_col_idx) {
    current_lv = 0;
    adjColIdx = adj_col_idx;
    initializeDSForNewLv(0);

    ExtentID target_eid = src_id >> 32;
    bool is_initialized = adjlist_iter_per_level[current_lv]->Initialize(context, adjColIdx, target_eid, LogicalType::FORWARD_ADJLIST); // TODO adjColIdx, adjlist direction
    // fprintf(stdout, "target_eid = %d, initialized = %s\n", target_eid, is_initialized ? "true" : "false");
    adjlist_iter_per_level[current_lv]->getAdjListPtr(src_id, target_eid, &cur_start_end_offsets_per_level[current_lv].first,
        &cur_start_end_offsets_per_level[current_lv].second, is_initialized);
    for (int lv = 0; lv < cursor_per_level.size(); lv++) cursor_per_level[lv] = 0;
}

bool DFSIterator::getNextEdge(ClientContext &context, int lv, uint64_t &tgt, uint64_t &edge) {
    idx_t cur_pos = cursor_per_level[lv];
    int64_t adj_size = (cur_start_end_offsets_per_level[lv].second - cur_start_end_offsets_per_level[lv].first) / 2;
    uint64_t *cur_adjlist_ptr = cur_start_end_offsets_per_level[lv].first;

    // fprintf(stdout, "[lv: %d/%d] cur_pos = %ld, adj_size = %ld\n", current_lv, lv, cur_pos, adj_size);

    if (cur_pos < adj_size) {
        tgt = cur_adjlist_ptr[cur_pos * 2];
        edge = cur_adjlist_ptr[cur_pos * 2 + 1];
        cursor_per_level[lv]++;
        changeLevel(context, true, tgt);
        return true;
    } else {
        cursor_per_level[lv] = 0;
        changeLevel(context, false);
        return false;
    }
}

void DFSIterator::changeLevel(ClientContext &context, bool traverse_child, uint64_t src_id) {
    ExtentID target_eid = src_id >> 32;
    if (traverse_child) {
        current_lv++;
        if (current_lv > max_lv) {
            initializeDSForNewLv(current_lv);
        }
        bool is_initialized = adjlist_iter_per_level[current_lv]->Initialize(context, adjColIdx, target_eid, LogicalType::FORWARD_ADJLIST);
        adjlist_iter_per_level[current_lv]->getAdjListPtr(src_id, target_eid, &cur_start_end_offsets_per_level[current_lv].first,
            &cur_start_end_offsets_per_level[current_lv].second, is_initialized);
    } else {
        current_lv--;
        // if (current_lv < 0) return;
    }
}

void DFSIterator::reduceLevel() {
    current_lv--;
}

void DFSIterator::initializeDSForNewLv(int new_lv) {
    max_lv = new_lv;

    if (adjlist_iter_per_level.size() < max_lv + 1) {
        cur_adjlist_ptr_per_level.push_back(nullptr);
        cur_start_end_offsets_per_level.push_back(std::make_pair<uint64_t *, uint64_t *>(nullptr, nullptr));
        cursor_per_level.push_back(0);
        adjlist_iter_per_level.push_back(new AdjacencyListIterator(this->ext_it, this->eid_to_bufptr_idx_map));

        D_ASSERT(cur_adjlist_ptr_per_level.size() == max_lv + 1);
        D_ASSERT(cur_start_end_offsets_per_level.size() == max_lv + 1);
        D_ASSERT(cursor_per_level.size() == max_lv + 1);
        D_ASSERT(adjlist_iter_per_level.size() == max_lv + 1);
    }
}

ShortestPathIterator::ShortestPathIterator() {
    ext_it = std::make_shared<ExtentIterator>();
    eid_to_bufptr_idx_map = std::make_shared<vector<BufPtrAdjIdxPair>>();
    eid_to_bufptr_idx_map->resize(INITIAL_EXTENT_ID_SPACE, INVALID_PTR_ADJ_IDX_PAIR);
}

ShortestPathIterator::~ShortestPathIterator() {}

void ShortestPathIterator::initialize(ClientContext &context, NodeID src_id, NodeID tgt_id, uint64_t adj_col_idx, Level lower_bound, Level upper_bound) {
    if (src_id == tgt_id) { D_ASSERT(false); }; // Source and target are the same

    srcId = src_id;
    tgtId = tgt_id;
    adjColIdx = adj_col_idx;
    lowerBound = lower_bound;
    upperBound = upper_bound;
    predecessor.clear();
    predecessor[srcId] = {src_id, 0}; // Source node is its own predecessor
    adjlist_iterator = std::make_shared<AdjacencyListIterator>(this->ext_it, this->eid_to_bufptr_idx_map);

    // Start the BFS from the source node
    std::queue<std::pair<NodeID, Level>> queue;
    queue.push(make_pair(srcId, 0));

    while (!queue.empty()) {
        std::pair<NodeID, Level> current = queue.front();
        queue.pop();
        if (current.second >= upper_bound) { continue; }
        bool found = enqueueNeighbors(context, current.first, current.second, queue);
        if (found) { break; }
    }
}

bool ShortestPathIterator::enqueueNeighbors(ClientContext &context, NodeID node_id, Level node_level, std::queue<std::pair<NodeID, Level>>& queue) {
    uint64_t *start_ptr, *end_ptr;
    ExtentID target_eid = node_id >> 32;
    bool is_initialized = adjlist_iterator->Initialize(context, adjColIdx, target_eid, LogicalType::FORWARD_ADJLIST);
    adjlist_iterator->getAdjListPtr(node_id, target_eid, &start_ptr, &end_ptr, is_initialized);

    for (uint64_t *ptr = start_ptr; ptr < end_ptr; ptr += 2) {
        uint64_t neighbor = *ptr;
        uint64_t edge_id = *(ptr + 1);

        // If found
        if (neighbor == tgtId && node_level + 1 >= lowerBound) {
            predecessor[neighbor] = {node_id, edge_id};  // Set the current node and edge as the predecessor of the neighbor
            return true;
        }

        // If the neighbor has not been visited
        if (predecessor.find(neighbor) == predecessor.end()) {
            queue.push(make_pair(neighbor, node_level + 1));
            predecessor[neighbor] = {node_id, edge_id};  // Set the current node and edge as the predecessor of the neighbor
        }
    }

    return false;
}


bool ShortestPathIterator::getShortestPath(ClientContext &context, std::vector<uint64_t>& edges, std::vector<uint64_t>& nodes) {
    if (predecessor.find(tgtId) == predecessor.end()) {
        return false; // Target not reachable from source
    }

    // Reconstruct the path from target to source
    uint64_t current = tgtId;
    while (current != srcId) {
        nodes.push_back(current);
        edges.push_back(predecessor[current].second);
        current = predecessor[current].first;
    }
    nodes.push_back(srcId);

    // Reverse the path to get the correct order from source to target
    std::reverse(nodes.begin(), nodes.end());
    std::reverse(edges.begin(), edges.end());

    return true;
}

}