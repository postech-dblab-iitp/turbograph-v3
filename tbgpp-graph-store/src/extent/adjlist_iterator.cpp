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
    
    if (!ext_it ->IsInitialized()) {
        // ext_it = std::make_shared<ExtentIterator>();
        ext_it->Initialize(context, nullptr, target_types, target_idxs, target_eid);
        eid_to_bufptr_idx_map->insert( {target_eid, 0} );
        // icecream::ic.enable(); IC(); fprintf(stdout, "%p\n", ext_it); IC(target_eid, 0); icecream::ic.disable();
        return false;
    } else {
        if (eid_to_bufptr_idx_map->find(target_eid) != eid_to_bufptr_idx_map->end()) {
            // icecream::ic.enable(); IC(); icecream::ic.disable();
            if (eid_to_bufptr_idx_map->at(target_eid) != -1) {
                // Find! Nothing to do
                // icecream::ic.enable(); IC(); icecream::ic.disable();
                return true;
            } else {
                // Evicted extent
                ExtentID evicted_eid;
                int bufptr_idx = ext_it->RequestNewIO(context, nullptr, target_types, target_idxs, target_eid, evicted_eid);
                eid_to_bufptr_idx_map->at(target_eid) = bufptr_idx;
                // eid_to_bufptr_idx_map[target_eid] = bufptr_idx;
                // icecream::ic.enable(); IC(); fprintf(stdout, "%p\n", ext_it); IC(target_eid, bufptr_idx, evicted_eid); icecream::ic.disable();
                if (evicted_eid != std::numeric_limits<ExtentID>::max()) (*eid_to_bufptr_idx_map)[evicted_eid] = -1;
                return false;
            }
        } else {
            // icecream::ic.enable(); IC(); icecream::ic.disable();
            // Fail to find
            ExtentID evicted_eid;
            int bufptr_idx = ext_it->RequestNewIO(context, nullptr, target_types, target_idxs, target_eid, evicted_eid);
            eid_to_bufptr_idx_map->insert( {target_eid, bufptr_idx} );
            // icecream::ic.enable(); IC(); fprintf(stdout, "%p\n", ext_it); IC(target_eid, bufptr_idx, evicted_eid); icecream::ic.disable();
            if (evicted_eid != std::numeric_limits<ExtentID>::max()) (*eid_to_bufptr_idx_map)[evicted_eid] = -1;
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

void AdjacencyListIterator::getAdjListPtr(uint64_t vid, ExtentID target_eid, uint64_t *&start_ptr, uint64_t *&end_ptr, bool is_initialized) {
    idx_t target_seqno = vid & 0x00000000FFFFFFFF;
    
    D_ASSERT(eid_to_bufptr_idx_map->at(target_eid) != -1);
    ext_it->GetExtent(cur_adj_list, eid_to_bufptr_idx_map->at(target_eid), is_initialized);
    idx_t *adjListBase = (idx_t *)cur_adj_list;
    idx_t start_idx = target_seqno == 0 ? STORAGE_STANDARD_VECTOR_SIZE : adjListBase[target_seqno - 1];
    idx_t end_idx = adjListBase[target_seqno];
    start_ptr = adjListBase + start_idx;
    end_ptr = adjListBase + end_idx;
}

void DFSIterator::initialize(ClientContext &context, uint64_t src_id, uint64_t adj_col_idx) {
    current_lv = 0;
    adjColIdx = adj_col_idx;
    initializeDSForNewLv(0);

    ExtentID target_eid = src_id >> 32;
    bool is_initialized = adjlist_iter_per_level[current_lv]->Initialize(context, adjColIdx, target_eid, LogicalType::FORWARD_ADJLIST); // TODO adjColIdx, adjlist direction
    // fprintf(stdout, "target_eid = %d, initialized = %s\n", target_eid, is_initialized ? "true" : "false");
    adjlist_iter_per_level[current_lv]->getAdjListPtr(src_id, target_eid, cur_start_end_offsets_per_level[current_lv].first,
        cur_start_end_offsets_per_level[current_lv].second, is_initialized);
    for (int lv = 0; lv < cursor_per_level.size(); lv++) cursor_per_level[lv] = 0;
}

bool DFSIterator::getNextEdge(ClientContext &context, int lv, uint64_t &tgt, uint64_t &edge) {
    idx_t cur_pos = cursor_per_level[lv];
    int64_t adj_size = (cur_start_end_offsets_per_level[lv].second - cur_start_end_offsets_per_level[lv].first) / 2;
    uint64_t *cur_adjlist_ptr = cur_start_end_offsets_per_level[lv].first;

    if (cur_pos < adj_size) {
        tgt = cur_adjlist_ptr[cur_pos * 2];
        edge = cur_adjlist_ptr[cur_pos * 2 + 1];
        cursor_per_level[lv]++;
        changeLevel(context, true, tgt);
        return true;
    } else {
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
        adjlist_iter_per_level[current_lv]->getAdjListPtr(src_id, target_eid, cur_start_end_offsets_per_level[current_lv].first,
            cur_start_end_offsets_per_level[current_lv].second, is_initialized);
    } else {
        current_lv--;
        // if (current_lv < 0) return;
    }
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

}