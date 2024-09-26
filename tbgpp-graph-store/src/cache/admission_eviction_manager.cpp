#include "admission_eviction_manager.h"
#include "cache/chunk_cache_manager.h"

#include <chrono>

namespace duckdb {

#ifndef USE_LIGHTNING_CLIENT
void EvictionManager::Touch(ChunkID column_chunk_id)
{
    cache_->n_req++;
    request_t request;
    SetCommonRequestInfo(request);
    request.obj_id = column_chunk_id;
    cache_->find(cache_, &request, true);
    return;
}

// Insert will do touch too.
void EvictionManager::Insert(ChunkID column_chunk_id)
{
    cache_->n_req++;
    request_t request;
    SetCommonRequestInfo(request);
    request.obj_id = column_chunk_id;
    request.obj_size =
        ChunkCacheManager::ccm->GetColumnChunkOffsetSize(column_chunk_id).second;
    // D_ASSERT(request.obj_size > 0, "obj_size should be greater than 0");
    cache_->insert(cache_, &request);
    // D_ASSERT(cache_->get_occupied_byte(cache_) ==
    //              ChunkCacheManager::ccm->GetCurrentCacheSize(),
    //          "Cache size is not consistent between cache manager and eviction "
    //          "manager");
    return;
}
std::shared_ptr<std::vector<ChunkID>> EvictionManager::Evict(
    size_t size_to_evict)
{
    // D_ASSERT(
    //      cache_->get_occupied_byte(cache_),
    //     "Cache size is not consistent between cache manager and eviction "
    //     "manager");
    std::shared_ptr<std::vector<ChunkID>> evicted_column_chunk_ids =
        std::make_shared<std::vector<ChunkID>>();
    uint64_t evicted_size = 0;
    while (evicted_size < size_to_evict) {
        request_t request;
        SetCommonRequestInfo(request);
        obj_id_t obj_id = cache_->evict_return_obj(cache_, &request);
        evicted_column_chunk_ids->push_back(obj_id);
        if (obj_id == -1) {
            return evicted_column_chunk_ids;
        }
        evicted_size +=
            ChunkCacheManager::ccm->GetColumnChunkOffsetSize(obj_id).second;
    }
    return evicted_column_chunk_ids;
}
void EvictionManager::Pin(ChunkID column_chunk_id)
{
    pin_obj(cache_, column_chunk_id);
}
void EvictionManager::UnPin(ChunkID column_chunk_id)
{
    unpin_obj(cache_, column_chunk_id);
}
bool EvictionManager::IsPinned(ChunkID column_chunk_id)
{
    return is_pinned(cache_, column_chunk_id);
}

std::shared_ptr<EvictionManager> EvictionManagerFactory::CreateEvictionManager(
    const std::string &eviction_manager_name)
{
    std::shared_ptr<EvictionManager> eviction_manager;
    if (eviction_manager_name == "LRU") {
        eviction_manager = std::make_shared<LRUEvictionManager>();
    }
    // else if (eviction_manager_name == "ARC") {
    //     eviction_manager = std::make_shared<ARCEvictionManager>();
    // }
    // else if (eviction_manager_name == "LIRS") {
    //     eviction_manager = std::make_shared<LIRSEvictionManager>();
    // }
    // else if (eviction_manager_name == "Sieve") {
    //     eviction_manager = std::make_shared<SieveEvictionManager>();
    // }
    // else if (eviction_manager_name == "LeCaR") {
    //     eviction_manager = std::make_shared<LeCaREvictionManager>();
    // }
    // else if (eviction_manager_name == "LRB") {
    //     eviction_manager = std::make_shared<LRBEvictionManager>();
    // }
    // else if (eviction_manager_name == "GDSF") {
    //     eviction_manager = std::make_shared<GDSFEvictionManager>();
    // }
    // else if (eviction_manager_name == "LHD") {
    //     eviction_manager = std::make_shared<LHDEvictionManager>();
    // }
    // else if (eviction_manager_name == "LFUDA") {
    //     eviction_manager = std::make_shared<LFUDAEvictionManager>();
    // }
    // else if (eviction_manager_name == "Hyperbolic") {
    //     eviction_manager = std::make_shared<HyperbolicEvictionManager>();
    // }
    // else if (eviction_manager_name == "S4LRU") {
    //     eviction_manager = std::make_shared<S4LRUEvictionManager>();
    // }
    // else if (eviction_manager_name == "LFU") {
    //     eviction_manager = std::make_shared<LFUEvictionManager>();
    // }
    // else if (eviction_manager_name == "LRUSP") {
    //     eviction_manager = std::make_shared<LRUSPEvictionManager>();
    // }
    // else if (eviction_manager_name == "LRU2") {
    //     eviction_manager = std::make_shared<LRU2EvictionManager>();
    // }
    // else {
    //     D_ASSERT(false, "Invalid eviction manager name: %s",
    //              eviction_manager_name.c_str());
    // }
    return eviction_manager;
}

void LRUEvictionManager::Init(uint64_t cache_size)
{
    auto default_params = default_common_cache_params();
    default_params.cache_size = cache_size;
    cache_ = LRU_init(default_params, nullptr);
    policy_ = EvictionPolicy::LRU;
}

// void ARCEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = ARC_init(default_params, nullptr);
// }

// void LIRSEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LIRS_init(default_params, nullptr);
// }

// void SieveEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = Sieve_init(default_params, nullptr);
// }

// void LeCaREvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LeCaR_init(default_params, nullptr);
// }

// void LRBEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LRB_init(default_params, nullptr);
// }

// void LRBEvictionManager::Insert(ChunkID column_chunk_id)
// {
//     cache_->n_req++;

//     request_t request;
//     SetCommonRequestInfo(request);
//     request.obj_id = column_chunk_id;
//     request.obj_size =
//         ChunkCacheManager::ccm.GetColumnChunkOffsetSize(column_chunk_id).second;
//     auto ret = cache_->find(cache_, &request, true);
//     D_ASSERT(ret == NULL,
//              "LRB, The column chunk to be inserted should not be in the cache");
//     DPRINTF("Inserting column chunk %lu with size %lu", column_chunk_id,
//             request.obj_size);
//     D_ASSERT(request.obj_size > 0, "obj_size should be greater than 0");
//     cache_->insert(cache_, &request);
//     D_ASSERT(cache_->get_occupied_byte(cache_) ==
//                  ChunkCacheManager::ccm.GetCurrentCacheSize(),
//              "Cache size is not consistent between cache manager and eviction "
//              "manager");
//     return;
// }

// void GDSFEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = GDSF_init(default_params, nullptr);
// }

// void LHDEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LHD_init(default_params, nullptr);
// }

// void LFUDAEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LFUDA_init(default_params, nullptr);
// }

// void HyperbolicEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = Hyperbolic_init(default_params, nullptr);
// }

// void S4LRUEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = S4LRU_init(default_params, nullptr);
// }

// void S4LRUEvictionManager::Pin(ChunkID column_chunk_id)
// {
//     pin_obj(cache_, column_chunk_id);
//     S4LRU_params_t *params = (S4LRU_params_t *)cache_->eviction_params;
//     pin_obj(params->segments[0], column_chunk_id);
//     pin_obj(params->segments[1], column_chunk_id);
//     pin_obj(params->segments[2], column_chunk_id);
//     pin_obj(params->segments[3], column_chunk_id);
// }
// void S4LRUEvictionManager::UnPin(ChunkID column_chunk_id)
// {
//     unpin_obj(cache_, column_chunk_id);
//     S4LRU_params_t *params = (S4LRU_params_t *)cache_->eviction_params;
//     unpin_obj(params->segments[0], column_chunk_id);
//     unpin_obj(params->segments[1], column_chunk_id);
//     unpin_obj(params->segments[2], column_chunk_id);
//     unpin_obj(params->segments[3], column_chunk_id);
// }

// void LFUEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LFU_init(default_params, nullptr);
// }

// void LRUSPEvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LRU_SP_init(default_params, nullptr);
// }

// void LRU2EvictionManager::Init()
// {
//     auto default_params = default_common_cache_params();
//     default_params.cache_size = TOTAL_CACHE_SIZE;
//     cache_ = LRU2_init(default_params, nullptr);
// }

void SetCommonRequestInfo(request_t &request)
{
    request.valid = true;
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(duration);
    int64_t timeInt64 = microseconds.count();
    request.clock_time = timeInt64;
    request.hv = 0;
    request.ttl = -1;
}

#endif

}  // namespace duckdb