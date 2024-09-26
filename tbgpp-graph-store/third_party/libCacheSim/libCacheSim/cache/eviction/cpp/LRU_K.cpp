#include "abstractRank.hpp"

namespace eviction {
class LRU2 : public abstractRank {
 public:
  LRU2() = default;
};
}  // namespace eviction

#ifdef __cplusplus
extern "C" {
#endif

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

cache_t *LRU2_init(const common_cache_params_t ccache_params, const char *cache_specific_params);
static void LRU2_free(cache_t *cache);
static bool LRU2_get(cache_t *cache, const request_t *req);
static cache_obj_t *LRU2_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *LRU2_insert(cache_t *cache, const request_t *req);
static cache_obj_t *LRU2_to_evict(cache_t *cache, const request_t *req);
static void LRU2_evict(cache_t *cache, const request_t *req);
static obj_id_t LRU2_evict_return_obj(cache_t *cache, const request_t *req);
static bool LRU2_remove(cache_t *cache, const obj_id_t obj_id);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************

cache_t *LRU2_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("LRU-2", ccache_params, cache_specific_params);
  cache->eviction_params = reinterpret_cast<void *>(new eviction::LRU2);

  cache->cache_init = LRU2_init;
  cache->cache_free = LRU2_free;
  cache->get = LRU2_get;
  cache->find = LRU2_find;
  cache->insert = LRU2_insert;
  cache->evict = LRU2_evict;
  cache->evict_return_obj = LRU2_evict_return_obj;
  cache->to_evict = LRU2_to_evict;
  cache->remove = LRU2_remove;

  if (ccache_params.consider_obj_metadata) {
    // Two timestamps for LRU-2
    cache->obj_md_size = sizeof(int64_t) * 2;
  } else {
    cache->obj_md_size = 0;
  }

  return cache;
}

static void LRU2_free(cache_t *cache) {
  delete reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);
  cache_struct_free(cache);
}

static bool LRU2_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

static cache_obj_t *LRU2_find(cache_t *cache, const request_t *req, const bool update_cache) {
  auto *lru2 = reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);

  if (obj != nullptr && update_cache) {
    // Update the timestamps
    if (obj->LRU2.last1_access_time != -1) {
      obj->LRU2.last2_access_time = obj->LRU2.last1_access_time;
    }
    obj->LRU2.last1_access_time = cache->n_req;

    // Update priority in the queue
    auto itr = lru2->itr_map[obj];
    lru2->pq.erase(itr);

    // The object accessed only once has always lower priority than the object accessed twice
    double pri;
    if (obj->LRU2.last2_access_time == -1) {
      pri = static_cast<double>(obj->LRU2.last1_access_time);
    } else {
      pri = static_cast<double>(obj->LRU2.last2_access_time) + static_cast<double>(1073741824);
    }
    itr = lru2->pq.emplace(obj, pri, cache->n_req).first;
    lru2->itr_map[obj] = itr;
  }

  return obj;
}

static cache_obj_t *LRU2_insert(cache_t *cache, const request_t *req) {
  auto *lru2 = reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);

  cache_obj_t *obj = cache_insert_base(cache, req);
  obj->LRU2.last2_access_time = -1;
  obj->LRU2.last1_access_time = cache->n_req;

  double pri = static_cast<double>(obj->LRU2.last1_access_time);
  auto itr = lru2->pq.emplace(obj, pri, cache->n_req).first;
  lru2->itr_map[obj] = itr;

  return obj;
}

static cache_obj_t *LRU2_to_evict(cache_t *cache, const request_t *req) {
  auto *lru2 = reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);
  eviction::pq_node_type p = lru2->peek_lowest_score();

  return p.obj;
}

static void LRU2_evict(cache_t *cache, const request_t *req) {
  auto *lru2 = reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);
  eviction::pq_node_type p = lru2->pop_lowest_score();
  cache_obj_t *obj = p.obj;

  cache_remove_obj_base(cache, obj, true);
}

static obj_id_t LRU2_evict_return_obj(cache_t *cache, const request_t *req) {
  auto *lru2 = reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);

  // Find the first unpinned object to evict
  eviction::pq_node_type victim = lru2->find_erase_unpinned_victim(cache);

  // If all objects are pinned, return an invalid object id
  if (victim.obj == nullptr) {
    return -1;  // Or another sentinel value indicating no object could be evicted
  }

  // Get the object ID before removing the object from the cache
  obj_id_t evicted_obj_id = victim.obj->obj_id;

  // Remove the object from the cache
  cache_remove_obj_base(cache, victim.obj, true);

  return evicted_obj_id;
}

static bool LRU2_remove(cache_t *cache, const obj_id_t obj_id) {
  auto *lru2 = reinterpret_cast<eviction::LRU2 *>(cache->eviction_params);
  return lru2->remove(cache, obj_id);
}

#ifdef __cplusplus
}
#endif