#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif


// Function declarations
static void S4LRU_free(cache_t *cache);
static bool S4LRU_get(cache_t *cache, const request_t *req);
static cache_obj_t *S4LRU_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *S4LRU_insert(cache_t *cache, const request_t *req);
static cache_obj_t *S4LRU_to_evict(cache_t *cache, const request_t *req);
static void S4LRU_evict(cache_t *cache, const request_t *req);
static obj_id_t S4LRU_evict_return_obj(cache_t *cache, const request_t *req);
static bool S4LRU_remove(cache_t *cache, const obj_id_t obj_id);
// static void S4LRU_segment_admit(cache_t *cache, S4LRU_params_t *params, int idx, const request_t *req);

// Initialize S4LRU cache
cache_t *S4LRU_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("S4LRU", ccache_params, cache_specific_params);
  cache->cache_init = S4LRU_init;
  cache->cache_free = S4LRU_free;
  cache->get = S4LRU_get;
  cache->find = S4LRU_find;
  cache->insert = S4LRU_insert;
  cache->evict = S4LRU_evict;
  cache->remove = S4LRU_remove;
  cache->to_evict = S4LRU_to_evict;
  cache->evict_return_obj = S4LRU_evict_return_obj;

  S4LRU_params_t *params = (S4LRU_params_t *)malloc(sizeof(S4LRU_params_t));
  params->total_size = ccache_params.cache_size;
  params->segment_size = params->total_size / 4;

  for (int i = 0; i < 4; i++) {
    common_cache_params_t segment_params = ccache_params;
    params->segments[i] = LRU_init(segment_params, NULL);
  }

  cache->eviction_params = params;
  return cache;
}

// Free S4LRU cache
static void S4LRU_free(cache_t *cache) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  for (int i = 0; i < 4; i++) {
    params->segments[i]->cache_free(params->segments[i]);
  }
  free(params);
  cache_struct_free(cache);
}

// S4LRU get operation
static bool S4LRU_get(cache_t *cache, const request_t *req) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  for (int i = 0; i < 4; i++) {
    if (params->segments[i]->get(params->segments[i], req)) {
      if (i < 3) {
        params->segments[i]->remove(params->segments[i], req->obj_id);
        // S4LRU_segment_admit(cache, params, i + 1, req);
      }
      return true;
    }
  }
  return false;
}

// S4LRU insert operation
static cache_obj_t *S4LRU_insert(cache_t *cache, const request_t *req) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  cache->occupied_byte += req->obj_size;
  cache_obj_t *cache_obj = hashtable_insert(cache->hashtable, req);
  return params->segments[0]->insert(params->segments[0], req);
}

// // Admit object to a specific segment and handle evictions
// static void S4LRU_segment_admit(cache_t *cache, S4LRU_params_t *params, int idx, const request_t *req) {
//   if (idx < 0 || idx >= 4) {
//     // Invalid segment index
//     return;
//   }

//   cache_t *segment = params->segments[idx];

//   // Check if the object fits in this segment
//   if (req->obj_size > segment->cache_size) {
//     // Object is too large for this segment, try the next lower segment
//     if (idx > 0) {
//       S4LRU_segment_admit(cache, params, idx - 1, req);
//     } else {
//       // Object is too large for even the lowest segment, can't admit
//       return;
//     }
//   } else {
//     // Try to make room for the new object
//     while (segment->get_occupied_byte(segment) + req->obj_size > segment->cache_size) {
//       obj_id_t evicted_id = segment->evict_return_obj(segment, req);
//       if (evicted_id == 0) {
//         // Couldn't evict, segment is likely empty or all objects are pinned
//         break;
//       }

//       cache_obj_t *evicted_obj = hashtable_find_obj_id(cache->hashtable, evicted_id);
//       if (evicted_obj) {
//         request_t evicted_req = {.obj_id = evicted_id, .obj_size = evicted_obj->obj_size};

//         // Remove from current segment and overall cache
//         segment->remove(segment, evicted_id);
//         // cache->occupied_byte -= evicted_obj->obj_size;

//         // Try to admit to the next lower segment
//         if (idx > 0) {
//           S4LRU_segment_admit(cache, params, idx - 1, &evicted_req);
//         }
//       }
//     }

//     // Now insert the object into this segment
//     segment->insert(segment, req);

//     // Update overall cache size
//     // cache->occupied_byte += req->obj_size;
//   }
// }

// S4LRU evict operation
static void S4LRU_evict(cache_t *cache, const request_t *req) {
  DEBUG_ASSERT(false);  // Not implemented
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  for (int i = 0; i < 4; i++) {
    params->segments[i]->evict(params->segments[i], req);
  }
}

// S4LRU evict_return_obj operation
static obj_id_t S4LRU_evict_return_obj(cache_t *cache, const request_t *req) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  obj_id_t ret = params->segments[0]->evict_return_obj(params->segments[0], req);
  if (ret != -1) {
    cache->occupied_byte -= hashtable_find_obj_id(cache->hashtable, ret)->obj_size;
    return ret;
  }
  ret = params->segments[1]->evict_return_obj(params->segments[1], req);
  if (ret != -1) {
    cache->occupied_byte -= hashtable_find_obj_id(cache->hashtable, ret)->obj_size;
    return ret;
  }
  ret = params->segments[2]->evict_return_obj(params->segments[2], req);
  if (ret != -1) {
    cache->occupied_byte -= hashtable_find_obj_id(cache->hashtable, ret)->obj_size;
    return ret;
  }
  ret = params->segments[3]->evict_return_obj(params->segments[3], req);
  if (ret != -1) {
    cache->occupied_byte -= hashtable_find_obj_id(cache->hashtable, ret)->obj_size;
    return ret;
  }
  return -1;
}

static void adjust_segment_sizes(cache_t *cache) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  // From segment 3 to 0, check if the size exceeds the segment_size
  // If it does, evict the object in the segment and insert it to the next segment
  // If the next segment's size exceeds the segment_size, repeat the process
  // Segment 0's size is defined as total size - segment1's current size - ... - segment3's current size such that the
  // total size is maintained
  for (int i = 3; i > 0; i--) {
    while (params->segments[i]->get_occupied_byte(params->segments[i]) > params->segment_size) {
      request_t req;  // placeholder
      cache_obj_t *evicted_obj = params->segments[i]->to_evict(params->segments[i], &req);
      if (evicted_obj) {
        params->segments[i]->evict(params->segments[i], &req);
        int32_t pin_count = evicted_obj->pin_count;
        request_t evicted_req = {.obj_id = evicted_obj->obj_id, .obj_size = evicted_obj->obj_size};
        // printf("Evicting object %lu of size %lu from segment %d and inserting to segment %d\n", evicted_req.obj_id,
        // evicted_req.obj_size, i, i-1); Remove from current segment and overall cache insert
        cache_obj_t *inserted_obj = params->segments[i - 1]->insert(params->segments[i - 1], &evicted_req);
        inserted_obj->pin_count = pin_count;
      } else {
        abort();
      }
    }
  }
  // Now some objects are inserted to 0.
  uint64_t seg0_size = params->total_size - params->segments[1]->get_occupied_byte(params->segments[1]) -
                       params->segments[2]->get_occupied_byte(params->segments[2]) -
                       params->segments[3]->get_occupied_byte(params->segments[3]);
  if (seg0_size < params->segments[0]->get_occupied_byte(params->segments[0])) {
    abort();
  }
}

// S4LRU find operation
static cache_obj_t *S4LRU_find(cache_t *cache, const request_t *req, const bool update_cache) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  cache_obj_t *found_obj = NULL;
  int found_segment = -1;

  // First, find the object in any segment
  for (int i = 0; i < 4; i++) {
    cache_obj_t *obj = params->segments[i]->find(params->segments[i], req, false);  // Don't update yet
    if (obj) {
      found_obj = obj;
      found_segment = i;
      break;
    }
  }

  // If object is found and we need to update the cache
  assert(found_obj && update_cache);
  if (found_obj && update_cache) {
    // If the object is not in the highest segment, move it up
    if (found_segment < 3) {
      // Remove from current segment
      params->segments[found_segment]->remove(params->segments[found_segment], req->obj_id);
      request_t insert_req = {.obj_id = req->obj_id, .obj_size = found_obj->obj_size};
      int32_t pin_count = found_obj->pin_count;
      // Admit to the next higher segment
      cache_obj_t *inserted_obj = params->segments[found_segment + 1]->insert(params->segments[found_segment + 1], &insert_req);
      inserted_obj->pin_count = pin_count;
      // Adjust the size. If the inserted segment's size exceed the
      // segment_size, evict the object in the inserted segment and insert it to the next segment
      // Update the found object to point to the new location
      adjust_segment_sizes(cache);
    } else {
      // If it's already in the highest segment, just update its position within that segment
      params->segments[3]->find(params->segments[3], req, true);
    }
  }

  return found_obj;
}

// S4LRU to_evict operation
static cache_obj_t *S4LRU_to_evict(cache_t *cache, const request_t *req) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  return params->segments[0]->to_evict(params->segments[0], req);
}

// S4LRU remove operation
static bool S4LRU_remove(cache_t *cache, const obj_id_t obj_id) {
  S4LRU_params_t *params = (S4LRU_params_t *)cache->eviction_params;
  for (int i = 0; i < 4; i++) {
    if (params->segments[i]->remove(params->segments[i], obj_id)) {
      return true;
    }
  }
  return false;
}

#ifdef __cplusplus
}
#endif
