#include <assert.h>
#include <glib.h>
#include <math.h>

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
} lru_queue_t;

typedef struct {
  GHashTable *freq_map;  // Maps size/nref category to LRU queue
  int64_t min_category;
  int64_t max_category;
} LRU_SP_params_t;

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void LRU_SP_free(cache_t *cache);
static bool LRU_SP_get(cache_t *cache, const request_t *req);
static cache_obj_t *LRU_SP_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *LRU_SP_insert(cache_t *cache, const request_t *req);
static cache_obj_t *LRU_SP_to_evict(cache_t *cache, const request_t *req);
static void LRU_SP_evict(cache_t *cache, const request_t *req);
static obj_id_t LRU_SP_evict_return_obj(cache_t *cache, const request_t *req);
static bool LRU_SP_remove(cache_t *cache, const obj_id_t obj_id);

static void free_lru_queue(gpointer data);
static int64_t get_size_freq_category(cache_t *cache, cache_obj_t *obj);
static void update_queue(cache_t *cache, cache_obj_t *obj);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

cache_t *LRU_SP_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("LRU-SP", ccache_params, cache_specific_params);
  cache->cache_init = LRU_SP_init;
  cache->cache_free = LRU_SP_free;
  cache->get = LRU_SP_get;
  cache->find = LRU_SP_find;
  cache->insert = LRU_SP_insert;
  cache->evict = LRU_SP_evict;
  cache->evict_return_obj = LRU_SP_evict_return_obj;
  cache->remove = LRU_SP_remove;
  cache->to_evict = LRU_SP_to_evict;

  if (ccache_params.consider_obj_metadata) {
    cache->obj_md_size = 8 * 2 + 1;  // prev, next pointers and access count
  } else {
    cache->obj_md_size = 0;
  }

  LRU_SP_params_t *params = my_malloc_n(LRU_SP_params_t, 1);
  cache->eviction_params = params;
  params->freq_map = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, free_lru_queue);
  params->min_category = INT64_MAX;
  params->max_category = INT64_MIN;

  return cache;
}

static void LRU_SP_free(cache_t *cache) {
  LRU_SP_params_t *params = (LRU_SP_params_t *)cache->eviction_params;
  g_hash_table_destroy(params->freq_map);
  my_free(sizeof(LRU_SP_params_t), params);
  cache_struct_free(cache);
}

static bool LRU_SP_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

static cache_obj_t *LRU_SP_find(cache_t *cache, const request_t *req, const bool update_cache) {
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);

  if (obj != NULL && update_cache) {
    obj->LRUSP.freq++;
    obj->LRUSP.last_access_time = cache->n_req;
    update_queue(cache, obj);
  }

  return obj;
}

static cache_obj_t *LRU_SP_insert(cache_t *cache, const request_t *req) {
  cache_obj_t *obj = cache_insert_base(cache, req);
  obj->LRUSP.freq = 1;
  obj->LRUSP.last_access_time = cache->n_req;  // Add this line
  update_queue(cache, obj);
  return obj;
}

static cache_obj_t *LRU_SP_to_evict(cache_t *cache, const request_t *req) {
  LRU_SP_params_t *params = (LRU_SP_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = NULL;
  double max_cost_to_size = -1;

  for (int64_t category = params->min_category; category <= params->max_category; category++) {
    lru_queue_t *queue = g_hash_table_lookup(params->freq_map, GSIZE_TO_POINTER(category));
    if (queue == NULL || queue->q_tail == NULL) continue;

    cache_obj_t *obj = queue->q_tail;
    double cost_to_size = (double)(cache->n_req - obj->LRUSP.last_access_time) * obj->obj_size / obj->LRUSP.freq;
    if (cost_to_size > max_cost_to_size) {
      max_cost_to_size = cost_to_size;
      obj_to_evict = obj;
    }
  }

  return obj_to_evict;
}

static void LRU_SP_evict(cache_t *cache, const request_t *req) {
  cache_obj_t *obj_to_evict = LRU_SP_to_evict(cache, req);
  if (obj_to_evict == NULL) {
    ERROR("no object to evict\n");
    abort();
  }

  LRU_SP_remove(cache, obj_to_evict->obj_id);
  cache_evict_base(cache, obj_to_evict, true);
}

static obj_id_t LRU_SP_evict_return_obj(cache_t *cache, const request_t *req) {
  LRU_SP_params_t *params = (LRU_SP_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = NULL;
  double max_cost_to_size = -1;
  int64_t current_time = cache->n_req;

  // Iterate through all categories from min to max
  for (int64_t category = params->min_category; category <= params->max_category; category++) {
    lru_queue_t *queue = g_hash_table_lookup(params->freq_map, GSIZE_TO_POINTER(category));
    if (queue == NULL || queue->q_tail == NULL) {
      continue;
    }

    // Iterate through all objects in the queue from tail to head
    cache_obj_t *obj = queue->q_tail;
    while (obj != NULL) {
      if (!is_pinned(cache, obj->obj_id)) {
        // Calculate the cost-to-size ratio for this object
        double cost_to_size = (double)(current_time - obj->LRUSP.last_access_time) * obj->obj_size / obj->LRUSP.freq;

        // Update the eviction candidate if this object has a higher cost-to-size ratio
        if (cost_to_size > max_cost_to_size) {
          max_cost_to_size = cost_to_size;
          obj_to_evict = obj;
        }
      }
      obj = obj->queue.prev;
    }
  }

  // If all objects are pinned, return -1
  if (obj_to_evict == NULL) {
    return -1;
  }

  obj_id_t evicted_obj_id = obj_to_evict->obj_id;

  // Remove the object from its current queue
  int64_t category = get_size_freq_category(cache, obj_to_evict);
  lru_queue_t *queue = g_hash_table_lookup(params->freq_map, GSIZE_TO_POINTER(category));
  if (queue != NULL) {
    remove_obj_from_list(&queue->q_head, &queue->q_tail, obj_to_evict);
  }

  // Remove object from hash table
  cache_remove_obj_base(cache, obj_to_evict, true);

  return evicted_obj_id;
}

static bool LRU_SP_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  LRU_SP_params_t *params = (LRU_SP_params_t *)cache->eviction_params;
  int64_t category = get_size_freq_category(cache, obj);
  lru_queue_t *queue = g_hash_table_lookup(params->freq_map, GSIZE_TO_POINTER(category));

  if (queue != NULL) {
    remove_obj_from_list(&queue->q_head, &queue->q_tail, obj);
  }

  return true;
}

static void free_lru_queue(gpointer data) {
  lru_queue_t *queue = (lru_queue_t *)data;
  my_free(sizeof(lru_queue_t), queue);
}

static int64_t get_size_freq_category(cache_t *cache, cache_obj_t *obj) {
  // Ensure that this function returns consistent categories that match the GHashTable keys
  int64_t category = (int64_t)log2((double)obj->obj_size / obj->LRUSP.freq);
  return category < 0 ? 0 : category;  // Prevent negative categories
}

static void update_queue(cache_t *cache, cache_obj_t *obj) {
  LRU_SP_params_t *params = (LRU_SP_params_t *)cache->eviction_params;
  int64_t old_category = get_size_freq_category(cache, obj);
  int64_t new_category = get_size_freq_category(cache, obj);

  if (old_category != new_category) {
    lru_queue_t *old_queue = g_hash_table_lookup(params->freq_map, GSIZE_TO_POINTER(old_category));
    if (old_queue != NULL) {
      if (obj->queue.prev == NULL && obj->queue.next == NULL) {
      } else
        remove_obj_from_list(&old_queue->q_head, &old_queue->q_tail, obj);

      // If the queue is now empty, remove it from the freq_map
      if (old_queue->q_head == NULL) {
        g_hash_table_remove(params->freq_map, GSIZE_TO_POINTER(old_category));
        // Update min_category and max_category after removing the old queue
        if (old_category == params->min_category || old_category == params->max_category) {
          // Recalculate min_category and max_category
          params->min_category = INT64_MAX;
          params->max_category = INT64_MIN;
          GHashTableIter iter;
          gpointer key, value;
          g_hash_table_iter_init(&iter, params->freq_map);
          while (g_hash_table_iter_next(&iter, &key, &value)) {
            int64_t category = GPOINTER_TO_SIZE(key);
            if (category < params->min_category) params->min_category = category;
            if (category > params->max_category) params->max_category = category;
          }
        }
      }
    }
  }

  lru_queue_t *new_queue = g_hash_table_lookup(params->freq_map, GSIZE_TO_POINTER(new_category));
  if (new_queue == NULL) {
    new_queue = my_malloc_n(lru_queue_t, 1);
    new_queue->q_head = new_queue->q_tail = NULL;
    g_hash_table_insert(params->freq_map, GSIZE_TO_POINTER(new_category), new_queue);
  }

  prepend_obj_to_head(&new_queue->q_head, &new_queue->q_tail, obj);

  // Update min_category and max_category based on the new category
  if (new_category < params->min_category) params->min_category = new_category;
  if (new_category > params->max_category) params->max_category = new_category;
}

#ifdef __cplusplus
}
#endif
