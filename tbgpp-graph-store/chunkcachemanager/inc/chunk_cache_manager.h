#ifdef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>

#include "store.h"
#include "client.h"

typedef SegmentID int32_t;

class ChunkCacheManager {
public:
  ChunkCacheManager();
  ~ChunkCacheManager() {}

  // ChunkCacheManager APIs
  void PinSegment(SegmentID sid, std::string file_path, uint8_t** ptr, size_t* size);
  void UnPinSegment(SegmentID sid);
  void SetDirty(SegmentID sid);
  void CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy);
  void DestroySegment(SegmentID sid);

  // ChunkCacheManager Internal Functions

public:
  // Member Variables
  LightningStore store;
  LightningClient client;
};

#endif // CHUNK_CACHE_MANAGER_H

