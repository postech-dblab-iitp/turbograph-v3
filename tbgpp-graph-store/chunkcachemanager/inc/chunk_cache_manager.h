#ifndef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>

#include "store.h"
#include "client.h"

typedef int32_t SegmentID;

class ChunkCacheManager {
public:
  ChunkCacheManager();
  ~ChunkCacheManager();

  // ChunkCacheManager APIs
  void PinSegment(SegmentID sid, std::string file_path, uint8_t** ptr, size_t* size);
  void UnPinSegment(SegmentID sid);
  void SetDirty(SegmentID sid);
  void CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy);
  void DestroySegment(SegmentID sid);

  // ChunkCacheManager Internal Functions
  void InitLightningStoreAndRun(const std::string unix_socket, int size/*, ChunkCacheManager* ccm_*/);

public:
  // Member Variables
  LightningStore* store;
  LightningClient* client;

  //
  std::thread* store_thread;
};

#endif // CHUNK_CACHE_MANAGER_H

