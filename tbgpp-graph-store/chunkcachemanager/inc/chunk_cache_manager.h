#ifndef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>

#include "client.h"
#include "Turbo_bin_aio_handler.hpp"

typedef int16_t SegmentID;

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
  bool SidValidityCheck(SegmentID sid);
  bool AllocSizeValidityCheck(size_t alloc_size);
  size_t GetSegmentSize(SegmentID sid, std::string file_path); // sid가 필요한지?
  void ReadData(SegmentID sid, std::string file_path, uint8_t** ptr, size_t segment_size);
  void WriteData(SegmentID sid);
  void CreateNewFile(SegmentID sid, std::string file_path, size_t alloc_size);

public:
  // Member Variables
  LightningClient* client;
  Turbo_bin_aio_handler file_handler;
};

#endif // CHUNK_CACHE_MANAGER_H

