#ifndef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>

#include "common/constants.hpp"
#include "cache/common.h"
#include "cache/client.h"
#include "cache/disk_aio/Turbo_bin_aio_handler.hpp"

namespace duckdb {

#define NUM_MAX_SEGMENTS 65536

class ChunkCacheManager {
public:
  static ChunkCacheManager* ccm;

public:
  ChunkCacheManager();
  ~ChunkCacheManager();

  // ChunkCacheManager APIs
  ReturnStatus PinSegment(SegmentID sid, std::string file_path, uint8_t** ptr, size_t* size);
  ReturnStatus UnPinSegment(SegmentID sid);
  ReturnStatus SetDirty(SegmentID sid);
  ReturnStatus CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy);
  ReturnStatus DestroySegment(SegmentID sid);

  // APIs for Debugging purpose
  int GetRefCount(SegmentID sid);

  // ChunkCacheManager Internal Functions
  bool SidValidityCheck(SegmentID sid);
  bool AllocSizeValidityCheck(size_t alloc_size);
  size_t GetSegmentSize(SegmentID sid, std::string file_path); // sid가 필요한지?
  size_t GetFileSize(SegmentID sid, std::string file_path); // sid가 필요한지?
  void ReadData(SegmentID sid, std::string file_path, uint8_t** ptr, size_t size_to_read);
  void WriteData(SegmentID sid);
  ReturnStatus CreateNewFile(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy);
  void MemAlign(uint8_t** ptr, size_t segment_size, size_t required_memory_size);

public:
  // Member Variables
  LightningClient* client;
  Turbo_bin_aio_handler* file_handlers[NUM_MAX_SEGMENTS];
  //Turbo_bin_aio_handler file_handler;
};

#endif // CHUNK_CACHE_MANAGER_H

}