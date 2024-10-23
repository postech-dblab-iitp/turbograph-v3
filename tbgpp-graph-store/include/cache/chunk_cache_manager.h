#ifndef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>
#include "common/unordered_map.hpp"

#include "common/constants.hpp"
#include "cache/common.h"
#include "cache/client.h"
#include "cache/disk_aio/Turbo_bin_aio_handler.hpp"

namespace duckdb {

#define NUM_MAX_SEGMENTS 65536

class ChunkCacheManager {
public:
  static ChunkCacheManager *ccm;

public:
  ChunkCacheManager(const char *path);
  ~ChunkCacheManager();

  void InitializeFileHandlersByIteratingDirectories(const char *path);
  void InitializeFileHandlersUsingMetaInfo(const char *path);
  void FlushMetaInfo(const char *path);

  // ChunkCacheManager APIs
  ReturnStatus PinSegment(ChunkID cid, std::string file_path, uint8_t** ptr, size_t* size, bool read_data_async=false, bool is_initial_loading=false);
  ReturnStatus UnPinSegment(ChunkID cid);
  ReturnStatus SetDirty(ChunkID cid);
  ReturnStatus CreateSegment(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy);
  ReturnStatus DestroySegment(ChunkID cid);
  ReturnStatus FinalizeIO(ChunkID cid, bool read=true, bool write=true);
  ReturnStatus FlushDirtySegmentsAndDeleteFromcache();

  // APIs for Debugging purpose
  int GetRefCount(ChunkID cid);

  // ChunkCacheManager Internal Functions
  bool CidValidityCheck(ChunkID cid);
  bool AllocSizeValidityCheck(size_t alloc_size);
  size_t GetSegmentSize(ChunkID cid, std::string file_path); // sid가 필요한지?
  size_t GetFileSize(ChunkID cid, std::string file_path); // sid가 필요한지?
  Turbo_bin_aio_handler* GetFileHandler(ChunkID cid);
  void ReadData(ChunkID cid, std::string file_path, void *ptr, size_t size_to_read, bool read_data_async);
  void WriteData(ChunkID cid);
  ReturnStatus CreateNewFile(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy);
  void *MemAlign(uint8_t** ptr, size_t segment_size, size_t required_memory_size, Turbo_bin_aio_handler* file_handler);

  void UnswizzleFlushSwizzle(ChunkID cid, Turbo_bin_aio_handler* file_handler, bool close_file=true);

public:
  // Member Variables
  LightningClient* client;
  //Turbo_bin_aio_handler* file_handlers[NUM_MAX_SEGMENTS];
  unordered_map<ChunkID, Turbo_bin_aio_handler*> file_handlers;
  //Turbo_bin_aio_handler file_handler;
  const std::string file_meta_info_name = ".file_meta_info";
};

#endif // CHUNK_CACHE_MANAGER_H

}