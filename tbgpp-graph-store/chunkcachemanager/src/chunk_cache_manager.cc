

#include "chunk_cache_manager.h"

ChunkCacheManager::ChunkCacheManager() {

}

void ChunkCacheManager::PinSegment(SegmentID sid, std::string file_path, uint8_t** ptr, size_t* size) {
  fprintf(stdout, "Not implemented\n");
  exit(-1);
}

void ChunkCacheManager::UnPinSegment(SegmentID sid) {
  fprintf(stdout, "Not implemented\n");
  exit(-1);
}

void ChunkCacheManager::SetDirty(SegmentID sid) {
  fprintf(stdout, "Not implemented\n");
  exit(-1);
}

void ChunkCacheManager::CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy) {
  fprintf(stdout, "Not implemented\n");
  exit(-1);
}

void ChunkCacheManager::DestroySegment(SegmentID sid) {
  fprintf(stdout, "Not implemented\n");
  exit(-1);
}
