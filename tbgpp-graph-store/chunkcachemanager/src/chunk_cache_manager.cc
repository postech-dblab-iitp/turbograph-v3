

#include "chunk_cache_manager.h"

ChunkCacheManager::ChunkCacheManager() {

}

void ChunkCacheManager::PinSegment(SegmentID sid, std::string file_path, uint8_t** ptr, size_t* size) {

}

void ChunkCacheManager::UnPinSegment(SegmentID sid) {

}

void ChunkCacheManager::SetDirty(SegmentID sid) {

}

void ChunkCacheManager::CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy) {

}

void ChunkCacheManager::DestroySegment(SegmentID sid) {

}
