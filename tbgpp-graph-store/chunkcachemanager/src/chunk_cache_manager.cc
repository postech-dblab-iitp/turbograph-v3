#include <string>
#include <thread>
#include <unordered_set>

#include "chunk_cache_manager.h"

ChunkCacheManager::ChunkCacheManager() {
  // Init LightningStore & Run
  // TODO parameter
  store_thread = new std::thread(&ChunkCacheManager::InitLightningStoreAndRun, this, "/tmp/lightning", 1024 * 1024 * 1024);

  // Init LightningClient
  client = new LightningClient("/tmp/lightning", "password");
}

ChunkCacheManager::~ChunkCacheManager() {
  store_thread->join();
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

//void ChunkCacheManager::InitLightningStoreAndRun(const std::string unix_socket, int size, ChunkCacheManager* ccm_) {
void ChunkCacheManager::InitLightningStoreAndRun(const std::string unix_socket, int size) {
  // TODO: socket & size will be given
  store = new LightningStore("/tmp/lightning", 1024 * 1024 * 1024);
  store->Run();
}