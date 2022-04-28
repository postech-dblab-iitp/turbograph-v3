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
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    //TODO exception 추가
    //throw InvalidInputException("[PinSegment] invalid sid");

  // Pin Segment using Lightning Get()
  if (client->Get(sid, ptr, size) != 0) {
    // Get fail: 1) object not found, 2) object is not sealed yet

    // TODO: Check if there is enough memory space
    size_t segment_size = GetSegmentSize(sid, file_path);
    //size_t deleted_size = 0;
    //if (!IsMemorySpaceEnough(segment_size)) {
    //  FindVictimAndDelete(segment_size, deleted_size);
    //  // Replacement algorithm은 일단 지원 X
    //}

    if (client->Create(sid, ptr, segment_size) == 0) {
      // Memory usage should be controlled in Lightning Create

      // Read data & Seal object
      ReadData(sid, file_path, ptr);
      client->Seal(sid);
      *size = segment_size;
    } else {
      // Create fail -> Subscribe object
      client->Subscribe(sid);
      client->Get(sid, ptr, size);
    }
  }
}

void ChunkCacheManager::UnPinSegment(SegmentID sid) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    // TODO
    // throw InvalidInputException("[UnpinSegment] invalid sid");

  // Unpin Segment using Lightning Release()
  client->Release(sid);
}

void ChunkCacheManager::SetDirty(SegmentID sid) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[SetDirty] invalid sid");

  // TODO: modify header information
}

void ChunkCacheManager::CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy) {
  // Check validity of given alloc_size
  // It fails if 1) the storage runs out of space, 2) the alloc_size exceeds the limit size (if it exists)
  if (AllocSizeValidityCheck(alloc_size))
    exit(-1);
    //TODO
    //throw InvalidInputException("[CreateSegment] invalid alloc_size");

  
  // Create file for the segment
  CreateNewFile(sid, file_path, alloc_size);
}

void ChunkCacheManager::DestroySegment(SegmentID sid) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[DestroySegment] invalid sid");

  // TODO: Check the reference count
  // If the count > 0, we cannot destroy the segment
  
  // Check if the segment can be destroyed
  /*if (segid_to_segcandestroy_map[sid]) {
    // Just remove the segment file
    DestroyFile(segid_to_fileid_map[sid]);
  } else {
    // Check if the segment need to be flushed
    if (segid_to_dirtybit_map[sid]) {
      WriteData(sid);
      segid_to_dirtybit_map[sid] = false;
    }
  }*/
  
  // Delete the segment from the buffer using Lightning Delete()
  client->Delete(sid);
  //AdjustMemoryUsage(-GetSegmentSize(sid)); // need type casting
}

//void ChunkCacheManager::InitLightningStoreAndRun(const std::string unix_socket, int size, ChunkCacheManager* ccm_) {
void ChunkCacheManager::InitLightningStoreAndRun(const std::string unix_socket, int size) {
  // TODO: socket & size will be given
  store = new LightningStore("/tmp/lightning", 1024 * 1024 * 1024);
  store->Run();
}

// Return true if the given SegmentID is not valid
bool ChunkCacheManager::SidValidityCheck(SegmentID sid) {
  // Catalog Manager에서 validity check 등? 아예 필요 없을 수도 있고.. 해서 내려올테니
  // 이 layer에서는 고민할 필요가 없을 수도 있음. 나중에 필요하면 추가
  return false;
}

// Return true if the given alloc_size is not valid
bool ChunkCacheManager::AllocSizeValidityCheck(size_t alloc_size) {
  // TODO: 제한된 크기가 있거나, 파일 시스템 상에 남은 공간이 충분하지 않을 경우를 다뤄야 함
  return false;
}

// Return the size of segment
size_t ChunkCacheManager::GetSegmentSize(SegmentID sid, std::string file_path) {
  // TODO
  exit(-1);
  return 0;
}

void ChunkCacheManager::ReadData(SegmentID sid, std::string file_path, uint8_t** ptr) {
  // TODO
  exit(-1);
  return;
}

void ChunkCacheManager::WriteData(SegmentID sid) {
  // TODO
  exit(-1);
  return;
}

void ChunkCacheManager::CreateNewFile(SegmentID sid, std::string file_path, size_t alloc_size) {
  // TODO
  exit(-1);
  return;
}