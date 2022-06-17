#include <string>
#include <thread>
#include <unordered_set>

#include "chunk_cache_manager.h"
#include "Turbo_bin_aio_handler.hpp"

using namespace duckdb {

ChunkCacheManager* ChunkCacheManager::ccm;

ChunkCacheManager::ChunkCacheManager() {
  // Init LightningClient
  client = new LightningClient("/tmp/lightning", "password");

  // Initialize file_handlers as nullptr
  for (int i = 0; i < NUM_MAX_SEGMENTS; i++) file_handlers[i] = nullptr;
}

ChunkCacheManager::~ChunkCacheManager() {
}

ReturnStatus ChunkCacheManager::PinSegment(SegmentID sid, std::string file_path, uint8_t** ptr, size_t* size) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    //TODO: exception 추가
    //throw InvalidInputException("[PinSegment] invalid sid");

  size_t segment_size = GetSegmentSize(sid, file_path);
  size_t file_size = GetFileSize(sid, file_path);
  size_t required_memory_size = file_size + 512; // Add 512 byte for memory aligning
  assert(file_size >= segment_size);

  // Pin Segment using Lightning Get()
  if (client->Get(sid, ptr, size) != 0) {
    // Get() fail: 1) object not found, 2) object is not sealed yet
    // TODO: Check if there is enough memory space

    //size_t deleted_size = 0;
    //if (!IsMemorySpaceEnough(segment_size)) {
    //  FindVictimAndDelete(segment_size, deleted_size);
    //  // Replacement algorithm은 일단 지원 X
    //}

    if (client->Create(sid, ptr, required_memory_size) == 0) {
      // TODO: Memory usage should be controlled in Lightning Create

      // Align memory
      MemAlign(ptr, segment_size, required_memory_size);
      
      // Read data & Seal object
      ReadData(sid, file_path, ptr, file_size);
      client->Seal(sid);
      *size = segment_size;
    } else {
      // Create fail -> Subscribe object
      client->Subscribe(sid);
      int status = client->Get(sid, ptr, size);
      assert(status == 0);

      // Align memory & adjust size
      MemAlign(ptr, segment_size, required_memory_size);
      *size = segment_size;
    }
    return NOERROR;
  }
  
  // Get() success. Align memory & adjust size
  MemAlign(ptr, segment_size, required_memory_size);
  *size = segment_size;

  return NOERROR;
}

ReturnStatus ChunkCacheManager::UnPinSegment(SegmentID sid) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    // TODO
    // throw InvalidInputException("[UnpinSegment] invalid sid");

  // Unpin Segment using Lightning Release()
  client->Release(sid);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::SetDirty(SegmentID sid) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[SetDirty] invalid sid");

  // TODO: modify header information
  if (client->SetDirty(sid) != 0) {
    // TODO: exception handling
    exit(-1);
  }
  return NOERROR;
}

ReturnStatus ChunkCacheManager::CreateSegment(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy) {
  // Check validity of given alloc_size
  // It fails if 1) the storage runs out of space, 2) the alloc_size exceeds the limit size (if it exists)
  if (AllocSizeValidityCheck(alloc_size))
    exit(-1);
    //TODO
    //throw InvalidInputException("[CreateSegment] invalid alloc_size");

  // Create file for the segment
  return CreateNewFile(sid, file_path, alloc_size, can_destroy);
}

ReturnStatus ChunkCacheManager::DestroySegment(SegmentID sid) {
  // Check validity of given SegmentID
  if (SidValidityCheck(sid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[DestroySegment] invalid sid");

  // TODO: Check the reference count
  // If the count > 0, we cannot destroy the segment
  
  // Delete the segment from the buffer using Lightning Delete()
  assert(file_handlers[sid] != nullptr);
  client->Delete(sid, file_handlers[sid]);
  file_handlers[sid]->Close();
  file_handlers[sid] = nullptr;
  //AdjustMemoryUsage(-GetSegmentSize(sid)); // need type casting
  return NOERROR;
}

int ChunkCacheManager::GetRefCount(SegmentID sid) {
  return client->GetRefCount(sid);
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
  assert(file_handlers[sid] != nullptr);
  if (file_handlers[sid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handlers[sid]->GetRequestedSize();
}

// Return the size of file
size_t ChunkCacheManager::GetFileSize(SegmentID sid, std::string file_path) {
  assert(file_handlers[sid] != nullptr);
  if (file_handlers[sid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handlers[sid]->file_size();
}

void ChunkCacheManager::ReadData(SegmentID sid, std::string file_path, uint8_t** ptr, size_t size_to_read) {
  if (file_handlers[sid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  file_handlers[sid]->Read(0, (int64_t) size_to_read, (char*) *ptr, nullptr, nullptr);
  file_handlers[sid]->WaitForMyIoRequests(true, true);
}

void ChunkCacheManager::WriteData(SegmentID sid) {
  // TODO
  exit(-1);
  return;
}

ReturnStatus ChunkCacheManager::CreateNewFile(SegmentID sid, std::string file_path, size_t alloc_size, bool can_destroy) {
  assert(file_handlers[sid] == nullptr);
  file_handlers[sid] = new Turbo_bin_aio_handler();
  ReturnStatus rs = file_handlers[sid]->OpenFile((file_path + std::to_string(sid)).c_str(), true, true, true, true);
  assert(rs == NOERROR);
  
  // Compute aligned file size
  int64_t alloc_file_size = ((alloc_size - 1 + 512) / 512) * 512;
  
  file_handlers[sid]->SetRequestedSize(alloc_size);
  file_handlers[sid]->ReserveFileSize(alloc_file_size);
  file_handlers[sid]->SetCanDestroy(can_destroy);
  return rs;
}

void ChunkCacheManager::MemAlign(uint8_t** ptr, size_t segment_size, size_t required_memory_size) {
  void* target_ptr = (void*) *ptr;
  std::align(512, segment_size, target_ptr, required_memory_size);
  if (target_ptr == nullptr) {
    exit(-1);
    // TODO throw exception
  }
}

}