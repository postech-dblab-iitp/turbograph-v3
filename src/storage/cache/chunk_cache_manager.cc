#include <string>
#include <thread>
#include <unordered_set>
#include <filesystem>
#include <cstdint>

#include "storage/cache/chunk_cache_manager.h"
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"
#include "storage/cache/cache_data_transformer.h"

#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "common/logger.hpp"
#include "common/types/string_type.hpp"

namespace duckdb {

ChunkCacheManager* ChunkCacheManager::ccm;

ChunkCacheManager::ChunkCacheManager(const char *path, bool standalone) {
  spdlog::debug("[ChunkCacheManager] Construct ChunkCacheManager");
  client = new LightningClient("/tmp/lightning", "password", standalone);
  if (std::filesystem::exists(std::string(path) + "/" + file_meta_info_name)) {
    InitializeFileHandlersUsingMetaInfo(path);
  } else {
    InitializeFileHandlersByIteratingDirectories(path);
  }
}

ChunkCacheManager::~ChunkCacheManager() {
  spdlog::debug("[~ChunkCacheManager] Deconstruct ChunkCacheManager");
  for (auto &file_handler: file_handlers) {
    if (file_handler.second == nullptr) continue;

    bool is_dirty;
    client->GetDirty(file_handler.first, is_dirty);
    if (!is_dirty) continue;

    spdlog::trace("[~ChunkCacheManager] Flush file: {} with size {}", file_handler.second->GetFilePath(), file_handler.second->file_size());

    // TODO we need a write lock
    UnswizzleFlushSwizzle(file_handler.first, file_handler.second);
    client->ClearDirty(file_handler.first);
  }

  for (auto &file_handler: file_handlers) {
    if (file_handler.second == nullptr) continue;
    delete file_handler.second;
  }

  delete client;
}

void ChunkCacheManager::UnswizzleFlushSwizzle(ChunkID cid, Turbo_bin_aio_handler* file_handler, bool close_file) {
  uint8_t *ptr;
  size_t size;

  /**
   * TODO we need a write lock
   * We flush in-memory data format to disk format.
   * Thus, unswizzling is needed.
   * However, since change the data in memory, we need to swizzle again.
   * After implementing eviction and so one, this code should be changed.
   * 
   * Ideal code is like below:
   * 1. Extent Manager write unswizzled data into memory
   * 2. ChunkCacheManager flush the data into disk, and remove all objects from store
   * 3. Proceeding clients can read the data from disk, and swizzle it.
  */
  PinSegment(cid, file_handler->GetFilePath(), &ptr, &size, false, false);
  CacheDataTransformer::Unswizzle(ptr);
  file_handler->FlushAllBlocking();
  file_handler->WaitAllPendingDiskIO(false);
  CacheDataTransformer::Swizzle(ptr);
  if (close_file)
    file_handler->Close();
  UnPinSegment(cid);
}

void ChunkCacheManager::InitializeFileHandlersByIteratingDirectories(const char *path)
{
  std::string partition_path = std::string(path);
  for (const auto &partition_entry : std::filesystem::directory_iterator(partition_path)) { // /path/
    std::string partition_entry_path = std::string(partition_entry.path());
    std::string partition_entry_name = partition_entry_path.substr(partition_entry_path.find_last_of("/") + 1);
    if (StringUtil::StartsWith(partition_entry_name, "part_")) {
      std::string extent_path = partition_entry_path + std::string("/");
      for (const auto &extent_entry : std::filesystem::directory_iterator(extent_path)) { // /path/part_/
        std::string extent_entry_path = std::string(extent_entry.path());
        std::string extent_entry_name = extent_entry_path.substr(extent_entry_path.find_last_of("/") + 1);
        if (StringUtil::StartsWith(extent_entry_name, "ext_")) {
          std::string chunk_path = extent_entry_path + std::string("/");
          for (const auto &chunk_entry : std::filesystem::directory_iterator(chunk_path)) { // /path/part_/ext_/
            std::string chunk_entry_path = std::string(chunk_entry.path());
            std::string chunk_entry_name = chunk_entry_path.substr(chunk_entry_path.find_last_of("/") + 1);
            ChunkDefinitionID chunk_id = (ChunkDefinitionID) std::stoull(chunk_entry_name.substr(chunk_entry_name.find("_") + 1));

            // Open File & Insert into file_handlers
            D_ASSERT(file_handlers.find(chunk_id) == file_handlers.end());
            file_handlers[chunk_id] = new Turbo_bin_aio_handler();
            ReturnStatus rs = file_handlers[chunk_id]->OpenFile(chunk_entry_path.c_str(), false, true, false, true);
            D_ASSERT(rs == NOERROR);

            // Read First Block & SetRequestedSize
            char *first_block;
            int status = posix_memalign((void **)&first_block, 512, 512);
            if (status != 0) throw InvalidInputException("posix_memalign fail"); // XXX wrong exception type

            file_handlers[chunk_id]->Read(0, 512, first_block, this, nullptr);
            file_handlers[chunk_id]->WaitAllPendingDiskIO(true);

            size_t requested_size = ((size_t *)first_block)[0];
            file_handlers[chunk_id]->SetRequestedSize(requested_size + 8);
            free(first_block);
          }
        }
      }
    }
  }
}

void ChunkCacheManager::InitializeFileHandlersUsingMetaInfo(const char *path)
{
  std::string meta_file_path = std::string(path) + "/" + file_meta_info_name;
  Turbo_bin_io_handler file_meta_info(meta_file_path.c_str(), false, false, false);
  size_t file_size = file_meta_info.file_size();
  uint64_t *meta_info = new uint64_t[file_size / sizeof(uint64_t)];
  file_meta_info.Read(0, file_size, (char *)meta_info);
  file_meta_info.Close();

  for (size_t i = 0; i < file_size / sizeof(uint64_t); i += 2) {
    ChunkDefinitionID chunk_id = (ChunkDefinitionID) meta_info[i];
    size_t requested_size = meta_info[i + 1];

    uint64_t extent_id = chunk_id >> 32;
    uint64_t partition_id = extent_id >> 16;
    std::string chunk_path = std::string(path) + "/part_" + std::to_string(partition_id) +
                             "/ext_" + std::to_string(extent_id) + "/chunk_" +
                             std::to_string(chunk_id);
    D_ASSERT(file_handlers.find(chunk_id) == file_handlers.end());
    file_handlers[chunk_id] = new Turbo_bin_aio_handler();
    ReturnStatus rs = file_handlers[chunk_id]->OpenFile(chunk_path.c_str(), false, true, false, true);
    D_ASSERT(rs == NOERROR);
    file_handlers[chunk_id]->SetRequestedSize(requested_size);
  }

  delete[] meta_info;
}

void ChunkCacheManager::FlushMetaInfo(const char *path)
{
  spdlog::debug("[FlushMetaInfo] Start to flush meta info");

  uint64_t *meta_info;
  int64_t num_total_files = file_handlers.size();
  meta_info = new uint64_t[num_total_files * 2];

  // TODO: I think file_handler.second is nullptr sometimes. Please fix.
  int64_t i = 0;
  for (auto &file_handler: file_handlers) {
    assert(file_handler.second != nullptr);
    meta_info[i] = file_handler.first;
    meta_info[i + 1] = file_handler.second->GetRequestedSize();
    i += 2;
  }

  std::string meta_file_path = std::string(path) + "/" + file_meta_info_name;
  Turbo_bin_io_handler file_meta_info(meta_file_path.c_str(), true, true, true);
  file_meta_info.Append(num_total_files * 2 * sizeof(uint64_t), (char *)meta_info);
  file_meta_info.Close(false);

  delete[] meta_info;
}

ReturnStatus ChunkCacheManager::PinSegment(ChunkID cid, std::string file_path, uint8_t** ptr, size_t* size, bool read_data_async, bool is_initial_loading) {
  spdlog::trace("[PinSegment] Start to pin segment: {}", cid);
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    //TODO: exception 추가
    //throw InvalidInputException("[PinSegment] invalid cid");

  auto file_handler = GetFileHandler(cid);
  size_t segment_size = file_handler->GetRequestedSize();
  size_t file_size = file_handler->file_size();
  size_t required_memory_size = file_size + 512; // Add 512 byte for memory aligning
  D_ASSERT(file_size >= segment_size);

  // Pin Segment using Lightning Get()
  if (client->Get(cid, ptr, size) != 0) {
    // Get() fail: 1) object not found, 2) object is not sealed yet
    // TODO: Check if there is enough memory space

    //size_t deleted_size = 0;
    //if (!IsMemorySpaceEnough(segment_size)) {
    //  FindVictimAndDelete(segment_size, deleted_size);
    //  // Replacement algorithm은 일단 지원 X
    //}

    if (client->Create(cid, ptr, required_memory_size) == 0) {
      // Align memory
      void *file_ptr = MemAlign(ptr, segment_size, required_memory_size, file_handler);

      // Read data & Seal object
      // TODO: we fix read_data_async as false, due to swizzling. May move logic to iterator.
      // ReadData(cid, file_path, file_ptr, file_size, read_data_async);
      ReadData(cid, file_path, file_ptr, file_size, false);

      if (!is_initial_loading) {
        CacheDataTransformer::Swizzle(*ptr);
      }
      client->Seal(cid);
      // if (!read_data_async) client->Seal(cid); // WTF???
      *size = segment_size - sizeof(size_t);
    } else {
      // Create fail -> Subscribe object
      client->Subscribe(cid);

      int status = client->Get(cid, ptr, size);
      D_ASSERT(status == 0);

      // Align memory & adjust size
      MemAlign(ptr, segment_size, required_memory_size, file_handler);
      *size = segment_size - sizeof(size_t);
    }
    return NOERROR;
  }
  // Get() success. Align memory & adjust size
  MemAlign(ptr, segment_size, required_memory_size, file_handler);
  *size = segment_size - sizeof(size_t);
  
  return NOERROR;
}

ReturnStatus ChunkCacheManager::UnPinSegment(ChunkID cid) {
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    // throw InvalidInputException("[UnpinSegment] invalid cid");

  // Unpin Segment using Lightning Release()
  // client->Release(cid);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::SetDirty(ChunkID cid) {
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[SetDirty] invalid cid");

  // TODO: modify header information
  if (client->SetDirty(cid) != 0) {
    // TODO: exception handling
    exit(-1);
  }
  return NOERROR;
}

ReturnStatus ChunkCacheManager::CreateSegment(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy) {
  spdlog::trace("[CreateSegment] Start to create segment: {}", cid);
  // Check validity of given alloc_size
  // It fails if 1) the storage runs out of space, 2) the alloc_size exceeds the limit size (if it exists)
  if (AllocSizeValidityCheck(alloc_size))
    exit(-1);
    //TODO
    //throw InvalidInputException("[CreateSegment] invalid alloc_size");

  // Create file for the segment
  auto ret = CreateNewFile(cid, file_path, alloc_size, can_destroy);
  return ret;
}

ReturnStatus ChunkCacheManager::DestroySegment(ChunkID cid) {
  spdlog::trace("[DestroySegment] Start to destroy segment: {}", cid);
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[DestroySegment] invalid cid");

  // TODO: Check the reference count
  // If the count > 0, we cannot destroy the segment
  
  // Delete the segment from the buffer using Lightning Delete()
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  client->Delete(cid);
  // jhha: disable file_handler close, due to flushMetaInfo
  // file_handlers[cid]->Close();
  // file_handlers[cid] = nullptr;
  //AdjustMemoryUsage(-GetSegmentSize(cid)); // need type casting
  return NOERROR;
}

ReturnStatus ChunkCacheManager::FinalizeIO(ChunkID cid, bool read, bool write) {
  file_handlers[cid]->WaitForMyIoRequests(read, write);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::FlushDirtySegmentsAndDeleteFromcache(bool destroy_segment) {
  spdlog::debug("[FlushDirtySegmentsAndDeleteFromcache] Start to {} flush dirty segments and delete from cache", file_handlers.size());

    // Collect iterators into a vector
    vector<unordered_map<ChunkID, Turbo_bin_aio_handler*>::iterator> iterators;
    iterators.reserve(file_handlers.size());
    for (auto it = file_handlers.begin(); it != file_handlers.end(); ++it) {
        iterators.push_back(it);
    }

  #pragma omp parallel for num_threads(32) 
  for (size_t i = 0; i < iterators.size(); i++) {
    auto &file_handler = *(iterators[i]);
    if (file_handler.second == nullptr) continue;

    bool is_dirty;
    client->GetDirty(file_handler.first, is_dirty);
    if (!is_dirty) continue;

    spdlog::trace("[FlushDirtySegmentsAndDeleteFromcache] Flush file: {} with size {}", file_handler.second->GetFilePath(), file_handler.second->file_size());

    // TODO we need a write lock
    UnswizzleFlushSwizzle(file_handler.first, file_handler.second, false);
    client->ClearDirty(file_handler.first);
    if (destroy_segment) {
      DestroySegment(file_handler.first);
    }
  }
  return NOERROR;
}

ReturnStatus ChunkCacheManager::GetRemainingMemoryUsage(size_t &remaining_memory_usage) {
  remaining_memory_usage = client->GetRemainingMemory();
  return NOERROR;
}

int ChunkCacheManager::GetRefCount(ChunkID cid) {
  return client->GetRefCount(cid);
}

// Return true if the given ChunkID is not valid
bool ChunkCacheManager::CidValidityCheck(ChunkID cid) {
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
size_t ChunkCacheManager::GetSegmentSize(ChunkID cid, std::string file_path) {
  D_ASSERT(file_handlers[cid] != nullptr);
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handlers[cid]->GetRequestedSize();
}

// Return the size of file
size_t ChunkCacheManager::GetFileSize(ChunkID cid, std::string file_path) {
  D_ASSERT(file_handlers[cid] != nullptr);
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handlers[cid]->file_size();
}

Turbo_bin_aio_handler* ChunkCacheManager::GetFileHandler(ChunkID cid) {
  auto file_handler = file_handlers.find(cid);
  D_ASSERT(file_handler != file_handlers.end());
  D_ASSERT(file_handler->second != nullptr);
  if (file_handler->second->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handler->second;
}

void ChunkCacheManager::ReadData(ChunkID cid, std::string file_path, void* ptr, size_t size_to_read, bool read_data_async) {
  auto file_handler = file_handlers.find(cid);
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }  
  // file_handlers[cid]->Read(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
  file_handlers[cid]->ReadWithSplittedIORequest(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
  if (!read_data_async) file_handlers[cid]->WaitForMyIoRequests(true, true);
}

void ChunkCacheManager::WriteData(ChunkID cid) {
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  //client->Flush(cid, file_handlers[cid]);
  return;
}

ReturnStatus ChunkCacheManager::CreateNewFile(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy) {
  D_ASSERT(file_handlers.find(cid) == file_handlers.end());
  file_handlers[cid] = new Turbo_bin_aio_handler();
  ReturnStatus rs = file_handlers[cid]->OpenFile((file_path + std::to_string(cid)).c_str(), true, true, true, true);
  D_ASSERT(rs == NOERROR);
  
  // Compute aligned file size
  int64_t alloc_file_size = ((alloc_size + sizeof(size_t) - 1 + 512) / 512) * 512;

  file_handlers[cid]->SetRequestedSize(alloc_size + sizeof(size_t));
  file_handlers[cid]->ReserveFileSize(alloc_file_size);
  file_handlers[cid]->SetCanDestroy(can_destroy);
  return rs;
}

void *ChunkCacheManager::MemAlign(uint8_t** ptr, size_t segment_size, size_t required_memory_size, Turbo_bin_aio_handler* file_handler) {
  void* target_ptr = (void*) *ptr;
  std::align(512, segment_size, target_ptr, required_memory_size);
  if (target_ptr == nullptr) {
    exit(-1);
    // TODO throw exception
  }

  size_t real_requested_segment_size = segment_size - sizeof(size_t);
  memcpy(target_ptr, &real_requested_segment_size, sizeof(size_t));
  *ptr = (uint8_t*) target_ptr;
  file_handler->SetDataPtr(*ptr);
  *ptr = *ptr + sizeof(size_t);

  return target_ptr;
}

}
