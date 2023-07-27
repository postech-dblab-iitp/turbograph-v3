#include <string>
#include <thread>
#include <unordered_set>
#include <filesystem>
#include <cstdint>

#include "cache/chunk_cache_manager.h"
#include "Turbo_bin_aio_handler.hpp"
#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "icecream.hpp"
#include "extent/compression/compression_header.hpp"
#include "common/types/string_type.hpp"


namespace duckdb {

ChunkCacheManager* ChunkCacheManager::ccm;

ChunkCacheManager::ChunkCacheManager(const char *path) {
  // Init LightningClient
  client = new LightningClient("/tmp/lightning", "password");

  // Initialize file handlers
// icecream::ic.enable();
// IC();
  std::string partition_path = std::string(path);
// IC(partition_path);
  for (const auto &partition_entry : std::filesystem::directory_iterator(partition_path)) { // /path/
    std::string partition_entry_path = std::string(partition_entry.path());
    std::string partition_entry_name = partition_entry_path.substr(partition_entry_path.find_last_of("/") + 1);
    // IC(partition_entry_name);
    if (StringUtil::StartsWith(partition_entry_name, "part_")) {
      std::string extent_path = partition_entry_path + std::string("/");
      // IC(extent_path);
      for (const auto &extent_entry : std::filesystem::directory_iterator(extent_path)) { // /path/part_/
        std::string extent_entry_path = std::string(extent_entry.path());
        std::string extent_entry_name = extent_entry_path.substr(extent_entry_path.find_last_of("/") + 1);
        if (StringUtil::StartsWith(extent_entry_name, "ext_")) {
          std::string chunk_path = extent_entry_path + std::string("/");
          // IC(chunk_path);
          for (const auto &chunk_entry : std::filesystem::directory_iterator(chunk_path)) { // /path/part_/ext_/
            std::string chunk_entry_path = std::string(chunk_entry.path());
            std::string chunk_entry_name = chunk_entry_path.substr(chunk_entry_path.find_last_of("/") + 1);
            ChunkDefinitionID chunk_id = (ChunkDefinitionID) std::stoull(chunk_entry_name.substr(chunk_entry_name.find("_") + 1));
            // fprintf(stdout, "chunk_entry_path %s\n", chunk_entry_path.c_str());

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
            // file_handlers[chunk_id]->WaitForMyIoRequests(true, true);
            // if (chunk_id == 0) {
              // fprintf(stdout, "%p %d\n", first_block, file_handlers[chunk_id]->fdval());
              // int *arr = (int *)first_block;
              // for (size_t i = 0; i < 512 / sizeof(int); i++) {
              //   std::cout << std::hex << std::setfill('0') << std::setw(2) << arr[i] << " ";
              // }
              // fprintf(stdout, "\n");
            // }

            // XXX Temporary use mmap
            // int open_flag, mmap_prot, mmap_flag, file_descriptor;
            // size_t file_size_ = file_handlers[chunk_id]->file_size();
            // file_descriptor = file_handlers[chunk_id]->fdval();
            // open_flag = O_RDONLY;
            // mmap_prot = PROT_READ;
            // mmap_flag = MAP_SHARED;
            // char* pFileMap_ = (char *) mmap64(NULL, file_size_, mmap_prot, mmap_flag, file_descriptor, 0);

            size_t requested_size = ((size_t *)first_block)[0];
            // memcpy(&requested_size, first_block, sizeof(size_t));
            file_handlers[chunk_id]->SetRequestedSize(requested_size);
            // fprintf(stdout, "Open %s, requested_size = %ld, file_size = %ld\n", chunk_entry_path.c_str(), requested_size, file_handlers[chunk_id]->file_size());
            // munmap(pFileMap_, file_size_);
            delete first_block;
          }
        }
      }
    }
  }
// icecream::ic.disable();
  //   file_handlers[cid] = new Turbo_bin_aio_handler();
  // ReturnStatus rs = file_handlers[cid]->OpenFile((file_path + std::to_string(cid)).c_str(), true, true, true, true);
  // Initialize file_handlers as nullptr
  //for (int i = 0; i < NUM_MAX_SEGMENTS; i++) file_handlers[i] = nullptr;
}

ChunkCacheManager::~ChunkCacheManager() {
  fprintf(stdout, "Deconstruct ChunkCacheManager\n");
  for (auto &file_handler: file_handlers) {
    if (file_handler.second == nullptr) continue;

    bool is_dirty;
    client->GetDirty(file_handler.first, is_dirty);
    if (!is_dirty) continue;

    file_handler.second->FlushAll();
    file_handler.second->WaitAllPendingDiskIO(false);
    file_handler.second->Close();
  }
}

ReturnStatus ChunkCacheManager::PinSegment(ChunkID cid, std::string file_path, uint8_t** ptr, size_t* size, bool read_data_async, bool is_initial_loading) {
// icecream::ic.enable();
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
// IC(cid);

  // Pin Segment using Lightning Get()
  if (client->Get(cid, ptr, size) != 0) {
    // IC();
    // Get() fail: 1) object not found, 2) object is not sealed yet
    // TODO: Check if there is enough memory space

    //size_t deleted_size = 0;
    //if (!IsMemorySpaceEnough(segment_size)) {
    //  FindVictimAndDelete(segment_size, deleted_size);
    //  // Replacement algorithm은 일단 지원 X
    //}

    if (client->Create(cid, ptr, required_memory_size) == 0) {
      // IC();
      // TODO: Memory usage should be controlled in Lightning Create

      // Align memory
      void *file_ptr = MemAlign(ptr, segment_size, required_memory_size, file_handler);
      // IC();
      
      // Read data & Seal object
      ReadData(cid, file_path, file_ptr, file_size, read_data_async);
      // IC();
      client->Seal(cid);
      // if (!read_data_async) client->Seal(cid); // WTF???
      // IC();
      *size = segment_size - sizeof(size_t);
      // IC();
      if(!is_initial_loading) SwizzleVarchar(*ptr);
    } else {
      // IC();
      // Create fail -> Subscribe object
      client->Subscribe(cid);
      // IC();
      int status = client->Get(cid, ptr, size);
      // fprintf(stdout, "cid %ld, ptr %p, status %d\n", cid, ptr, status);
      // IC();
      D_ASSERT(status == 0);

      // Align memory & adjust size
      MemAlign(ptr, segment_size, required_memory_size, file_handler);
      // IC();
      *size = segment_size - sizeof(size_t);
      // IC();
    }

    // icecream::ic.disable();
    return NOERROR;
  }
  // IC();
  // Get() success. Align memory & adjust size
  MemAlign(ptr, segment_size, required_memory_size, file_handler);
  // IC();
  *size = segment_size - sizeof(size_t);
  // IC();
// icecream::ic.disable();

  return NOERROR;
}

ReturnStatus ChunkCacheManager::UnPinSegment(ChunkID cid) {
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    // throw InvalidInputException("[UnpinSegment] invalid cid");

  // Unpin Segment using Lightning Release()
  client->Release(cid);
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
  // Check validity of given alloc_size
  // It fails if 1) the storage runs out of space, 2) the alloc_size exceeds the limit size (if it exists)
  if (AllocSizeValidityCheck(alloc_size))
    exit(-1);
    //TODO
    //throw InvalidInputException("[CreateSegment] invalid alloc_size");

  // Create file for the segment
  return CreateNewFile(cid, file_path, alloc_size, can_destroy);
}

ReturnStatus ChunkCacheManager::DestroySegment(ChunkID cid) {
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[DestroySegment] invalid cid");

  // TODO: Check the reference count
  // If the count > 0, we cannot destroy the segment
  
  // Delete the segment from the buffer using Lightning Delete()
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  client->Delete(cid, file_handlers[cid]);
  file_handlers[cid]->Close();
  file_handlers[cid] = nullptr;
  //AdjustMemoryUsage(-GetSegmentSize(cid)); // need type casting
  return NOERROR;
}

ReturnStatus ChunkCacheManager::FinalizeIO(ChunkID cid, bool read, bool write) {
  file_handlers[cid]->WaitForMyIoRequests(read, write);
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
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  file_handlers[cid]->Read(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
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
// IC();
  void* target_ptr = (void*) *ptr;
// IC();
  std::align(512, segment_size, target_ptr, required_memory_size);
// IC();
  if (target_ptr == nullptr) {
    // IC();
    exit(-1);
    // TODO throw exception
  }
// IC();
  size_t real_requested_segment_size = segment_size - sizeof(size_t);
  // fprintf(stdout, "segment_size %ld, real_requested_size %ld\n", segment_size, real_requested_segment_size);
  memcpy(target_ptr, &real_requested_segment_size, sizeof(size_t));
  *ptr = (uint8_t*) target_ptr;
  file_handler->SetDataPtr(*ptr);
  *ptr = *ptr + sizeof(size_t);
// IC();
  return target_ptr;
}

void ChunkCacheManager::SwizzleVarchar(uint8_t* ptr) {
  // Read Compression Header
  CompressionHeader comp_header;
  memcpy(&comp_header, ptr, sizeof(CompressionHeader));

  // Check Swizzle Flag
  if (!comp_header.swizzling_needed) return;

  // Calculate Offsets
  size_t size = comp_header.data_len;
  size_t string_t_offset = sizeof(CompressionHeader);
  size_t string_data_offset = sizeof(CompressionHeader) + size * sizeof(string_t);

  // Iterate over strings and swizzle
  for (int i = 0; i < size; i++) {
    // Get string
    string_t& str = ((string_t *)(ptr + string_t_offset))[i];

    // Check not inlined
    if (!str.IsInlined()) {
      // Calculate address
      uint64_t offset = str.GetOffset();
      uint8_t* address = ptr + string_data_offset + offset;
      
      // Replace offset to address
      size_t size_without_offset = sizeof(string_t) - sizeof(offset);
      memcpy(ptr + string_t_offset + size_without_offset, &address, sizeof(address));
    }

    // Update offset
    string_t_offset += sizeof(string_t);
  }
}

void ChunkCacheManager::UnswizzleVarchar(uint8_t* ptr) {
  // Read Compression Header
  CompressionHeader comp_header;
  memcpy(&comp_header, ptr, sizeof(CompressionHeader));

  // Check Swizzle Flag
  if (!comp_header.swizzling_needed) return;

  // Calculate Offsets
  size_t size = comp_header.data_len;
  size_t string_t_offset = sizeof(CompressionHeader);
  size_t string_data_offset = sizeof(CompressionHeader) + size * sizeof(string_t);

  // Iterate over strings and unswizzle
  for (int i = 0; i < size; i++) {
    // Get string
    string_t& str = ((string_t *)(ptr + string_t_offset))[i];

    // Check not inlined
    if (!str.IsInlined()) {
      // Calculate offset
      auto str_ptr = str.GetDataUnsafe();
      uint64_t offset = reinterpret_cast<uintptr_t>(str_ptr) - (reinterpret_cast<uintptr_t>(ptr) + string_data_offset);

      // Replace address to offset
      size_t size_without_address = sizeof(string_t) - sizeof(str_ptr);
      memcpy(ptr + string_t_offset + size_without_address, &offset, sizeof(offset));
    }

    // Update offset
    string_t_offset += sizeof(string_t);
  }
}

}