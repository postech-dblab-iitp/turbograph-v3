#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <vector>

#include "chunk_cache_manager.h"

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

bool helper_check_file_exists (const std::string& name) {
    struct stat buffer;
    return (stat (name.c_str(), &buffer) == 0); 
}

TEST_CASE ("Client connection and Create, Pin, UnPin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  REQUIRE(ChunkCacheManager::ccm->CreateSegment(0, file_path, 512, true) == NOERROR);
  REQUIRE(ChunkCacheManager::ccm->PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size) == NOERROR);
  REQUIRE(ChunkCacheManager::ccm->UnPinSegment(0) == NOERROR);
}

TEST_CASE ("Pin, Write Data, Set Dirty, Unpin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  REQUIRE(ChunkCacheManager::ccm->PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size) == NOERROR);
  REQUIRE(buf_size == 512);
  
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    buf_ptr[i] = i;
  }

  REQUIRE(ChunkCacheManager::ccm->SetDirty(0) == NOERROR);
  REQUIRE(ChunkCacheManager::ccm->UnPinSegment(0) == NOERROR);
}

TEST_CASE ("Pin, Read Data, Unpin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  REQUIRE(ChunkCacheManager::ccm->PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size) == NOERROR);
  REQUIRE(buf_size == 512);
  
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    REQUIRE(buf_ptr[i] == i);
  }

  REQUIRE(ChunkCacheManager::ccm->UnPinSegment(0) == NOERROR);
}

TEST_CASE ("Destroy Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  REQUIRE(ChunkCacheManager::ccm->DestroySegment(0) == NOERROR);
  REQUIRE(!helper_check_file_exists(file_path + std::to_string(0)));
}

int main(int argc, char **argv) {
  // Initialize System Parameters
  DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

  // Initialize ChunkCacheManager
  ChunkCacheManager::ccm = new ChunkCacheManager();

  // Run Catch Test
  int result = Catch::Session().run(argc, argv);
                  
  return 0;
}