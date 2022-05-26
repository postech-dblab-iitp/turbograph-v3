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
#include <omp.h>

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

bool helper_check_file_exists (const std::string& name) {
    struct stat buffer;
    return (stat (name.c_str(), &buffer) == 0); 
}

TEST_CASE ("Multi Thread Test 1 - Create, Pin, UnPin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  REQUIRE(ChunkCacheManager::ccm->CreateSegment(0, file_path, 512, true) == NOERROR);
  REQUIRE(ChunkCacheManager::ccm->PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size) == NOERROR);
  REQUIRE(buf_size == 512);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    buf_ptr[i] = 0;
  }
  REQUIRE(ChunkCacheManager::ccm->UnPinSegment(0) == NOERROR);
}

TEST_CASE ("Multi Thread Test 1 - Pin, Write Data, Set Dirty, Unpin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr[DiskAioParameters::NUM_THREADS];
  size_t buf_size[DiskAioParameters::NUM_THREADS];

  // Initialize data structure
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) buf_ptr[i] = nullptr;
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) buf_size[i] = 0;

  int64_t target_value = 0;
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) target_value += i;

  #pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
  {
    int i = omp_get_thread_num();
    REQUIRE(ChunkCacheManager::ccm->PinSegment(0, file_path, (uint8_t**) &buf_ptr[i], &buf_size[i]) == NOERROR);
    REQUIRE(buf_size[i] == 512);
    for (int j = 0; j < buf_size[i] / sizeof(int64_t); j++) {
      std::atomic_fetch_add((std::atomic<int64_t>*) &buf_ptr[i][j], +i);
    }
    #pragma omp barrier
  }

  REQUIRE(ChunkCacheManager::ccm->GetRefCount(0) == DiskAioParameters::NUM_THREADS);
  #pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
  {
    int i = omp_get_thread_num();
    for (int j = 0; j < buf_size[i] / sizeof(int64_t); j++) {
      REQUIRE(buf_ptr[i][j] == target_value);
    }
  }

  #pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
  {
    REQUIRE(ChunkCacheManager::ccm->SetDirty(0) == NOERROR);
    REQUIRE(ChunkCacheManager::ccm->UnPinSegment(0) == NOERROR);
  }

  REQUIRE(ChunkCacheManager::ccm->GetRefCount(0) == 0);
}

TEST_CASE ("Multi Thread Test 1 - Pin, Read Data, Unpin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  int64_t target_value = 0;
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) target_value += i;

  REQUIRE(ChunkCacheManager::ccm->PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size) == NOERROR);
  REQUIRE(buf_size == 512);

  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    REQUIRE(buf_ptr[i] == target_value);
  }

  REQUIRE(ChunkCacheManager::ccm->UnPinSegment(0) == NOERROR);
}

TEST_CASE ("Multi Thread Test 1 - Destroy Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";

  REQUIRE(ChunkCacheManager::ccm->DestroySegment(0) == NOERROR);
  REQUIRE(!helper_check_file_exists(file_path + std::to_string(0)));
}

TEST_CASE ("Multi Thread Test 2 - Create Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  REQUIRE(ChunkCacheManager::ccm->CreateSegment(1, file_path, 512, true) == NOERROR);
}

TEST_CASE ("Multi Thread Test 2 - Pin, Write Data, Set Dirty, Unpin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr[DiskAioParameters::NUM_THREADS];
  size_t buf_size[DiskAioParameters::NUM_THREADS];

  // Initialize data structure
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) buf_ptr[i] = nullptr;
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) buf_size[i] = 0;

  int64_t target_value = 0;
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) target_value += i;

  #pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
  {
    int i = omp_get_thread_num();
    REQUIRE(ChunkCacheManager::ccm->PinSegment(1, file_path, (uint8_t**) &buf_ptr[i], &buf_size[i]) == NOERROR);
    REQUIRE(buf_size[i] == 512);
    if (i == 0) {
      for (int j = 0; j < buf_size[i] / sizeof(int64_t); j++) {
        buf_ptr[i][j] = 0;
      }
    }
    #pragma omp barrier
    for (int j = 0; j < buf_size[i] / sizeof(int64_t); j++) {
      std::atomic_fetch_add((std::atomic<int64_t>*) &buf_ptr[i][j], +i);
    }
    #pragma omp barrier
  }

  REQUIRE(ChunkCacheManager::ccm->GetRefCount(1) == DiskAioParameters::NUM_THREADS);
  #pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
  {
    int i = omp_get_thread_num();
    for (int j = 0; j < buf_size[i] / sizeof(int64_t); j++) {
      REQUIRE(buf_ptr[i][j] == target_value);
    }
  }

  #pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
  {
    REQUIRE(ChunkCacheManager::ccm->SetDirty(1) == NOERROR);
    REQUIRE(ChunkCacheManager::ccm->UnPinSegment(1) == NOERROR);
  }

  REQUIRE(ChunkCacheManager::ccm->GetRefCount(1) == 0);
}

TEST_CASE ("Multi Thread Test 2 - Pin, Read Data, Unpin Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  int64_t target_value = 0;
  for (int i = 0; i < DiskAioParameters::NUM_THREADS; i++) target_value += i;

  REQUIRE(ChunkCacheManager::ccm->PinSegment(1, file_path, (uint8_t**) &buf_ptr, &buf_size) == NOERROR);
  REQUIRE(buf_size == 512);

  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    REQUIRE(buf_ptr[i] == target_value);
  }

  REQUIRE(ChunkCacheManager::ccm->UnPinSegment(1) == NOERROR);
}

TEST_CASE ("Multi Thread Test 2 - Destroy Segment test", "[chunkcachemanager]") {
  std::string file_path = "/home/tslee/data/seg";

  REQUIRE(ChunkCacheManager::ccm->DestroySegment(1) == NOERROR);
  REQUIRE(!helper_check_file_exists(file_path + std::to_string(1)));
}

int main(int argc, char **argv) {
  // Initialize System Parameters
  DiskAioParameters::NUM_THREADS = 10;
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