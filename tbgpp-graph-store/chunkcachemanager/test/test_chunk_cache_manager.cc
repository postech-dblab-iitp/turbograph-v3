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

SCENARIO ("Client connection test", "[chunkcachemanager]") {
  DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

  int x;
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  ChunkCacheManager ccm;
  REQUIRE(0 == 0);
}

int main(int argc, char **argv) {
  int result = Catch::Session().run(argc, argv);                               
  return 0; 
}

/*int main(int argc, char **argv) {
  // TODO: How to initialize these parameters?
  DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

  int x;
  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  ChunkCacheManager ccm;
  ccm.CreateSegment(0, file_path, 512, false);
  ccm.PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size);
  
  fprintf(stdout, "size = %ld\n", buf_size);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    buf_ptr[i] = i;
  }
  ccm.SetDirty(0);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    fprintf(stdout, "buf_ptr[%d] = %ld\n", i, buf_ptr[i]);
  }
  ccm.UnPinSegment(0);
  
  buf_ptr = nullptr;
  buf_size = 0;
  ccm.PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size);
  fprintf(stdout, "size = %ld\n", buf_size);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    fprintf(stdout, "buf_ptr[%d] = %ld\n", i, buf_ptr[i]);
  }

  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    buf_ptr[i] = i * 2;
  }

  ccm.UnPinSegment(0);

  buf_ptr = nullptr;
  buf_size = 0;

  ccm.PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size);
  fprintf(stdout, "size = %ld\n", buf_size);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    fprintf(stdout, "buf_ptr[%d] = %ld\n", i, buf_ptr[i]);
  }
  ccm.UnPinSegment(0);
  ccm.DestroySegment(0);

  buf_ptr = nullptr;
  buf_size = 0;

  ccm.PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size);
  fprintf(stdout, "size = %ld\n", buf_size);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    fprintf(stdout, "buf_ptr[%d] = %ld\n", i, buf_ptr[i]);
  }
  ccm.UnPinSegment(0);
  ccm.DestroySegment(0);
  return 0;
}*/