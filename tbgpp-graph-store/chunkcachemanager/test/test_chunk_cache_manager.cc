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


int main(int argc, char **argv) {
  DiskAioParameters::NUM_THREADS = 1;
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);

  std::string file_path = "/home/tslee/data/seg";
  int64_t* buf_ptr;
  size_t buf_size;

  ChunkCacheManager ccm;
  ccm.CreateSegment(0, file_path, 64 * 1024, false);
  ccm.PinSegment(0, file_path, (uint8_t**) &buf_ptr, &buf_size);

  fprintf(stdout, "size = %ld", buf_size);
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    buf_ptr[i] = i;
  }
  for (int i = 0; i < buf_size / sizeof(int64_t); i++) {
    fprintf(stdout, "buf_ptr[%d] = %ld\n", i, buf_ptr[i]);
  }
  return 0;
}