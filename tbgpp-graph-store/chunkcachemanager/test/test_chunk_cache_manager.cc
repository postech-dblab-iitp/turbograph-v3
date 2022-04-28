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
  ChunkCacheManager ccm;

  ccm.CreateSegment(0, "home/tslee/data/seg0", 64 * 1024, false);

  return 0;
}