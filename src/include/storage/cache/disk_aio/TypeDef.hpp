#ifndef DISKAIO_TYPEDEF_H
#define DISKAIO_TYPEDEF_H

#include <stdint.h>
#include <string>

#ifndef PER_THREAD_PARAMETERS
#define PER_THREAD_PARAMETERS
#define MAX_NUM_PER_THREAD_DATASTRUCTURE 64
#define PER_THREAD_MAXIMUM_ONGOING_DISK_AIO 512
#endif

#ifndef CACHE_PARAMETERS
#define CACHE_PARAMETERS
#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHELINE_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define CACHE_PADOUT  \
  char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))
#endif

// struct PaddedIdx {
// 	int64_t idx;
// 	CACHE_PADOUT;
// };

//TODO change file name

struct DiskAioParameters {
  static int64_t NUM_THREADS;
  static int64_t NUM_TOTAL_CPU_CORES;
  static int64_t NUM_CPU_SOCKETS;
  static int64_t NUM_DISK_AIO_THREADS;
  static std::string WORKSPACE;
};

// enum IOModes {
//   READ_IO = 0,
//   WRITE_IO = 1,
// };

#endif
