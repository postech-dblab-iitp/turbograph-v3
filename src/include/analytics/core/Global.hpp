#pragma once
#ifndef GLOBAL_H
#define GLOBAL_H

/*
 * Design of the Global
 *
 * In this file, macro constants and functions used in the turbograph system 
 * are defined. Several optimization flags are also defined. By commenting 
 * out each flag, you can run the system without each optimization applied.
 */

#include <sys/stat.h>
#include <omp.h>
#include <string>
#include <algorithm>
#include <immintrin.h>
#include <mpi.h>

#include "analytics/core/TypeDef.hpp"

#define GLOG_NO_ABBREVIATED_SEVERITIES
#include "analytics/glog/logging.hpp"

// Optimization Flags
// For no_reorder & no_prune, turn off OPT 2 & OPT 3 (Base & +TR)
// For distance_prune & distance_po_prune, turn off OPT 3 (+NP)
// For tc, turn on OPT 2 & OPT 3 (+SWS)
#define NEIGHBOR_PRUNING // OPT 2
#define OPTIMIZE_WINDOW_SHARING // OPT 3
#define OPTIMIZE_SPARSE_COMPUTATION // OPT 4
#define COUNTING_METHOD // OPT 5

// Single-Machine Execution Optimization Flag
#define TEMP_SINGLE_MACHINE
//#define GB_VERSIONED_ARRAY
//#define NO_LOGGING_VERSIONED_ARRAY

#define MAT_ADJ_CURRENT_AND_DELTA
#define NUM_BITS_FOR_DELETED_VERSION 8 // Allow maximum 256 versions
#define DELETE_MASKING_MSBS 0xff00000000000000 //XXX 64bit assumed
#define DELETE_MASKING_LSBS 0x00000000000000ff //XXX 64bit assumed

#ifdef COUNTING_METHOD
#define NUM_BITS_FOR_CNT_64BIT 16 // Allow maximum 256 cnts
#define MAX_CNT_64BIT 32767
#define MIN_CNT_64BIT -32768
#define MAX_CNT2_64BIT 9223372036854775807
#define MIN_CNT2_64BIT -9223372036854775808
//#define NUM_BITS_FOR_CNT_64BIT 32 // Allow maximum 256 cnts
//#define MAX_CNT_64BIT 2147483647
//#define MIN_CNT_64BIT -2147483648
#define NUM_BITS_FOR_CNT_32BIT 8 // Allow maximum 256 cnts
#define MAX_CNT_32BIT 127
#define MIN_CNT_32BIT -128
#define CNT_MASKING_MSBS_64BIT 0xffff000000000000
#define CNT_MASKING_LSBS_64BIT 0x000000000000ffff
//#define CNT_MASKING_MSBS_64BIT 0xffffffff00000000
//#define CNT_MASKING_LSBS_64BIT 0x00000000ffffffff
#define CNT_MASKING_MSBS_32BIT 0xff000000
#define CNT_MASKING_LSBS_32BIT 0x000000ff
#define GET_CNT2_64BIT(x) (((int64_t)x) & CNT_MASKING_MSBS_32BIT)
#define GET_CNT_64BIT(x) ((int64_t)x >> 48)
#define GET_CNT_32BIT(x) (((int32_t)x >> (32 - NUM_BITS_FOR_CNT_32BIT)))
//#define GET_CNT_64BIT(x) ((x >> (64 - NUM_BITS_FOR_CNT_64BIT)) & (CNT_MASKING_LSBS_64BIT))
//#define GET_CNT_32BIT(x) ((x >> (32 - NUM_BITS_FOR_CNT_32BIT)) & (CNT_MASKING_LSBS_32BIT))
#define GET_VAL_64BIT(x) (((uint64_t) x) & ~(CNT_MASKING_MSBS_64BIT))
#define GET_VAL_32BIT(x) (((uint32_t) x) & ~(CNT_MASKING_MSBS_32BIT))
#define MARK_CNT_64BIT(val, cnt) val = val | ((uint64_t) cnt << (64 - NUM_BITS_FOR_CNT_64BIT));
#define MARK_CNT_32BIT(val, cnt) val = val | ((uint32_t) cnt << (32 - NUM_BITS_FOR_CNT_32BIT));
#define MARK_CNT2_64BIT(val, cnt) val = val | ((uint64_t) cnt);
#define MARK_CNT_OUTPUT_64BIT(output, val, cnt) output = val | ((uint64_t) cnt << (64 - NUM_BITS_FOR_CNT_64BIT));
#define MARK_CNT_OUTPUT_32BIT(output, val, cnt) output = val | ((uint32_t) cnt << (32 - NUM_BITS_FOR_CNT_32BIT));
#else
#define NUM_BITS_FOR_CNT_64BIT 2
#define MAX_CNT_64BIT 1
#define MIN_CNT_64BIT -2
#define CNT_MASKING_MSBS_64BIT 0xc000000000000000
#define CNT_MASKING_LSBS_64BIT 0x0000000000000003
//#define NUM_BITS_FOR_CNT_64BIT 2
//#define MAX_CNT_64BIT 1
//#define MIN_CNT_64BIT -2
//#define CNT_MASKING_MSBS_64BIT 0xc000000000000000
//#define CNT_MASKING_LSBS_64BIT 0x0000000000000003
#define NUM_BITS_FOR_CNT_32BIT 2
#define MAX_CNT_32BIT 1
#define MIN_CNT_32BIT -2
#define CNT_MASKING_MSBS_32BIT 0xc0000000
#define CNT_MASKING_LSBS_32BIT 0x00000003
#define GET_CNT_64BIT(x) (((int64_t)x >> (64 - NUM_BITS_FOR_CNT_64BIT)))
#define GET_CNT_32BIT(x) (((int32_t)x >> (32 - NUM_BITS_FOR_CNT_32BIT)))
#define GET_VAL_64BIT(x) (((uint64_t) x) & ~(CNT_MASKING_MSBS_64BIT))
#define GET_VAL_32BIT(x) (((uint32_t) x) & ~(CNT_MASKING_MSBS_32BIT))
#define MARK_CNT_64BIT(val, cnt) val = val | ((uint64_t) cnt << (64 - NUM_BITS_FOR_CNT_64BIT));
#define MARK_CNT_32BIT(val, cnt) val = val | ((uint32_t) cnt << (32 - NUM_BITS_FOR_CNT_32BIT));
#define MARK_CNT_OUTPUT_64BIT(output, val, cnt) output = val | ((uint64_t) cnt << (64 - NUM_BITS_FOR_CNT_64BIT));
#define MARK_CNT_OUTPUT_32BIT(output, val, cnt) output = val | ((uint32_t) cnt << (32 - NUM_BITS_FOR_CNT_32BIT));
#endif

#define TARGET_VID (-1)
#define TARGET_VID2 (1009015)
#define INCREMENTAL_DEBUGGING_TARGET(x) ((false))
//#define INCREMENTAL_DEBUGGING_TARGET(x) ((true))
//#define INCREMENTAL_DEBUGGING_TARGET(x) ((x == TARGET_VID) || (x == TARGET_VID2))

#define MAX_NUM_MACHINES 128
#define MAX_NUM_VERSIONS 8192
#define MAX_NUM_CPU_CORES 32
#define MAX_NUM_PER_THREAD_DATASTRUCTURE 64
#define MAX_NUM_DB_TYPES 9

#define PerThreadLgbDirtyFlagGranule (1024)

#define TBGPP_PAGE_SIZE (8*1024L)
//#define PER_THREAD_BUFF_SIZE (768*1024)
#define PER_THREAD_BUFF_SIZE (840*1024)
#define PER_THREAD_MAXIMUM_ONGOING_DISK_AIO 512
//#define PER_THREAD_MAXIMUM_ONGOING_DISK_AIO 128

#define TURBO_BIN_AIO_MAX_PENDING_REQS 36
#define MAT_ADJ_MAX_PENDING_REQS 24

#define DEFAULT_NUM_CORES_FOR_DISK_AND_NETWORK 4
#define DEFAULT_NIO_THREADS 24
//#define DEFAULT_NIO_BUFFER_SIZE (3*256*1024)
#define DEFAULT_NIO_BUFFER_SIZE (11*105*1024)
#define DEFAULT_NIO_PREALLOCATED_BUFFER_TOTAL_SIZE (3*1024*1024*1024L)
#define DEFAULT_NIO_DYNAMIC_BUFFER_TOTAL_SIZE (3*1024*1024*1024L)
#define DEFAULT_NIO_NUM_BITMAP_FOR_SENDER 3
#define DEFAULT_NIO_NUM_BITMAP_FOR_RECEIVER 6
#define DEFAULT_NUM_GENERAL_TCP_CONNECTIONS 8

//#define VECTOR_UPDATE_AGGREGATION_THRESHOLD (4*1024L)
#define VECTOR_UPDATE_AGGREGATION_THRESHOLD (24*1024L)

#define TCP_MAX_FAIL_COUNT 5
#define HISTOGRAM_BIN_WIDTH 1024

// Intersection
#define BINARY_SPLITTING_THRESHOLD 256
#define BINARY_SPLITTING_MAX_DEGREE 65536
#define BINARY_SPLITTING_MAGNITUDE 40
#define MAXIMUM_BINARY_SPLITTING_IDX_SIZE 64

#define SETBIT 0x01
#define RESETBIT 0x00

//#define PACKED __attribute__((packed)) // XXX - syko: it's defined already in MPI

#define NEVER_INLINE  __attribute__((noinline))
#define ALWAYS_INLINE __attribute__(())
#define UNUSED __attribute__((unused))


#ifdef PERFORMANCE
//#define ALWAYS_ASSERT(expr) (expr) ? (void)0 : abort_();
//#define ALWAYS_ASSERT(expr) LOG_ASSERT((expr))
#define ALWAYS_ASSERT(expr)
#else
//#define ALWAYS_ASSERT(expr) LOG_ASSERT_THREAD((expr))
#define ALWAYS_ASSERT(expr) LOG_ASSERT((expr))
#endif

#ifdef CHECK_INVARIANTS
#define INVARIANT(expr) LOG_ASSERT(expr)
#else
#define INVARIANT(expr) ((void)0)
#endif

//#define USE_TG_VECTOR

#ifdef USE_TG_VECTOR
#define VECTOR TG_Vector
#else
#define VECTOR std::vector
#endif


// TODO - tslee
// Move below codes into another file?
enum IteratorMode_t {
	PARTIAL_LIST,
	FULL_LIST
};

struct UserArguments {
	//	static std::string BASE_WORKSPACE_PATH;
	static std::string WORKSPACE_PATH;
	static std::string RAWDATA_PATH;

	static int64_t NUM_THREADS;
	static int64_t NUM_TOTAL_CPU_CORES;
	static int64_t NUM_CPU_SOCKETS;
	static int64_t NUM_DISK_AIO_THREADS;

	static int64_t VECTOR_PARTITIONS;
	static int64_t NUM_SUBCHUNKS_PER_EDGE_CHUNK;
	static int64_t NUM_ITERATIONS;
	static int64_t PHYSICAL_MEMORY;
	static int64_t SCHEDULE_TYPE;

	static int64_t MAX_LEVEL;
	static int64_t CURRENT_LEVEL;
	static bool USE_DEGREE_ORDER_REPRESENTATION;
	static IteratorMode_t ITERATOR_MODE;
	static bool BUILD_DB;

	static int64_t DEGREE_THRESHOLD;

	static int64_t UPDATE_VERSION;
	static int64_t SUPERSTEP;

	static int64_t BEGIN_VERSION;
	static int64_t END_VERSION;
	static int64_t MAX_VERSION;

	static bool IS_GRAPH_DIRECTED_GRAPH_OR_NOT; // ..._OR_UNDIRECTED
	static bool INCREMENTAL_PROCESSING;
	static bool USE_INCREMENTAL_SCATTER;
	static bool USE_INCREMENTAL_APPLY;
	static bool USE_PULL;
	static bool RUN_SUBQUERIES;
	static bool RUN_PRUNING_SUBQUERIES;
	static bool OV_FLUSH;
	static bool GRAPH_IN_MEMORY;
  	static bool QUERY_ONGOING;

	static int INC_STEP;
	static int PRUNING_BFS_LV;
	static bool INS_OR_DELETE;

	static bool LOAD_DELETEDB;
	static bool INSERTION_WORKLOAD;

	static EdgeType EDGE_DB_TYPE;
	static DynamicDBType DYNAMIC_DB_TYPE;

	static bool USE_DELTA_NWSM;
	static bool USE_FULLIST_DB;
    static bool ONLY_FIRST_SUPERSTEP;
	static int64_t tmp;

	static int LATENT_FACTOR;
	static int64_t LATENT_RANDOM_NUM_DECIMAL_PLACE;
	static int64_t LATENT_RANDOM_MAX_VALUE;

	static int NUM_TOTAL_SUBQUERIES;
	static int CURRENT_SUBQUERY_IDX;
};

#endif

