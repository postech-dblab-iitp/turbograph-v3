//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <cstdint>
#include "common/string.hpp"
#include "common/winapi.hpp"

namespace duckdb {

// API versions
// if no explicit API version is defined, the latest API version is used
// Note that using older API versions (i.e. not using DUCKDB_API_LATEST) is deprecated.
// These will not be supported long-term, and will be removed in future versions.

#ifndef DUCKDB_API_0_3_1
#define DUCKDB_API_0_3_1 1
#endif
#ifndef DUCKDB_API_0_3_2
#define DUCKDB_API_0_3_2 2
#endif
#ifndef DUCKDB_API_LATEST
#define DUCKDB_API_LATEST DUCKDB_API_0_3_2
#endif

#ifndef DUCKDB_API_VERSION
#define DUCKDB_API_VERSION DUCKDB_API_LATEST
#endif

//! inline std directives that we use frequently
using std::move;
using std::shared_ptr;
using std::unique_ptr;
using std::weak_ptr;
using data_ptr = unique_ptr<char[]>;
using std::make_shared;

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA "main"
#define TEMP_SCHEMA    "temp"
#define INVALID_SCHEMA ""

#define DEFAULT_GRAPH "graph1"
#define DEFAULT_VERTEX_PARTITION_PREFIX "vpart_"
#define DEFAULT_EDGE_PARTITION_PREFIX "epart_"
#define DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX "vps_"
#define DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX "eps_"
#define DEFAULT_EXTENT_PREFIX "ext_"
#define DEFAULT_CHUNKDEFINITION_PREFIX "cdf_"
#define DEFAULT_TEMPORAL_INFIX "_temp_"

#define DYNAMIC_SCHEMA_INSTANTIATION

//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The type used for hashes
typedef uint64_t hash_t;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

//! Type used for the selection vector
typedef uint32_t sel_t;
//! Type used for storage (column) identifiers
typedef idx_t storage_t;
//! Type used for transaction timestamps
typedef idx_t transaction_t;

//! Type used for column identifiers
typedef idx_t column_t;
//! Special value used to signify the ROW ID of a table
extern const column_t COLUMN_IDENTIFIER_ROW_ID;

//! The maximum row identifier used in tables
extern const row_t MAX_ROW_ID;

// ID types for graph catalog
typedef uint16_t PartitionID;
typedef uint32_t ExtentID;
typedef uint16_t LocalExtentID;
typedef uint32_t LocalChunkDefinitionID;
typedef uint64_t ChunkDefinitionID;
typedef idx_t PropertyKeyID;
typedef idx_t PropertySchemaID;
typedef idx_t VertexLabelID;
typedef idx_t EdgeTypeID;

typedef uint64_t ChunkID;

#define GET_EXTENT_SEQNO_FROM_EID(eid) (eid & 0xFFFF);
#define GET_PARTITION_ID_FROM_EID(eid) (eid >> 16);
#define GET_EID_FROM_PARTITION_ID_AND_SEQNO(pid, seqno) ((pid << 16) | seqno);
#define GET_EID_FROM_PHYSICAL_ID(pid) (pid >> 32);
#define GET_SEQNO_FROM_PHYSICAL_ID(pid) (pid & 0x00000000FFFFFFFF);

extern const transaction_t TRANSACTION_ID_START;
extern const transaction_t MAX_TRANSACTION_ID;
extern const transaction_t MAXIMUM_QUERY_ID;
extern const transaction_t NOT_DELETED_ID;

extern const double PI;

struct DConstants {
	//! The value used to signify an invalid index entry
	static constexpr const idx_t INVALID_INDEX = idx_t(-1);
};

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static int SECTOR_SIZE = 4096;
	//! Block header size for blocks written to the storage
	constexpr static int BLOCK_HEADER_SIZE = sizeof(uint64_t);
	// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
	// default to 256KB. (1 << 18)
	constexpr static int BLOCK_ALLOC_SIZE = 262144;
	//! The actual memory space that is available within the blocks
	constexpr static int BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static int FILE_HEADER_SIZE = 4096;
};

uint64_t NextPowerOfTwo(uint64_t v);

} // namespace duckdb
