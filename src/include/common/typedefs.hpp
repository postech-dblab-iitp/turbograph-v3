#pragma once

#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>
#include <memory>
#include <algorithm>

namespace duckdb {

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

typedef std::string Label;
typedef std::string Labels;
typedef std::string NodeLabel;
typedef std::string EdgeType;
typedef std::vector<std::string> PropertyKeys;

}  // namespace duckdb
