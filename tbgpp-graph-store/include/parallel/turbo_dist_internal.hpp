#ifndef TURBO_DIST_INTERNAL_H
#define TURBO_DIST_INTERNAL_H

#include <iostream>
#include <math.h>
#include <stdlib.h>
#include <iomanip>
#include <string>
#include <sstream>
#include <map>
#include <vector>
#include <mpi.h>

#include"common/constants.hpp"

static const int NTimers = 32;
extern duckdb::PartitionID num_partitions__;
extern duckdb::PartitionID my_partition_id__;


class PartitionStatistics {
	// friend class TurboDB;
	friend class LocalStatistics;
  public:
	PartitionStatistics() = default;

	static void init();
	// static void replicate();
	static void close();

	// inline static node_t& per_machine_first_node_id(PartitionID pid) {
	// 	return per_machine_first_node_id_[pid];
	// }
	// inline static node_t& per_machine_last_node_id(PartitionID pid) {
	// 	return per_machine_last_node_id_[pid];
	// }
	// inline static node_t& per_partition_num_nodes(PartitionID pid) {
	// 	return per_partition_num_nodes_[pid];
	// };
	// static node_t max_internal_nodes();
	static duckdb::PartitionID& num_machines();
	static duckdb::PartitionID& my_machine_id();
	// static node_t& my_num_internal_nodes();
	// static node_t& my_first_node_id();
	// static node_t& my_last_node_id();

	// inline static node_t& my_first_internal_vid() {
	// 	return per_machine_first_node_id_[my_partition_id_];
	// }
	// inline static node_t& my_last_internal_vid() {
	// 	return per_machine_last_node_id_[my_partition_id_];
	// }
    // inline static Range<node_t> my_internal_vid_range() {
    //     return Range<node_t>(my_first_internal_vid(), my_last_internal_vid());
    // }
	// static Range<node_t> my_chunkID_to_range(int64_t chunkID);
	// static Range<node_t> machine_id_and_chunk_idx_to_vid_range(PartitionID pid, int64_t chunkID);
	// static Range<node_t> per_edge_partition_vid_range(int64_t edge_partition_id);
	// static Range<node_t> per_machine_vid_range(int64_t machine_id);

	static void wait_for_all();

	// static node_t& num_total_nodes();
	// static node_t& max_num_nodes_per_vector_chunk();

	// static int64_t& num_target_vector_chunks();
	// static Range<int64_t>& my_vector_chunks();

	// static int64_t my_total_num_subchunks_;
	// static int64_t& my_total_num_subchunks();
	// static int64_t num_subchunks_per_edge_chunk_;
	// static int64_t& num_subchunks_per_edge_chunk();
	// static Range<int64_t> vector_chunks_to_edge_subchunks(Range<int64_t> vector_chunks);

	// static void print_partition_info(int64_t edge_db_file_size) ;

	// //performance measurement
	// static per_partition_array<double> per_partition_timers_[NTimers];
	// static per_partition_array<double> per_partition_minor_timers_[NTimers];

  public:

    // static node_t GetDstVidValue_(node_t vid) {
    //     return (vid) & ~(DELETE_MASKING_MSBS);
    // }
    // static node_t GetDeletedVersionOfAnEdge_(node_t vid) {
    //     node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
    //     return deleted_version == 0 ? std::numeric_limits<node_t>::max(): deleted_version;
    // }
    // static bool IsEdgePrevDeleted_(node_t vid) {
    //     node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
    //     if (deleted_version == 0) deleted_version = std::numeric_limits<node_t>::max();
    //     return deleted_version < UserArguments::UPDATE_VERSION;
    // }
    // static node_t GetPrevVisibleDstVid(node_t vid) {
    //     if (IsEdgePrevDeleted_(vid)) return -1;
    //     else return GetDstVidValue_(vid);
    // }
    // static bool IsEdgeDeleted_(node_t vid) {
    //     node_t deleted_version = (vid >> ((8 * sizeof(node_t)) - NUM_BITS_FOR_DELETED_VERSION)) & (DELETE_MASKING_LSBS);
    //     if (deleted_version == 0) deleted_version = std::numeric_limits<node_t>::max();
    //     return deleted_version <= UserArguments::UPDATE_VERSION;
    // }
    // static node_t GetVisibleDstVid(node_t vid) {
    //     if (IsEdgeDeleted_(vid)) return -1;
    //     else return GetDstVidValue_(vid);
    // }
    // static node_t VidToDegreeOrder(node_t vid);
	// static node_t VidToDegreeOrder(node_t vid, PartitionID pid);
	// static node_t DegreeOrderToVid(node_t vid);
	// static node_t DegreeOrderToVid(node_t vid, PartitionID pid);

  private:

	static duckdb::PartitionID& num_partitions_;
	static duckdb::PartitionID& my_partition_id_;

	// static node_t num_total_nodes_;
	// static node_t max_num_nodes_per_vector_chunk_;

	// static per_partition_elem<node_t> per_partition_num_nodes_;

	// static per_partition_elem<node_t> per_machine_first_node_id_;
	// static per_partition_elem<node_t> per_machine_last_node_id_;

	// //These vector measure the amount of communication.
	// //not thread-safe
	// static std::vector<int64_t> rget_amount_per_partition; //byte
	// static std::vector<int64_t> raccum_amount_per_partition; //byte

	// static int64_t num_target_vector_chunks_;
	// static std::vector<Range<int64_t>> per_machine_vector_chunks_;
	// static Range<int64_t> my_vector_chunks_;

	// static std::vector<Range<node_t>>  vector_chunk_id_to_vid_range_;
};


class LocalStatistics {
	// friend class TurboDB;
  public:

	// Partition-aware APIs
	// static bool IsInternalVertex(node_t node_id);
	static int64_t& page_buffer_size_in_bytes() ;

	// static node_t first_local_internal_vid() ;
	// static node_t last_local_internal_vid();

	// // (Global) VertexID to (Local) VertexID
	// inline static node_t Vid2Lid(node_t vid) {
    //     D_ASSERT(vid >= PartitionStatistics::my_first_internal_vid() && vid <= PartitionStatistics::my_last_internal_vid());
    //     return vid - PartitionStatistics::my_first_internal_vid();
	// }

	static void register_mem_alloc_info(const char* name, int64_t MBytes);
	static void print_mem_alloc_info();
	static int64_t get_total_mem_size_allocated();

	// performance measurement
	static std::vector<double> timers_[NTimers];
	static double timers_prv[NTimers];

	static std::vector<double> minor_timers_[NTimers];
	static double minor_timers_prv[NTimers];
  private:

	static int64_t page_buffer_size_in_bytes_;	// buffer size for data pages

	static std::map<std::string, int64_t> mem_alloc_info;
};

#endif
