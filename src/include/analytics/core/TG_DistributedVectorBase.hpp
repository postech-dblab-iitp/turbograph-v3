#pragma once
#ifndef TG_DISTRIBUTEDVECTORBASE_H
#define TG_DISTRIBUTEDVECTORBASE_H

/*
 * Design of the TG_DistributedVectorBase
 *
 * This class is the base class of TG_DistributedVector which implements the 
 * vertex window. It provides several APIs and static variables commonly used
 * in all TG_DistributedVector instances. The main uses are:
 *     - The request passed from RequestRespond class is transferred to the 
 *     vector that needs to process it. The following functions are included:
 *     (RespondSequentialVectorPull, RespondOutputVectorMessagePush,
 *     RespondOutputVectorPull, RespondInputVectorPull)
 *     - It provides functions that can be used to manage various flags related
 *     to global gather buffer. The following flags are included:
 *     (ggb_reaggregation_flags, ggb_msg_received_flags, ggb_reapply_flags)
 */

#include <sys/stat.h>
#include <thread>
#include <atomic>
#include <algorithm>

#include "util.hpp"
#include "atom.hpp"
#include "Aio_Helper.hpp"
#include "atomic_util.hpp"
#include "turbo_tcp.hpp"
#include "RequestRespond.hpp"
#include "turbo_dist_internal.hpp"

#include "TwoLevelBitMap.hpp"

// TODO not used since IndexedWritebackMessage is not used
template <typename T>
struct IndexedWritebackMessageEntry {
	typedef int32_t idx_t;
	idx_t idx;
	T value;
};


// TODO not used 
template <typename T>
struct IndexedWritebackMessage {
	typedef int32_t idx_t;

	static const int32_t max_num_entries = (DEFAULT_NIO_BUFFER_SIZE - sizeof(node_t) - sizeof(int32_t)) / sizeof(IndexedWritebackMessageEntry<T>);

	node_t first_dst_vid;
	int32_t num_msgs;
	IndexedWritebackMessageEntry<T> entries[(DEFAULT_NIO_BUFFER_SIZE - sizeof(node_t) - sizeof(int32_t)) / sizeof(IndexedWritebackMessageEntry<T>)];

	int32_t atomic_add_num_msgs (int32_t add) {
		return std::atomic_fetch_add((std::atomic<idx_t>*) &num_msgs, add);
	}

	node_t get_first_dst_vid() {
		return first_dst_vid;
	}
	node_t get_dst_vid(int32_t idx) {
		return (first_dst_vid + entries[idx].idx);
	}
	T get_value(int32_t idx) {
		return entries[idx].value;
	}

	void set_first_dst_vid(node_t vid) {
		first_dst_vid = vid;
	}
	void set_dst_vid(int32_t idx, node_t vid) {
		entries[idx].idx = vid - first_dst_vid;
	}
	void set_value(int32_t idx, T value) {
		entries[idx].value = value;
	}
	void insert_msg(node_t vid, T value) {
		entries[num_msgs].idx = vid - first_dst_vid;
		entries[num_msgs].value = value;
		num_msgs++;
	}

	bool can_include (node_t vid) {
		if (vid - first_dst_vid < std::numeric_limits<idx_t>::max()) {
			return true;
		} else {
			return false;
		}
	}
	void clear() {
		first_dst_vid = - 1;
		num_msgs = 0;
	}
	int32_t get_num_msgs() {
		return num_msgs;
	}
	int32_t get_num_empty_slots() {
		return (max_num_entries - num_msgs);
	}
	bool is_full() {
		return (num_msgs == max_num_entries);
	}
	int32_t size_used() {
		return (sizeof(node_t) + sizeof(int32_t) + sizeof(IndexedWritebackMessageEntry<T>) * num_msgs);
	}
};



/**
 * Class definition for TG_DistributedVectorBase
 */
class TG_DistributedVectorBase {
  public:
	int max_level;  /* The maximum level of vector */
	int current_level; /* Level of current vector */
    bool no_logging; 

    /* Counters for vector write */
	std::atomic<int64_t> rand_read_recv_eom_count;
	std::atomic<int64_t> rand_read_send_eom_count;
	std::atomic<int64_t> rand_write_recv_eom_count;
	std::atomic<int64_t> rand_write_send_eom_count;

	Op operation;
	int64_t vectorID; /* Vector ID */
	std::string vector_name; /* Vector name */
	bool vector_exists; /* Whether the vector file exists in the vector folder */
	bool vector_persistent; /* Whether vector remains persistent */
    bool create_read_only; /* Whether to construct input vector for previous snapshot version */

    /* Read/Write permission for input vector and output vector.

       The permission variable for the input vector is named as SeqRDWR since the
       input vector is stored locally and thus can be read sequentially. On the
       other hand, permission for the output vector is named RandRDWR because 
       contents of output vector may be accessed via outgoing edges in 
       random manner.
     */
	RDWRMODE SeqRDWR;   
	RDWRMODE RandRDWR;

    /** File path for storing vector data */
	std::string vector_dir_path; 
	std::string iv_path;
	std::string ov_path;
	std::string ov_spilled_updates_path; /* Path to store spilled vectors */
    
    /** Output vector distributivity for insert/delete */
    bool is_ov_distributive_ins;
    bool is_ov_distributive_del;

    bool use_main_in_versioned_iv; /* */ // TODO @tslee

    bool flush_ov;  /* Whether to flush output vector */
    bool use_diff_semantics; /* Whether output vector is caching aggregator */
	
    int64_t elm_size; /* The size of vector element */
	int64_t max_entry; /* Maximum number of entries in a vertex chunk */
	int64_t alloc_num_entries; /* Number of enteries to allocate */

    turbo_tcp server_sockets; /* Server-side sockets */
    turbo_tcp client_sockets; /* Client-side sockets */

  public:
    /**
     * Set output vector's distributivity for insert and for delete
     */
    void SetOvDistributive(bool i, bool d) { is_ov_distributive_ins = i; is_ov_distributive_del = d; }
    
    /**
     * Returns if output vector is distributive of insert or delete vector,
     * based on flag i_or_d
     */
    bool IsOvDistributive(bool i_or_d) {
        if (i_or_d) {
            return is_ov_distributive_ins;
        } else {
            return is_ov_distributive_del;
        }
    }

    /**
     * Accumulate bytes of the vector that are spilled to disk
     */
	static void AccumulateSpilledDiskVectorWrites (int64_t bytes) {
		TG_DistributedVectorBase::SpilledVectorUpdatesInBytes.fetch_add(bytes);
	}

    /**
     * Return bytes of the vector that are spilled to disk
     */
	static int64_t GetSpilledDiskVectorWrites() {
		return TG_DistributedVectorBase::SpilledVectorUpdatesInBytes.load();
	}

    /**
     * Accumulate bytes of the vector that are written to disk
     */
	static void AccumulateDiskVectorWrites (int64_t bytes) {
		TG_DistributedVectorBase::VectorUpdatesInBytes.fetch_add(bytes);
	}

    /**
     * Return bytes of the vector that are written to disk
     */
	static int64_t GetDiskVectorWrites() {
		return TG_DistributedVectorBase::VectorUpdatesInBytes.load();
	}

    /**
     * Return pointer to vector given its vector ID
     */
	static TG_DistributedVectorBase* vectorID2vector(int64_t vectorID) {
		return vectorIDtable[vectorID];
	}

    /**
     * The following declarations of virtual functions are ones that are
     * implemented in class TG_DistributedVector
     */

	virtual void RespondSequentialVectorPull(int64_t chunkID, int partition_id, int lv) = 0;
	virtual void RespondOutputVectorPull(int64_t chunkID, int partition_id, int lv) = 0;
    // TODO not used
	static void callRespondOutputVectorPull(TG_DistributedVectorBase* vec, int64_t chunkID, int partition_id, int lv) {
		vec->RespondOutputVectorPull(chunkID, partition_id, lv);
	}

    virtual void RespondInputVectorPull(int64_t chunkID, int partition_id, int lv) = 0;

    // TODO not used
	static void callRespondInputVectorPull(TG_DistributedVectorBase* vec, int64_t chunkID, int partition_id, int lv) {
		vec->RespondInputVectorPull(chunkID, partition_id, lv);
	}
    virtual void SetFlagForPull (bool flag) = 0;

	virtual void RespondOutputVectorMessagePush(int64_t fromChunkID, int64_t chunkID, int partition_id, int tid, int idx, int64_t send_num, int64_t combined, int lv) = 0;
	virtual void RespondOutputVectorMessagePushForIncProcessing(int64_t fromChunkID, int64_t chunkID, int partition_id, int tid, int idx, int64_t send_num, int64_t combined, int lv) = 0;
	virtual void RespondOutputVectorMessagePushForStaticProcessing(int64_t fromChunkID, int64_t chunkID, int partition_id, int tid, int idx, int64_t send_num, int64_t combined, int lv) = 0;

    virtual bool IsReadOnlyPrevOpened() = 0;
    virtual bool CheckConstructNextSuperStepArrayAsync() = 0;
    virtual void SetConstructNextSuperStepArrayAsync(bool construct_next_async_) = 0;
    virtual void SetConstructedAlready(bool constructed_already) = 0;
    virtual void SetAdvanceSuperstep(bool advance_superstep_) = 0;
    virtual void WaitForVectorIO() = 0;
	virtual void ResetStates() = 0;
    virtual void getvectornames(std::vector<json11::Json>& vector_names) = 0;
    virtual void writeVersionedArrayIOWaitTime(JsonLogger*& json_logger) = 0;
    virtual void logVersionedArrayIOWaitTime() = 0;
    virtual void aggregateReadIO(JsonLogger*& json_logger) = 0;
    virtual void aggregateWriteIO(JsonLogger*& json_logger) = 0;
    virtual void aggregateNonOverlappedTime(JsonLogger*& json_logger) = 0;
	virtual void Close() = 0;
    virtual void OpenReadOnlyPrev() {};
	virtual void AllocateMemory() {};
	virtual void AllocateMemory(bool _create_read_only) {};
    virtual void FlushOv(int u, int s) = 0;
    virtual void CreateIvForNextUpdate(int next_u) = 0;
    virtual void CreateOvForNextUpdate(int next_u) = 0;
    virtual void CreateIvForNextSuperstep(int next_ss) = 0;
    virtual void CreateOvForNextSuperstep(int next_ss) = 0;
	virtual void CreateFiles(int ss) = 0;
    virtual void PullInputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id) = 0;
	virtual void ReadInputVectorsAtApplyPhase (int vector_chunk_idx) = 0;
	virtual void FlushInputVector(int64_t vector_chunk_idx, int64_t superstep, const std::function<bool(node_t)>& iv_delta_write_if) = 0;
    virtual void FlushInputVector(int update_version, int superstep_version) = 0;
	virtual void FlushInputVectorAndReadAndMerge(int64_t vector_chunk_idx, int64_t superstep, const std::function<bool(node_t)>& iv_delta_write_if) = 0;
	virtual void FlushSequentialVector(int64_t start_part_id, int64_t part_id) = 0;
	virtual void ReadInputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int ss) = 0;
	virtual bool InitializeOutputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id) = 0;
	virtual void ReadGGB(int u, int s) = 0;
	virtual bool FlushLocalGatherBuffer(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int64_t edges_cnt) = 0;
	virtual void FlushUpdateBuffer(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int64_t subchuk_idx, int64_t edges_cnt) = 0;
	virtual bool FlushUpdateBufferPerThread(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int64_t subchuk_idx) = 0;
	virtual void ResetWriteBuffer(node_t vid) = 0;
	virtual void ClearPulledMessagesInLocalGatherBuffer(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int64_t edges_cnt) = 0;

	virtual void ReadAndMergeSpilledWritesFromDisk(int64_t chunk_id) = 0;
	virtual void ReadSpilledWritesAndMerge(int64_t to_chunk_id, char* buf) = 0;
    
    // TODO not used
	static void callReadSpilledWritesAndMerge(TG_DistributedVectorBase* vec, int64_t to_chunk_id, char* buf) {
		vec->ReadSpilledWritesAndMerge(to_chunk_id, buf);
	}
    virtual bool CheckTargetForPull() = 0;
    virtual void PULL_StoreMessageDefault(node_t vid) = 0;
    virtual void PULL_ClearGeneratedMessages() = 0;

    virtual bool checkEqualityWithPrev(node_t vid) = 0;

    virtual void SetSkipIv(bool skip) = 0;
    virtual void MarkIvDirty(node_t vid) = 0;
    virtual void ClearIvDirty(node_t vid) = 0;
    virtual void MarkOvDirty(node_t vid) = 0;
    virtual void ClearOvDirty(node_t vid) = 0;
    virtual void ClearOvChanged(node_t vid) = 0;
    virtual void ClearAllOvDirty() = 0;
    virtual void ClearAllOvChanged() = 0;

    virtual bool HasIv() { abort(); return false; }
    virtual bool HasOv() { abort(); return false; }
    virtual bool IsOvCachingAggregator() { abort(); return false; }
    virtual TwoLevelBitMap<node_t>* GetIvDirtyBitMap() = 0;
    virtual TwoLevelBitMap<node_t>* GetIvChangedBitMap() = 0;
    virtual TwoLevelBitMap<node_t>* GetOvNextBitMap(bool t) = 0;
    virtual TwoLevelBitMap<node_t>* GetOvFirstSnapshotBitMap(bool t) = 0;

    virtual bool IsIvDirty(node_t vid) = 0;

    virtual bool IsUpdatable() = 0;
    virtual void PruneReaggregationRequired(node_t vid) = 0;
    virtual void PruneReaggregationRequired() = 0;
    virtual bool IsReaggregationRequired(node_t vid) = 0;
    virtual bool IsReapplyRequired(node_t vid) = 0;
    virtual void ClearOvFlagsForIncrementalProcessing() = 0;
    virtual void ClearIvFlagsForIncrementalProcessing(bool clear_dirty = true, bool clear_changed = true) = 0;

    virtual int64_t GetNumIvSuperstepVersions(int u) = 0;
  
    virtual bool hasIvVersion(int u, int s) = 0;

    /**
     * The following definitions are functions that are used to set and
     * get ggb-related flags
     */

    /**
     * Clear ggb_reapply_flags
     */
    static void ClearAllReapplyRequired() {
        ggb_reapply_flags.ClearAll();
    }

    /**
     * Set reapply flag for given bitmap idx
     */
    static void MarkReapplyRequiredByIdx(node_t idx) {
        ggb_reapply_flags.Set_Atomic(idx);
    }

    /**
     * Set reapply flag for given vertex id
     */
    static void MarkReapplyRequired(node_t vid) {
        ggb_reapply_flags.Set_Atomic(vid - PartitionStatistics::my_first_internal_vid());
    }

    /**
     * Clear reapply flag for given vertex id
     */
    static void ClearReapplyRequired(node_t vid) {
        ggb_reapply_flags.Clear_Atomic(vid - PartitionStatistics::my_first_internal_vid());
    }
    
    /**
     * Reaggregation required for given vertex id
     */
    static void MarkReaggregationRequired(node_t vid) {
        ggb_reaggregation_flags.Set_Atomic(vid - PartitionStatistics::my_first_internal_vid());
    }
    
    /**
     * Clear reaggregation flag for given vertex id
     */
    static void ClearReaggregationRequired(node_t vid) {
        ggb_reaggregation_flags.Clear_Atomic(vid - PartitionStatistics::my_first_internal_vid());
    }

    /**
     * Return ggb reaggregation flags bitmap
     */
    static TwoLevelBitMap<int64_t>* GetGgbReaggregationFlags() {
        return &ggb_reaggregation_flags;
    }
    
    /**
     * Return ggb message received flags bitmap
     */
    static TwoLevelBitMap<int64_t>* GetGgbMsgReceivedFlags() {
        return &ggb_msg_received_flags;
    }
    
    /**
     * Clear ggb message received flags bitmap
     */
    static void ClearOvMessageReceivedAll() {
        ggb_msg_received_flags.ClearAll(); 
    }

]   /**
     * Clear ggb message received flags for given vertex id
     */
    void ClearOvMessageReceived(node_t vid) {
        ALWAYS_ASSERT(RandRDWR == WRONLY || RandRDWR == RDWR);
        node_t idx = vid - PartitionStatistics::my_first_internal_vid();
        ggb_msg_received_flags.Clear_Atomic(idx); 
    }

    /**
     * Set ggb message received flags for given bitmap idx
     */
    void MarkOvMessageReceived(node_t idx) {
        ALWAYS_ASSERT(RandRDWR == WRONLY || RandRDWR == RDWR);
        ggb_msg_received_flags.Set_Atomic_IfNotSet(idx); 
    }

    /**
     * Get ggb message received flags for given vertex id
     */
    bool IsOvMessageReceived(node_t vid) {
        if (!(RandRDWR == WRONLY || RandRDWR == RDWR)) return false;
        node_t idx = vid - PartitionStatistics::my_first_internal_vid();
        return ggb_msg_received_flags.Get(idx); 
    }
    
    /**
     * Get total count of ggb message received flags
     */
    static int64_t GetOvMessageReceivedTotal() {
        return ggb_msg_received_flags.GetTotal(); 
    }


    virtual void SetCurrentVersion(int u, int s) = 0;
    virtual void CallConstructNextSuperStepArrayAsync(int u, int s, bool run_static_processing) = 0;

    /**
     * Compute buffer memory size required for random vector write. Note that
     * this function returns non-zero value in incremental processing mode.
     */
	static int64_t ComputeRandVectorWriteBufferMemorySize() {
        int64_t bytes = 0;
		node_t num_entries = PartitionStatistics::max_num_nodes_per_vector_chunk();
        if (UserArguments::INCREMENTAL_PROCESSING) {
            bytes += BitMap<int64_t>::compute_container_size(num_entries); // ggb_reaggregation_flags
            bytes += BitMap<int64_t>::compute_container_size(num_entries); // ggb_reapply_flags
            bytes += BitMap<int64_t>::compute_container_size(num_entries); // ggb_pull_message_store_flags
            bytes += (PartitionStatistics::num_machines() * BitMap<int64_t>::compute_container_size(num_entries)); // ggb_pull_message_store_flags_per_machine
            bytes += (2 * BitMap<int64_t>::compute_container_size(num_entries)); // lgb_dirty_flags
        }
        return bytes;
    }
    
    // XXX tslee: this function do not pin rand vector write buffer..
    /**
     * Allocate incremental processing-related output vector write buffer in memory
     */ 
	virtual void PinRandVectorWriteBufferInMemory() {
		node_t num_entries = PartitionStatistics::max_num_nodes_per_vector_chunk();
        if (UserArguments::INCREMENTAL_PROCESSING) {
            /* This function only works in incremental processing mode */
            if (ggb_reaggregation_flags.num_entries() != num_entries) {
                ggb_pull_message_store_flags_per_machine = new TwoLevelBitMap<int64_t>[PartitionStatistics::num_machines()];
                ALWAYS_ASSERT(ggb_reapply_flags.num_entries() != num_entries);
                ggb_reaggregation_flags.Init(num_entries);
                ggb_reapply_flags.Init(num_entries);
                for (int i = 0; i < PartitionStatistics::num_machines(); i++)
                    ggb_pull_message_store_flags_per_machine[i].Init(num_entries);
                ggb_pull_message_store_flags.Init(num_entries);
                for (int k = 0; k < 2; k++) {
                    lgb_dirty_flags[k].Init(num_entries);
                }
                LocalStatistics::register_mem_alloc_info("DistributedVectorBase Static BitMap", (ggb_reaggregation_flags.container_size() + ggb_reapply_flags.container_size() + ggb_pull_message_store_flags.container_size() + PartitionStatistics::num_machines() * ggb_pull_message_store_flags_per_machine[0].container_size() + 2 * lgb_dirty_flags[0].container_size()) / (1024 * 1024));
            }
        }
    }

    /**
     * The following declarations of virtual functions are ones that are
     * implemented in class TG_DistributedVector
     */

	virtual int64_t GetRandVectorWriteBufferSize() = 0;
	virtual int64_t GetSeqVectorReadBufferSize() = 0;
	virtual int64_t GetRandVectorWriteReceiveBufferSize() = 0;
	virtual int64_t GetSizeOfVW() = 0;
	virtual int64_t GetSizeOfGGB() = 0;
	virtual int64_t GetSizeOfLGB() = 0;
    virtual int64_t GetTotalDeltaSizeInDisk(Version from, Version to) = 0;
    virtual int64_t GetSizeOfVectorElement() = 0;
    virtual void* GetGGB() = 0;
    virtual void* GetOMGGB() = 0;

    virtual void ProcessPendingAioRequests() = 0;

    virtual void PrintTimers() {}
    virtual void ResetTimers() {}

    /**
     * Sort and remove duplicates of the vector write_vec_changed
     */
	static void SortAndMergeUpdatedListOfVertices() {
        ALWAYS_ASSERT (update_delta_buffer_overflow == 0 && write_vec_changed_idx >= 0);
		if (write_vec_changed_idx.load() == 0) return;
	
        /**
         * The operation below that utilizes std::sort and std::unique is used to
         * sort and eliminate duplicated elements. After the list has been updated,
         * the last valid element index of write_vec_changed will be stored back to
         * write_vec_changed_idx.
         */
        int64_t prev_j = std::atomic_load(&TG_DistributedVectorBase::write_vec_changed_idx);
        std::sort(&write_vec_changed[0], &write_vec_changed[prev_j], std::less<node_t>());
		int64_t j = (std::unique(&write_vec_changed[0], &write_vec_changed[prev_j]) - &write_vec_changed[0]);
        TG_DistributedVectorBase::write_vec_changed_idx.store(j);
	}

    // TODO not used
	int64_t GetIoBytesPerUpdate() {
		if (SeqRDWR == RDWR || SeqRDWR == WRONLY) {
			return elm_size;
		}
		return 0;
	}

    /**
     * Toggle value of the member lgb_toggle 
     */
    static void ToggleLgb() {
        //fprintf(stdout, "ToggleLgb %d -> %d\n", lgb_toggle, (lgb_toggle + 1) % 2);
        /* For the performance issue, we exploit two lgb_dirty_flags bitmap in order to
         * overlap disk I/O and query processing. When one dirty flags bitmap is being
         * used by disk I/O, another bitmap is toggled and activated by calling this
         * function and is ready to be used for next processing */
        lgb_toggle = (lgb_toggle + 1) % 2;
    }

    // TODO not used
    static void ClearLgbDirtyFlags(int toggle) {
        lgb_dirty_flags[toggle].ClearAll();
    }
    
    // TODO not used
    static void ClearPrevLgbDirtyFlags() {
        int64_t prev_toggle = (lgb_toggle + 1) % 2;
        lgb_dirty_flags[prev_toggle].ClearAll();
    }
    
    /**
     * Get LGB dirty flag for given toggle
     */
    static TwoLevelBitMap<int64_t>* GetLgbDirtyFlag(int toggle) {
        return &lgb_dirty_flags[toggle];
    }
    
    /* A mapping table from vector ID to vector pointer, used by RequestRespond */
	static std::vector<TG_DistributedVectorBase *> vectorIDtable;

    /** Records of accumulated vector updates in bytes. The variable is
     * defined as atomic integer in order to be thread safe */
	static std::atomic<int64_t> SpilledVectorUpdatesInBytes;
	static std::atomic<int64_t> VectorUpdatesInBytes;

    // TODO two below are not actually used
	static std::atomic<int64_t> NumMessagesBeforeCombiningPerThreadBuffer;
	static std::atomic<int64_t> NumMessagesAfterCombiningPerThreadBuffer;

	static std::atomic<int64_t> write_vec_changed_idx;  /* The last index of vector write_vec_changed */
	static std::atomic<int64_t> NumReceivedOV; // TODO not used
	static node_t* write_vec_changed; /* Pointer to the array of vertices that have its record changed in output vector */
	static atom lock; /* Atomic lock */
	static std::atomic<bool> update_delta_buffer_overflow; /* Indicates whether the update delta buffer has overflown */
	static node_t** ggb_pull_idx_per_machine;   /* */ // TODO resolve
	static PaddedIdx* ggb_pull_idx_per_machine_idx;  /* */ // TODO resolve
	static bool* ggb_pull_idx_overflow;  /* */ // TODO resolve
    
  public:
    /**
     * Flags related to local gather buffer and global gather buffer.
     * Note that the flags are declared as static members.
     */
    static TwoLevelBitMap<int64_t> ggb_msg_received_flags;
    static TwoLevelBitMap<int64_t> ggb_reaggregation_flags;
    static TwoLevelBitMap<int64_t> ggb_reapply_flags;
    static TwoLevelBitMap<int64_t> ggb_pull_message_store_flags;
    static TwoLevelBitMap<int64_t> lgb_dirty_flags[2]; /* The flags are accessed based on member lgb_toggle */
    static TwoLevelBitMap<int64_t>* ggb_pull_message_store_flags_per_machine;
    static int64_t         lgb_toggle; /* A toggle bit to indicate which index of lgb_dirty_flags to access */
};



#endif
