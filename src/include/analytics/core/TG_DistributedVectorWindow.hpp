#pragma once
#ifndef TG_DISTRIBUTED_VECTOR_WINDOW_H
#define TG_DISTRIBUTED_VECTOR_WINDOW_H

#include "TG_DistributedVectorBase.hpp"
#include "VersionedArray.hpp"
#include "GBVersionedArray.hpp"
#include "TurboDB.hpp"

#ifndef likely
#define likely(x) __builtin_expect((x),1)
#endif

//#define PER_THREAD_LGB

/**
 * Class definition for TG_DistributedVectorWindow. The class is a template class
 * of type T and op, where T is the type of vector element and Op is the aggregation
 * operation.
 */
template <typename T, Op op = UNDEFINED>
class TG_DistributedVectorWindow {
  public:
  /**
   * Initialize class members
   */
	TG_DistributedVectorWindow() {

        /* Identity element for given operation. Refer to Typedef.hpp
           to see supported operations */
        identity_element = idenelem<T>(op); 
        cur_iv_mmap = NULL;
        
        seq_read_vector_buff[0] = NULL;
		seq_read_vector_buff[1] = NULL;
		seq_write_vector_buff[0] = NULL;
		seq_write_vector_buff[1] = NULL;

		cur_read_random_vector_buff = NULL;
		fut_read_random_vector_buff = NULL;

		cur_local_gather_buffer_vid_indexable = NULL;
		cur_local_gather_buffer = NULL;
		prev_local_gather_buffer = NULL;
		OM_cur_local_gather_buffer = NULL;
		OM_prev_local_gather_buffer = NULL;
		cur_target_gather_buffer_vid_indexable = NULL;
		OM_cur_target_gather_buffer_vid_indexable = NULL;

		write_rand_req_buf_queue = NULL;
		vector = NULL;
		cur_dst_vector_part_writeback_tasks[0] = NULL;
		cur_dst_vector_part_writeback_tasks[1] = NULL;

		per_thread_write_vector[0] = NULL;
		per_thread_write_vector[1] = NULL;
		per_thread_idx[0] = NULL;
		per_thread_idx[1] = NULL;
		per_thread_cnt[0] = NULL;
		per_thread_cnt[1] = NULL;
		per_thread_cnt[2] = NULL;
		per_thread_cnt[3] = NULL;
		per_thread_cnt[4] = NULL;
	}
	
    /**
     * Create input vector for next update of the window (version index next_u).
     * Called from TG_DistributedVector::CreateIvForNextUpdate()
     */
    virtual void CreateIvForNextUpdate(int next_u) {
	    if (!UserArguments::INCREMENTAL_PROCESSING) return;
        if (!(SeqRDWR == RDONLY || SeqRDWR == RDWR)) return;
        if (my_level != 0) return;

        /* Finish remaining async job and update version */
        versioned_iv_array.FinishAsyncJob();
        versioned_iv_array.AdvanceUpdateVersion(next_u);
    }

    /**
     * Create input vector for next superstep next_ss. Called from
     * TG_DistributedVector::CreatedIvForNextSuperstep()
     */
    virtual void CreateIvForNextSuperstep(int next_ss) {
        if (!(SeqRDWR == RDONLY || SeqRDWR == RDWR)) return;
        if (my_level != 0) return;
	    if (!UserArguments::INCREMENTAL_PROCESSING) return;

        if (versioned_iv_array.GetNumberOfSuperstepVersions(UserArguments::UPDATE_VERSION) <= next_ss) {
            /* Update version of versioned_iv_array */
            versioned_iv_array.AdvanceSuperstepVersion(UserArguments::UPDATE_VERSION);
#ifdef GB_VERSIONED_ARRAY
            //fprintf(stdout, "%s set seq_read_vector_buff[0] to data() %p, seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), versioned_iv_array.data(), versioned_iv_array.next_data());
            seq_read_vector_buff[0] = versioned_iv_array.data();
            seq_read_vector_buff[1] = versioned_iv_array.next_data();
            //fprintf(stdout, "%s set prev_seq_read_vector_buff[0] to data() %p, prev_seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), versioned_iv_array_prev.data(), versioned_iv_array_prev.next_data());
            prev_seq_read_vector_buff[0] = versioned_iv_array_prev.data();
            prev_seq_read_vector_buff[1] = versioned_iv_array_prev.next_data();
#endif
        }
    }

    /**
     * Set input vector to advance superstep
     */
    void SetAdvanceSuperstep(bool advance_superstep_) {
        versioned_iv_array.SetAdvanceSuperstep(advance_superstep_);
    }
    
    /**
     * Set input vector to construct next super array asynchronously
     */
    void SetConstructNextSuperStepArrayAsync(bool construct_next_ss_array_async_) {
        versioned_iv_array.SetConstructNextSuperStepArrayAsync(construct_next_ss_array_async_);
    }
    
    /**
     * Set whether the version is constructed already
     */
    void SetConstructedAlready(bool constructed_already) {
        versioned_iv_array.SetConstructedAlready(constructed_already);
    }
    
    /**
     * Flush input vector and read and merge
     */
    virtual void FlushInputVectorAndReadAndMerge(int64_t vector_chunk_idx, int64_t superstep, const std::function<bool(node_t)>& iv_delta_write_if) {
        if (SeqRDWR == WRONLY || SeqRDWR == NOUSE) return;

        /* Construct index range */
        Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + vector_chunk_idx);
        node_t my_first_vid = PartitionStatistics::my_first_internal_vid();
        Range<int64_t> idx_range(vid_range.GetBegin() - my_first_vid, vid_range.GetEnd() - my_first_vid);

        /* Whether to construct for previous version */
        bool construct_prev = create_read_only && (UserArguments::UPDATE_VERSION >= 1);
        versioned_iv_array.CopyFlagsBeforeFlush();
        /* Construct and flush for versioned_iv_array */
        versioned_iv_array.ConstructAndFlush(idx_range, UserArguments::UPDATE_VERSION, superstep, construct_prev, &versioned_iv_array_prev, true);
#ifdef GB_VERSIONED_ARRAY
        seq_read_vector_buff[0] = versioned_iv_array.data();
        prev_seq_read_vector_buff[0] = versioned_iv_array_prev.data();
#endif
    }

    /**
     * Flush input vector for (update, superstep)
     */
    virtual void FlushInputVector(int64_t vector_chunk_idx, int64_t superstep, const std::function<bool(node_t)>& iv_delta_write_if) {
        if (SeqRDWR == WRONLY || SeqRDWR == NOUSE) return;
        
        Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + vector_chunk_idx);
        node_t my_first_vid = PartitionStatistics::my_first_internal_vid();
        Range<int64_t> idx_range(vid_range.GetBegin() - my_first_vid, vid_range.GetEnd() - my_first_vid);

        // TODO vid_range above not used
		versioned_iv_array.FlushAll(UserArguments::UPDATE_VERSION, superstep);
	}
    
    /**
     * Flush input vector for (update, superstep)
     */
    virtual void FlushInputVector(int update_version, int superstep_version) {
		versioned_iv_array.FlushAll(update_version, superstep_version);
    }
    
    /**
     * Read input vectors at apply phase for given vector chunk
     */
    virtual void ReadInputVectorsAtApplyPhase (int vector_chunk_idx) {
        /* Construct index range */
        Range<node_t> vid_range = PartitionStatistics::per_edge_partition_vid_range(PartitionStatistics::my_machine_id() * UserArguments::VECTOR_PARTITIONS + vector_chunk_idx);
        node_t my_first_vid = PartitionStatistics::my_first_internal_vid();
        Range<int64_t> idx_range(vid_range.GetBegin() - my_first_vid, vid_range.GetEnd() - my_first_vid);

        bool construct_prev = create_read_only && (UserArguments::UPDATE_VERSION >= 1);
        /* Construct and flush */
        versioned_iv_array.ConstructAndFlush(Range<int64_t>(0, PartitionStatistics::my_num_internal_nodes() - 1), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, construct_prev, &versioned_iv_array_prev);
#ifdef GB_VERSIONED_ARRAY
        //fprintf(stdout, "%s set seq_read_vector_buff[0] to data() %p, seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), versioned_iv_array.data(), versioned_iv_array.next_data());
        //fprintf(stdout, "%s set prev_seq_read_vector_buff[0] to data() %p, prev_seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), versioned_iv_array_prev.data(), versioned_iv_array_prev.next_data());
        seq_read_vector_buff[0] = versioned_iv_array.data();
        seq_read_vector_buff[1] = versioned_iv_array.next_data();
        prev_seq_read_vector_buff[0] = versioned_iv_array_prev.data();
        prev_seq_read_vector_buff[1] = versioned_iv_array_prev.next_data();
#endif
    }

    /**
     * Create vector file for each window.
     */
    virtual void CreateFiles(int ss) {
        if (!(SeqRDWR == RDONLY || SeqRDWR == RDWR)) return;
        ALWAYS_ASSERT (my_level == 0);
        
        std::string iv_ss_path = vector->iv_path + "_superstep" + std::to_string(ss);
        /* Create versioned_iv_array */
        versioned_iv_array.Create();
        if (create_read_only && !versioned_iv_array_prev.IsCreated()) {
            versioned_iv_array_prev.CreateReadOnly();
        }
    }
    
    /**
     * Allocate memory of the current window. Called from AllocateMemory() of
     * its vector.
     */
	void AllocateMemory(bool _create_read_only) {
		cur_dst_vector_part_writeback_tasks[0] = new Range<node_t>[PartitionStatistics::num_subchunks_per_edge_chunk()];
		cur_dst_vector_part_writeback_tasks[1] = new Range<node_t>[PartitionStatistics::num_subchunks_per_edge_chunk()];
		for (int64_t i = 0; i < PartitionStatistics::num_subchunks_per_edge_chunk(); i++) {
			cur_dst_vector_part_writeback_tasks[0][i].Set (-1, -2);
			cur_dst_vector_part_writeback_tasks[1][i].Set (-1, -2);
		}
		write_rand_req_buf_queue = new tbb::concurrent_queue<SimpleContainer>[PartitionStatistics::num_machines()];
		rand_write_combined = 1; // XXX - syko

		// Max # of entries in a Vchunk
		max_entry = (PartitionStatistics::max_internal_nodes() + UserArguments::VECTOR_PARTITIONS - 1) / UserArguments::VECTOR_PARTITIONS + UserArguments::VECTOR_PARTITIONS;
		alloc_num_entries = max_entry;

        // Create files for the superstep
        create_read_only = _create_read_only;
        CreateFiles(UserArguments::SUPERSTEP);

		// Input vector window. The input vector window must be set readable.
		cur_src_vector_vid_range = PartitionStatistics::per_machine_vid_range(PartitionStatistics::my_machine_id());
		if (SeqRDWR == RDONLY || SeqRDWR == RDWR) {
			if (my_level == 0) {
                seq_read_vector_buff[0] = versioned_iv_array.data();
                seq_read_vector_buff[1] = seq_read_vector_buff[0];
                //fprintf(stdout, "%s set seq_read_vector_buff[0] to data() %p, seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), seq_read_vector_buff[0], seq_read_vector_buff[1]);
                INVARIANT (seq_read_vector_buff[0] != NULL);
				INVARIANT (seq_read_vector_buff[1] != NULL);
                prev_seq_read_vector_buff[0] = versioned_iv_array_prev.data();
                prev_seq_read_vector_buff[1] = prev_seq_read_vector_buff[0];
                if (maintain_prev_ss) {
                    LOG_ASSERT(false);
                    prev_ss_seq_read_vector_buff[0] = versioned_iv_array.prev_ss_data();
                    prev_ss_seq_read_vector_buff[1] = prev_ss_seq_read_vector_buff[0];
                    prev_sssn_seq_read_vector_buff[0] = versioned_iv_array_prev.prev_ss_data();
                    prev_sssn_seq_read_vector_buff[1] = prev_sssn_seq_read_vector_buff[0];
                }
                // Allocate pull vector for MIN/MAX op
                if (op == MIN || op == MAX) {
                    pull_send_buff = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * max_entry);
                    pull_receive_buff[0] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * max_entry);
                    pull_receive_buff[1] = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * max_entry);
#pragma omp parallel for
                    for(int64_t i = 0; i < max_entry; i++) pull_send_buff[i] = idenelem<T>(op);
#pragma omp parallel for
                    for(int64_t i = 0; i < max_entry; i++) pull_receive_buff[0][i] = idenelem<T>(op);
#pragma omp parallel for
                    for(int64_t i = 0; i < max_entry; i++) pull_receive_buff[1][i] = idenelem<T>(op);
                    LocalStatistics::register_mem_alloc_info((vector->vector_name + " Buffer for Pull").c_str(), (3 * sizeof(T) * max_entry) / (1024 * 1024));
                }
			} else {
                LOG_ASSERT(false); // by syko @ 19.03.04
			}
		}

		// Output Vector Window
		// Double Buffering for Pull
        // XXX tslee disabled this 2020.06.06
		/*if(RandRDWR == RDONLY || RandRDWR == RDWR) {
			cur_read_random_vector_buff = new T[max_entry];
			fut_read_random_vector_buff = new T[max_entry];
			#pragma omp parallel for
			for(int64_t i = 0 ; i < max_entry ; i++) {
				cur_read_random_vector_buff[i] = fut_read_random_vector_buff[i] = idenelem<T>(op);
			}
			LocalStatistics::register_mem_alloc_info((vector->vector_name + " Double Buffering for Pull").c_str(), (2 * sizeof(T) * max_entry) / (1024 * 1024));
		}*/

		// Double Buffering for push when using PER_THREAD_LGB
#ifdef PER_THREAD_LGB
        //per_thread_lgb_num_nodes = ((int64_t) PartitionStatistics::my_num_internal_nodes());
        per_thread_lgb_num_nodes = ((int64_t) PartitionStatistics::my_num_internal_nodes() / 100) + 1;
        //per_thread_lgb_num_nodes = (int64_t) PartitionStatistics::my_num_internal_nodes() / 10000;
        per_thread_cur_local_gather_buffer = new T*[UserArguments::NUM_THREADS];
        //per_thread_OM_cur_local_gather_buffer = new T*[UserArguments::NUM_THREADS];
        per_thread_cur_local_gather_buffer_vid_indexable = new T*[UserArguments::NUM_THREADS];
        //per_thread_OM_cur_local_gather_buffer_vid_indexable = new T*[UserArguments::NUM_THREADS];
#else
        per_thread_lgb_num_nodes = -1;
#endif
        // Allocate local gather buffer. The lgb must be set to writable.
		if(RandRDWR == WRONLY || RandRDWR == RDWR) {
/*
			cur_local_gather_buffer = (T*) NumaHelper::alloc_numa_memory_two_part(sizeof(T) * alloc_num_entries);
			prev_local_gather_buffer = (T*) NumaHelper::alloc_numa_memory_two_part(sizeof(T) * alloc_num_entries);
            OM_cur_local_gather_buffer = (T*) NumaHelper::alloc_numa_memory_two_part(sizeof(T) * alloc_num_entries);
			OM_prev_local_gather_buffer = (T*) NumaHelper::alloc_numa_memory_two_part(sizeof(T) * alloc_num_entries);
*/
			cur_local_gather_buffer = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * alloc_num_entries);
			prev_local_gather_buffer = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * alloc_num_entries);
            OM_cur_local_gather_buffer = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * alloc_num_entries);
			OM_prev_local_gather_buffer = (T*) NumaHelper::alloc_numa_interleaved_memory(sizeof(T) * alloc_num_entries);

            // Allocate and initialize per-thread local gather buffer
#ifdef PER_THREAD_LGB
			#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
			{
				int i = omp_get_thread_num();
                per_thread_cur_local_gather_buffer[i] = (T*) NumaHelper::alloc_numa_local_memory(sizeof(T) * per_thread_lgb_num_nodes);
                //per_thread_OM_cur_local_gather_buffer[i] = (T*) NumaHelper::alloc_numa_local_memory(sizeof(T) * per_thread_lgb_num_nodes);
                for (int64_t idx = 0; idx < per_thread_lgb_num_nodes; idx++)
                    per_thread_cur_local_gather_buffer[i][idx] = idenelem<T>(op);
                //memset(per_thread_cur_local_gather_buffer[i], 0, sizeof(T) * per_thread_lgb_num_nodes);
                //memset(per_thread_OM_cur_local_gather_buffer[i], 0, sizeof(T) * per_thread_lgb_num_nodes);
                //memset(per_thread_cur_local_gather_buffer[i], idenelem<T>(op), sizeof(T) * per_thread_lgb_num_nodes);
                //memset(per_thread_OM_cur_local_gather_buffer[i], idenelem<T>(op), sizeof(T) * per_thread_lgb_num_nodes);
			}
#endif

            // Initialize local gather buffer
			#pragma omp parallel for
			for(int64_t i = 0 ; i < max_entry ; i++) {
				cur_local_gather_buffer[i] = idenelem<T>(op);
				prev_local_gather_buffer[i] = idenelem<T>(op);
				OM_cur_local_gather_buffer[i] = idenelem<T>(op);
				OM_prev_local_gather_buffer[i] = idenelem<T>(op);
			}
			InitializePerThreadVectorUpdateBuffer();
			
            LocalStatistics::register_mem_alloc_info((vector->vector_name + " LGB Double Buffering for Push").c_str(), (4 * sizeof(T) * max_entry) / (1024 * 1024));
#ifdef PER_THREAD_LGB
            LocalStatistics::register_mem_alloc_info((vector->vector_name + " LGB Double Buffering for Push (Per Thread LGB)").c_str(), (2 * sizeof(T) * per_thread_lgb_num_nodes * UserArguments::NUM_THREADS) / (1024 * 1024));
#endif
		}
	}
	
    /**
     * Returns whether input vector has element for version u
     * and superstep s. Called from TG_DistributedVector::hasIvVersion()
     */
    virtual bool hasIvVersion(int u, int s) {
        if (SeqRDWR == NOUSE) return false;
        return versioned_iv_array.hasVersion(u, s);
    }

    /**
     * Opens and initializes window. Called from TG_DistributedVector::OpenWindow()
     */
	void Open(RDWRMODE Seqmode, RDWRMODE Randmode, TG_DistributedVectorBase* vec, int lv, bool maintain_prev_ss_, bool advance_superstep_, bool construct_next_ss_array_async_) {
		
        /* Records information of the window */
        SeqRDWR = Seqmode;
		RandRDWR = Randmode;
		vector = vec;
		my_level = lv;
		vectorID = vec->vectorID;
        maintain_prev_ss = maintain_prev_ss_;

		cur_read_random_vector_buff = NULL;
		fut_read_random_vector_buff = NULL;
		cur_local_gather_buffer = NULL;
		prev_local_gather_buffer = NULL;
		OM_cur_local_gather_buffer = NULL;
		OM_prev_local_gather_buffer = NULL;

		seq_cur = 0;
		cur = 0;
        
          printProcessMemoryUsage("Before Init VA");
        /* Initialize versioned input vector array for the window */
        versioned_iv_array.Init(vector->iv_path, PartitionStatistics::my_num_internal_nodes(), vector->vector_exists, vector->use_main_in_versioned_iv, false, maintain_prev_ss, advance_superstep_, construct_next_ss_array_async_);
        if (vector->create_read_only && !versioned_iv_array_prev.IsCreated()) {
            /* If previous versioned array is not created, create in read-only mode */
            versioned_iv_array_prev.InitReadOnly(versioned_iv_array, maintain_prev_ss);
            versioned_iv_array.SetPrev(versioned_iv_array_prev);
        }
          printProcessMemoryUsage("After Init VA");
    }

    /**
     * Initialize per thread vector update buffer
     */
	void InitializePerThreadVectorUpdateBuffer() {
		per_thread_write_vector_capacity = PER_THREAD_BUFF_SIZE / sizeof(WritebackMessage<T>);
        // TODO why 5?
        for (int k = 0; k < 5; k++) {
            int result = posix_memalign( (void**) &per_thread_cnt[k], 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
            for (int i = 0; i < UserArguments::NUM_THREADS; i++) per_thread_cnt[k][i].idx = 0;
        }
        /* Instantiate and allocate for per_thread_write_vector for each level(k) and thread_id(i) */
		for (int k = 0; k < 2; k++) {
			per_thread_write_vector[k] = new WritebackMessage<T>*[UserArguments::NUM_THREADS];
			int result = posix_memalign( (void**) &per_thread_idx[k], 64, sizeof(PaddedIdx) * UserArguments::NUM_THREADS);
			#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
			{
				int i = omp_get_thread_num();
				per_thread_write_vector[k][i] = (WritebackMessage<T>*) NumaHelper::alloc_numa_local_memory(sizeof(WritebackMessage<T>) * (PER_THREAD_BUFF_SIZE / sizeof(WritebackMessage<T>)));
				ALWAYS_ASSERT (per_thread_write_vector[k][i] != NULL);
				memset(per_thread_write_vector[k][i], -1, sizeof(WritebackMessage<T>) * (PER_THREAD_BUFF_SIZE / sizeof(WritebackMessage<T>)));
				per_thread_idx[k][i].idx = 0;
			}
		}
	}

    /**
     * Free inputvector read buffers
     */
	void ReleaseInputVectorReadBuffers() {
	    ALWAYS_ASSERT (my_level == 0);
		if (!(SeqRDWR == RDONLY || SeqRDWR == RDWR)) return;
        /* Close versioned_iv_array_prev */
        if (create_read_only) versioned_iv_array_prev.Close(false);
        /* Close versioned_iv_array */
        versioned_iv_array.Close(!vector->vector_persistent);
	}

    /**
     * Free outputvector write buffers
     */
	void ReleaseOutputVectorWriteBuffers() {
		if (cur_local_gather_buffer == NULL || prev_local_gather_buffer == NULL) return;
		if(!(RandRDWR == WRONLY || RandRDWR == RDWR)) return;

		ALWAYS_ASSERT(cur_local_gather_buffer != NULL);
		ALWAYS_ASSERT(prev_local_gather_buffer != NULL);
		ALWAYS_ASSERT(OM_cur_local_gather_buffer != NULL);
		ALWAYS_ASSERT(OM_prev_local_gather_buffer != NULL);
		
        /* Free all buffer memories */

        NumaHelper::free_numa_local_memory(cur_local_gather_buffer, sizeof(T) * alloc_num_entries);
		NumaHelper::free_numa_memory(prev_local_gather_buffer, sizeof(T) * alloc_num_entries);
        NumaHelper::free_numa_memory(OM_cur_local_gather_buffer, sizeof(T) * alloc_num_entries);
		NumaHelper::free_numa_memory(OM_prev_local_gather_buffer, sizeof(T) * alloc_num_entries);
#ifdef PER_THREAD_LGB
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
        {
            int i = omp_get_thread_num();
            NumaHelper::free_numa_memory(per_thread_cur_local_gather_buffer[i], sizeof(T) * per_thread_lgb_num_nodes);
            //NumaHelper::free_numa_memory(per_thread_OM_cur_local_gather_buffer[i], sizeof(T) * per_thread_lgb_num_nodes);
        }
        delete[] per_thread_cur_local_gather_buffer;
        //delete[] per_thread_OM_cur_local_gather_buffer;
        delete[] per_thread_cur_local_gather_buffer_vid_indexable;
        //delete[] per_thread_OM_cur_local_gather_buffer_vid_indexable;
#endif
		
        cur_local_gather_buffer = NULL;
		prev_local_gather_buffer = NULL;
		OM_cur_local_gather_buffer = NULL;
		OM_prev_local_gather_buffer = NULL;
		
        LocalStatistics::register_mem_alloc_info((vector->vector_name + " Double Buffer for Push").c_str(), 0);
        LocalStatistics::register_mem_alloc_info((vector->vector_name + " Double Buffering for Push (Per Thread LGB)").c_str(), 0);
	}

    /**
     * Free outputvector read buffers
     */
	void ReleaseOutputVectorReadBuffers() {
        // XXX tslee disabled this 2020.06.06
        // TODO why?
        return;
		if(!(RandRDWR == RDONLY || RandRDWR == RDWR)) return;

		ALWAYS_ASSERT(cur_read_random_vector_buff != NULL);
		ALWAYS_ASSERT(fut_read_random_vector_buff != NULL);
		delete[] cur_read_random_vector_buff;
		delete[] fut_read_random_vector_buff;
		LocalStatistics::register_mem_alloc_info((vector->vector_name + " Double Buffer for Pull").c_str(), 0);
	}

    /**
     * Close window
     */
	void Close() {

        /* Free all memory regions */
		delete[] cur_dst_vector_part_writeback_tasks[0];
		delete[] cur_dst_vector_part_writeback_tasks[1];
		delete[] write_rand_req_buf_queue;

		ReleaseInputVectorReadBuffers();
		ReleaseOutputVectorReadBuffers(); // TODO not doing anything
		ReleaseOutputVectorWriteBuffers();
	}

    /**
     * Read input vector for given partition
     */
	void ReadInputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int ss) {
		ALWAYS_ASSERT (part_id >= 0 && part_id < PartitionStatistics::num_target_vector_chunks());
		ALWAYS_ASSERT (my_level == 0);  // syko, 2019/11/05
		
        cur_src_vector_part = part_id;
		cur_src_vector_part_machine = part_id / UserArguments::VECTOR_PARTITIONS;
		cur_src_vector_part_chunk = part_id % UserArguments::VECTOR_PARTITIONS;
		cur_src_vector_vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(cur_src_vector_part_machine, cur_src_vector_part_chunk);

		if(SeqRDWR == NOUSE || SeqRDWR == WRONLY) return;

        /* Construct versioned array */
        bool construct_prev = create_read_only && (UserArguments::UPDATE_VERSION >= 1);
        versioned_iv_array.ConstructAndFlush(Range<int64_t>(0, PartitionStatistics::my_num_internal_nodes() - 1), UserArguments::UPDATE_VERSION, ss, construct_prev, &versioned_iv_array_prev);
#ifdef GB_VERSIONED_ARRAY
        //fprintf(stdout, "%s set seq_read_vector_buff[0] to data() %p, seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), versioned_iv_array.data(), versioned_iv_array.next_data());
        //fprintf(stdout, "%s set prev_seq_read_vector_buff[0] to data() %p, prev_seq_read_vector_buff[1] to next_data() %p\n", vector->vector_name.c_str(), versioned_iv_array_prev.data(), versioned_iv_array_prev.next_data());
        seq_read_vector_buff[0] = versioned_iv_array.data();
        seq_read_vector_buff[1] = versioned_iv_array.next_data();
        prev_seq_read_vector_buff[0] = versioned_iv_array_prev.data();
        prev_seq_read_vector_buff[1] = versioned_iv_array_prev.next_data();
#endif
    }

    // TODO not used
    void _InitializeInputVectorPull(int64_t start_part_id, int64_t part_id, int64_t next_part_id) {}

	void _InitializeOutputVectorPush(int64_t start_part_id, int64_t part_id, int64_t next_part_id);

    void InitializeOutputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id); 

	/**
     * Pull input vector for given partition range
     */
    void PullInputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id) {
		ALWAYS_ASSERT (UserArguments::USE_PULL);
        if (!vector->CheckTargetForPull()) return;

        //fprintf(stdout, "[%ld] (%ld,%ld) [PullInputVector] %s %ld %ld %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, vector->vector_name.c_str(), start_part_id, part_id, next_part_id);

        int machine_id = part_id / UserArguments::VECTOR_PARTITIONS;
		int64_t chunk_id = part_id % UserArguments::VECTOR_PARTITIONS;
		int next_machine_id = next_part_id / UserArguments::VECTOR_PARTITIONS;
		int64_t next_chunk_id = next_part_id % UserArguments::VECTOR_PARTITIONS;
		int64_t idx_to_wait = cur;
		int64_t idx_to_put_future = (cur + 1) % 2;

#ifndef PERFORMANCE
        ALWAYS_ASSERT(pull_receive_buff != NULL);
        ALWAYS_ASSERT(pull_receive_buff[0] != NULL);
        ALWAYS_ASSERT(pull_receive_buff[1] != NULL);
        if (start_part_id == part_id) {
#pragma omp parallel for
            for(int64_t i = 0 ; i < max_entry ; i++) {
                ALWAYS_ASSERT (pull_receive_buff[0][i] == idenelem<T>(op));
                ALWAYS_ASSERT (pull_receive_buff[1][i] == idenelem<T>(op));
            }
        }
#endif

        /** Send request input vector pull for initial partition */
        if (start_part_id == part_id) {
            SendRequestInputVectorPull(machine_id, chunk_id);
            ReceiveInputVectorPull(machine_id, chunk_id, UserArguments::CURRENT_LEVEL);
        } else {
            ALWAYS_ASSERT(read_rand_thread.valid());
            read_rand_thread.get();
        }
        
        pull_vector_buffer_vid_indexable = pull_receive_buff[cur] - cur_dst_vector_first_vid;
		cur = (cur + 1 ) % 2;
		//std::swap(OM_cur_local_gather_buffer, OM_prev_local_gather_buffer);
        //OM_cur_local_gather_buffer_vid_indexable = OM_cur_local_gather_buffer - cur_dst_vector_first_vid;

        //  if Not END, Request the next one
        if (next_part_id != -1) {
            SendRequestInputVectorPull(next_machine_id, next_chunk_id);
            read_rand_thread = Aio_Helper::EnqueueAsynchronousTask(TG_DistributedVectorWindow::callReceiveInputVectorPull, this, next_machine_id, next_chunk_id, UserArguments::CURRENT_LEVEL);
        }
    }
    
    /**
     * Clear pulled message in LGB
     */
    void ClearPulledMessagesInLocalGatherBuffer(int64_t start_part_id, int64_t part_id, int64_t next_dst_part_id, int64_t edges_cnt) {
		if (!vector->CheckTargetForPull()) return;
        if (!UserArguments::USE_PULL) return;

        /* For nodes marked dirty in lgb, set its pull_receive_buff to identity element */
        int64_t prev_cur = (cur + 1) % 2;
        TG_DistributedVectorBase::lgb_dirty_flags[prev_cur].TwoLevelBitMap<int64_t>::InvokeIfMarked([&](node_t idx) {
            pull_receive_buff[prev_cur][idx] = identity_element;
        });
#ifdef PERFORMANCE
        //fprintf(stdout, "ClearAll LGB[%d]\n", prev_cur);
        TG_DistributedVectorBase::lgb_dirty_flags[prev_cur].ClearAll();    // XXX - Assuming that we have only one vector to pull
#endif
    }

    /**
     * Flush LGB
     */
	bool FlushLocalGatherBuffer(int64_t start_part_id, int64_t part_id, int64_t next_dst_part_id, int64_t edges_cnt) {
		if (RandRDWR == NOUSE || RandRDWR == RDONLY) return false;
        if (UserArguments::USE_PULL) return false;

		int64_t cur_src_chunk_id = cur_src_vector_part_chunk;
		int machine_id = part_id / UserArguments::VECTOR_PARTITIONS;
		int64_t chunk_id = part_id % UserArguments::VECTOR_PARTITIONS;
		int next_machine_id = next_dst_part_id / UserArguments::VECTOR_PARTITIONS;
		int64_t next_chunk_id = next_dst_part_id % UserArguments::VECTOR_PARTITIONS;
		int64_t combined = rand_write_combined;

		int64_t idx_to_put_future = (cur + 1) % 2;
		cur = (cur + 1 ) % 2;
        //fprintf(stdout, "(%d, %d) Swap cur_lgb with prev_lgb\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP);
		/* Swap buffers of current version and previous version */
        std::swap(cur_local_gather_buffer, prev_local_gather_buffer);
		std::swap(OM_cur_local_gather_buffer, OM_prev_local_gather_buffer);

        if(next_dst_part_id != start_part_id && PartitionStatistics::num_machines() > 1) {
            // Spawn async task if the chunk is not last chunk
            write_rand_thread[idx_to_put_future]  = Aio_Helper::EnqueueAsynchronousTask(TG_DistributedVectorWindow::callRequestOutputVectorMessagePush, this, prev_local_gather_buffer, OM_prev_local_gather_buffer,  machine_id, cur_src_chunk_id, chunk_id, combined, edges_cnt, cur_rand_write_idx);
        } else {
            // For the last chunk
            RequestOutputVectorMessagePush(prev_local_gather_buffer, OM_prev_local_gather_buffer, machine_id, cur_src_chunk_id, chunk_id, combined, edges_cnt, cur_rand_write_idx);
        }
        return true;
    }

    /**
     * Flush update buffer. Returns whether overflow occured
     */
	bool FlushUpdateBufferPerThread(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int64_t subchuk_idx) {
		/* Returns true if overflow has occured. Otherwise return false */
        if (RandRDWR == NOUSE || RandRDWR == RDONLY) return false;
        if (UserArguments::USE_PULL) return false;
		
        return FlushUpdateBufferPerThread(TG_ThreadContexts::thread_id, cur_rand_write_idx, cur_dst_vector_vid_range);
	}
    
    /**
     * Flush update buffer per thread. Returns whether overflow occured
     */
	bool FlushUpdateBufferPerThread (int tid, int64_t rand_write_idx, Range<node_t> send_range) {
        //fprintf(stdout, "%s FlushUpdateBufferPerThread tid %d, rand_write_idx %ld, send_range [%ld, %ld], per_thread_idx = %ld, per_thread_write_vector_capacity = %ld, lgb total = %ld\n", vector->vector_name.c_str(), tid, rand_write_idx, send_range.GetBegin(), send_range.GetEnd(), (int64_t) per_thread_idx[rand_write_idx][tid].idx, per_thread_write_vector_capacity, cur_target_flag->GetTotal());
        ALWAYS_ASSERT(!UserArguments::USE_PULL);
		if (per_thread_idx[rand_write_idx][tid].idx == 0) return false;
		
		tbb::concurrent_queue<SimpleContainer>* req_buf_queue = &write_rand_req_buf_queue[cur_dst_vector_part_machine];
        bool overflow = (per_thread_idx[rand_write_idx][tid].idx == per_thread_write_vector_capacity);
#ifdef TEMP_SINGLE_MACHINE
		if (overflow || (PartitionStatistics::num_machines() == 1)) {
#else
		if (overflow) {
		//if (true) {
#endif
            // Flush per-thread buffer
            node_t dst_vid, loc;
            T value, old_value;
			for (int j = 0; j < per_thread_idx[rand_write_idx][tid].idx; j++) {
				dst_vid = per_thread_write_vector[rand_write_idx][tid][j].dst_vid;
                loc = dst_vid - cur_dst_vector_first_vid;
				value = per_thread_write_vector[rand_write_idx][tid][j].message;
				//AtomicOperation<T, op>(&cur_target_gather_buffer_vid_indexable[dst_vid], value);
#ifdef OldMessageTransfer
			    old_value = per_thread_write_vector[rand_write_idx][tid][j].old_message;
				//AtomicOperation<T, op>(&OM_cur_target_gather_buffer_vid_indexable[dst_vid], old_value);
#endif
                //_AccumulateNewAndOldMessagesIntoTGB(loc, value, old_value);
                AtomicAccumulateNewAndOldMessagesIntoTGB<T, op>(&cur_target_gather_buffer[loc], value, old_value);
                //fprintf(stdout, "\tFlushUpdateBuffer.. tid %ld, vid %ld, (%s, %s)\n", tid, dst_vid, std::to_string(value).c_str(), std::to_string(old_value).c_str());
                cur_target_flag->Set_Atomic_IfNotSet(loc);
            }
#ifdef PER_THREAD_LGB
            if (overflow) {
                // Flush per-thread lgb
                Range<int64_t> idx_range (0, per_thread_lgb_num_nodes - 1);
                cur_target_flag->InvokeIfMarked([&](node_t idx) {
                    value = per_thread_cur_local_gather_buffer[tid][idx];
                    if (value == identity_element) return;
                    per_thread_cur_local_gather_buffer[tid][idx] = identity_element;
                    //_AccumulateNewAndOldMessagesIntoTGB(idx, value);
                    AtomicAccumulateNewAndOldMessagesIntoTGB<T, op>(&cur_target_gather_buffer[idx], value);
                }, idx_range, true);
                /*for (int j = 0; j < per_thread_lgb_num_nodes; j++) {
                    //XXX if iden value do not mv
                    value = per_thread_cur_local_gather_buffer[tid][j];
                    if (value == idenelem<T>(op)) continue;
                    //AtomicOperation<T, op>(&cur_target_gather_buffer[j], per_thread_cur_local_gather_buffer[tid][j]);
                    per_thread_cur_local_gather_buffer[tid][j] = idenelem<T>(op);
#ifdef OldMessageTransfer
                    //old_value = per_thread_OM_cur_local_gather_buffer[tid][j];
                    //AtomicOperation<T, op>(&OM_cur_target_gather_buffer[j], per_thread_OM_cur_local_gather_buffer[tid][j]);
                    //per_thread_OM_cur_local_gather_buffer[tid][j] = idenelem<T>(op);
#endif
                    _AccumulateNewAndOldMessagesIntoTGB(j, value);
                }*/
            }
#endif
#ifdef TEMP_SINGLE_MACHINE
		} else if (PartitionStatistics::num_machines() != 1) {
#else
		} else {
#endif
			SimpleContainer cont = RequestRespond::GetDataSendBuffer(DEFAULT_NIO_BUFFER_SIZE);
#ifdef OldMessageTransfer
			WritebackMessageWOOldMessage<T>* msg_buffer = (WritebackMessageWOOldMessage<T>*) cont.data;
			int64_t msg_buffer_capacity = cont.capacity / sizeof(WritebackMessageWOOldMessage<T>);
#else
			WritebackMessage<T>* msg_buffer = (WritebackMessage<T>*) cont.data;
			int64_t msg_buffer_capacity = cont.capacity / sizeof(WritebackMessage<T>);
#endif
			int64_t loc = 0;
            T msg;

			for (int j = 0; j < per_thread_idx[rand_write_idx][tid].idx; j++) {
				msg_buffer[loc].dst_vid = per_thread_write_vector[rand_write_idx][tid][j].dst_vid;
                msg_buffer[loc].message = identity_element;
                UpdateAggregation<T, op>(&msg_buffer[loc].message, per_thread_write_vector[rand_write_idx][tid][j].old_message, per_thread_write_vector[rand_write_idx][tid][j].message);
                //fprintf(stdout, "(%d, %d) %s LGB Flush dst_vid %ld, message = %ld\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, vector->vector_name.c_str(), msg_buffer[loc].dst_vid, msg_buffer[loc].message);
				//msg_buffer[loc].message = per_thread_write_vector[rand_write_idx][tid][j].dst_vid;
				ALWAYS_ASSERT (send_range.contains(msg_buffer[loc].dst_vid));

				loc++;
				if (loc == msg_buffer_capacity) {
#ifdef OldMessageTransfer
					cont.size_used = loc * sizeof(WritebackMessageWOOldMessage<T>);
#else
					cont.size_used = loc * sizeof(WritebackMessage<T>);
#endif
					req_buf_queue->push(cont);

					loc = 0;
					cont = RequestRespond::GetDataSendBuffer(DEFAULT_NIO_BUFFER_SIZE);
				}
			}
			if (loc != 0) {
#ifdef OldMessageTransfer
				cont.size_used = loc * sizeof(WritebackMessageWOOldMessage<T>);
#else
				cont.size_used = loc * sizeof(WritebackMessage<T>);
#endif
				req_buf_queue->push(cont);
			} else {
				RequestRespond::ReturnDataSendBuffer(cont);
			}
		}
		per_thread_idx[rand_write_idx][tid].idx = 0;
        //fprintf(stdout, "%s After FlushUpdateBufferPerThread tid %d, rand_write_idx %ld, send_range [%ld, %ld], per_thread_idx = %ld, per_thread_write_vector_capacity = %ld, lgb total %ld\n", vector->vector_name.c_str(), tid, rand_write_idx, send_range.GetBegin(), send_range.GetEnd(), (int64_t) per_thread_idx[rand_write_idx][tid].idx, per_thread_write_vector_capacity, cur_target_flag->GetTotal());
		return overflow;
	}

    /**
     * Flush update buffer
     */
	void FlushUpdateBuffer(int64_t start_part_id, int64_t part_id, int64_t next_part_id, int64_t subchunk_idx, int64_t overflow) {
        if (UserArguments::USE_PULL) return;
		if (!(RandRDWR == WRONLY || RandRDWR == RDWR)) return;
		
        int machine_id = part_id / UserArguments::VECTOR_PARTITIONS;
		int64_t chunk_idx = part_id % UserArguments::VECTOR_PARTITIONS;
		int next_machine_id = next_part_id / UserArguments::VECTOR_PARTITIONS;
		int64_t next_chunk_idx = next_part_id % UserArguments::VECTOR_PARTITIONS;
		ALWAYS_ASSERT (cur_rand_write_idx == 0 || cur_rand_write_idx == 1);
		ALWAYS_ASSERT (subchunk_idx >= 0 && subchunk_idx < PartitionStatistics::num_subchunks_per_edge_chunk());
		ALWAYS_ASSERT (cur_dst_vector_vid_range.contains(cur_dst_vector_part_writeback_tasks[cur_rand_write_idx][subchunk_idx]));

        Range<node_t> send_vid_range = cur_dst_vector_part_writeback_tasks[cur_rand_write_idx][subchunk_idx];
        ALWAYS_ASSERT(PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(machine_id, chunk_idx).contains(send_vid_range));
        
        //fprintf(stdout, "cur_dst_vector_part_writeback_tasks[%ld][%ld].begin <- -1\n", cur_rand_write_idx, subchunk_idx);
        std::atomic_store((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[cur_rand_write_idx][subchunk_idx].begin, (node_t) -1);

        // Issue writeback IO
        Aio_Helper::EnqueueAsynchronousTask(TG_DistributedVectorWindow::callOutputVectorWriteMessageGeneration, this, (char*) cur_local_gather_buffer, (char*) OM_cur_local_gather_buffer, TG_DistributedVectorBase::lgb_toggle, send_vid_range.GetBegin() - cur_dst_vector_vid_range.GetBegin(), send_vid_range.GetEnd() - cur_dst_vector_vid_range.GetBegin(), cur_dst_vector_vid_range, cur_rand_write_idx, subchunk_idx, &write_rand_req_buf_queue[cur_dst_vector_part_machine], overflow);
    }
    
    /**
     * Accumulate messages for per-thread LGB
     */
    void _AccumulateNewAndOldMessagesIntoTGB(int64_t loc, T new_msg, T old_msg) {
        switch (op) {
            case MAX:
            case MIN:
                //AtomicOperation<T, op>(&cur_target_gather_buffer[loc], new_msg);
                //AtomicOperation<T, op>(&OM_cur_target_gather_buffer[loc], old_msg);
								//if (UserArguments::UPDATE_VERSION >= 1)
	              //  fprintf(stdout, "[%ld] [_AccumulateNewAndOldMessagesIntoTGB] TGB[%ld] = (%ld, cnt %d) <- old_msg %ld, new_msg %ld\n", PartitionStatistics::my_machine_id(), loc, (int64_t) GET_VAL_64BIT(cur_target_gather_buffer[loc]), (int) GET_CNT_64BIT(cur_target_gather_buffer[loc]), old_msg, new_msg);
                if (old_msg == identity_element) {
                    AtomicUpdateAggregationWithCntStatic<T, op>(&cur_target_gather_buffer[loc], new_msg);
                } else {
                    AtomicUpdateAggregation<T, op>(&cur_target_gather_buffer[loc], old_msg, new_msg);
                }
                //fprintf(stdout, "[%ld] [After _AccumulateNewAndOldMessagesIntoTGB] TGB[%ld] = (%ld, cnt %d)\n", PartitionStatistics::my_machine_id(), loc, (int64_t) GET_VAL_64BIT(cur_target_gather_buffer[loc]), (int) GET_CNT_64BIT(cur_target_gather_buffer[loc]));
                break;
            case MULTIPLY:
            case PLUS:
                int tid = TG_ThreadContexts::thread_id;
                //std::atomic_fetch_add((std::atomic<int64_t>*) &per_thread_cnt[0][tid].idx, +1L);
                if (!checkEquality(new_msg, old_msg)) {
                  //std::atomic_fetch_add((std::atomic<int64_t>*) &per_thread_cnt[1][tid].idx, +1L);
                    AtomicUpdateAggregation<T, op>(&cur_target_gather_buffer[loc], old_msg, new_msg);
                }
                //AtomicUpdateAggregation<T, op>(&OM_global_gather_buffer[loc], old_msg, new_msg);
                break;
            default: // LOR
                AtomicOperation<T, op>(&cur_target_gather_buffer[loc], new_msg);
                //AtomicOperation<T, op>(&OM_cur_target_gather_buffer[loc], old_msg);
                //fprintf(stdout, "%s op = %d\n", vector->vector_name.c_str(), op);
                //LOG_ASSERT(false);
                break;
        }
    }
    
    /**
     * Accumulate messages for per-thread LGB
     */
    void _AccumulateNewAndOldMessagesIntoTGB(int64_t loc, T& new_msg) {
        switch (op) {
            case MAX:
            case MIN:
                AtomicUpdateAggregationWithCnt<T, op>(&cur_target_gather_buffer[loc], new_msg);
                break;
            case MULTIPLY:
            case PLUS:
                int tid = TG_ThreadContexts::thread_id;
                //std::atomic_fetch_add((std::atomic<int64_t>*) &per_thread_cnt[2][tid].idx, +1L);
                AtomicOperation<T, op>(&cur_target_gather_buffer[loc], new_msg);
                break;
            default: // LOR
                AtomicOperation<T, op>(&cur_target_gather_buffer[loc], new_msg);
                break;
        }
    }
    
    /**
     * Accumulate messages for per-thread LGB
     */
    void _AccumulateNewAndOldMessagesIntoTGB(T* target_buffer, int64_t loc, T& new_msg, T& old_msg) {
        switch (op) {
            case MAX:
            case MIN:
                UpdateAggregation<T, op>(&target_buffer[loc], old_msg, new_msg);
                break;
            case MULTIPLY:
            case PLUS:
                int tid = TG_ThreadContexts::thread_id;
                //std::atomic_fetch_add((std::atomic<int64_t>*) &per_thread_cnt[3][tid].idx, +1L);
                if (!checkEquality(new_msg, old_msg)) {
                    //std::atomic_fetch_add((std::atomic<int64_t>*) &per_thread_cnt[4][tid].idx, +1L);
                    UpdateAggregation<T, op>(&target_buffer[loc], old_msg, new_msg);
                }
                break;
            default: // LOR
                Operation<T, op>(&target_buffer[loc], new_msg);
                break;
        }
    }

    /**
     * Generate output vector write message
     */
    // TODO @tslee help
	void OutputVectorWriteMessageGeneration (char* buf, char* OM_buf, int64_t lgb_dirty_flag_toggle, int64_t from, int64_t to, Range<node_t> send_range, int64_t rand_write_idx, int64_t subchunk_idx, tbb::concurrent_queue<SimpleContainer>* req_buf_queue, int64_t overflow) {
		ALWAYS_ASSERT (subchunk_idx != -1);
		ALWAYS_ASSERT (std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].begin) == -1);
		ALWAYS_ASSERT (std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].end) != -1);
		ALWAYS_ASSERT (buf != NULL);
        ALWAYS_ASSERT (!UserArguments::USE_PULL);
		
        T* prev_buff = (T*) buf;
        //T* OM_prev_buff = (T*) OM_buf;
        TwoLevelBitMap<node_t>* lgb_dirty_flag = &TG_DistributedVectorBase::lgb_dirty_flags[lgb_dirty_flag_toggle];
        //fprintf(stdout, "[%ld] FlushUpdateBuffer from %ld, to %ld, subchunk_idx %ld, lgb_dirty_flag[] overflow %ld\n", PartitionStatistics::my_machine_id(), from, to, subchunk_idx, overflow);
        //fprintf(stdout, "[%ld] FlushUpdateBuffer from %ld, to %ld, subchunk_idx %ld, lgb_dirty_flag[%ld] total = %ld, overflow %ld\n", PartitionStatistics::my_machine_id(), from, to, subchunk_idx, (int64_t) lgb_dirty_flag_toggle, lgb_dirty_flag->GetTotal(), overflow);
		turbo_timer tmp_tim;


		tmp_tim.start_timer(0);
		int64_t msg_cnts = 0;
        node_t vid_from = from + send_range.GetBegin();
        node_t vid_to = to + send_range.GetBegin();
        
#ifdef TEMP_SINGLE_MACHINE
        if (overflow && (PartitionStatistics::num_machines() != 1)) {
#else
        if (overflow) {
#endif
			thread_local int64_t idx__ = 0;
			thread_local SimpleContainer cont__;
#ifdef OldMessageTransfer
			thread_local WritebackMessageWOOldMessage<T>* send_buff__;
			int64_t size_once = DEFAULT_NIO_BUFFER_SIZE / sizeof(WritebackMessageWOOldMessage<T>);
            int64_t message_size = sizeof(WritebackMessageWOOldMessage<T>);
#else
			thread_local WritebackMessage<T>* send_buff__;
			int64_t size_once = DEFAULT_NIO_BUFFER_SIZE / sizeof(WritebackMessage<T>);
            int64_t message_size = sizeof(WritebackMessage<T>);
#endif
			auto iden = idenelem<T>(op);
		    tmp_tim.start_timer(1);
		    tmp_tim.stop_timer(1);

			int64_t to_open = to + 1;

			T* prev_buff_vid_indexable = prev_buff - send_range.GetBegin();
			//T* OM_prev_buff_vid_indexable = OM_prev_buff - send_range.GetBegin();

		    tmp_tim.start_timer(2);

            /** Send data for dirty values in LGB */
            Range<node_t> range(from, to);
            lgb_dirty_flag->InvokeIfMarked([&](node_t bitmap_idx) {
                node_t i = bitmap_idx + send_range.GetBegin();

                // TODO if msg == old_msg, do not send
#ifdef OldMessageTransfer
                //send_buff__[idx__].old_message = OM_prev_buff_vid_indexable[i];
#endif
                send_buff__[idx__].message = prev_buff_vid_indexable[i];
                send_buff__[idx__].dst_vid = i;
                //fprintf(stdout, "(%d, %d) %s LGB Flush dst_vid %ld, message = %ld\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, vector->vector_name.c_str(), i, prev_buff_vid_indexable[i]);
                ALWAYS_ASSERT(send_range.contains(send_buff__[idx__].dst_vid));

                prev_buff_vid_indexable[i] = iden;
                //OM_prev_buff_vid_indexable[i] = iden;

                if (++idx__ == size_once) {
                    cont__.size_used = idx__ * message_size;
                    req_buf_queue->push(cont__);

                    idx__ = 0;
                    cont__ = RequestRespond::GetDataSendBuffer(DEFAULT_NIO_BUFFER_SIZE);

                    INVARIANT(cont__.size_used == 0);
                    INVARIANT(cont__.data != NULL);

#ifdef OldMessageTransfer
                    send_buff__ = (WritebackMessageWOOldMessage<T>*) cont__.data;
#else
                    send_buff__ = (WritebackMessage<T>*) cont__.data;
#endif
                }
            }, range, false, UserArguments::NUM_THREADS,
            [&](){
                cont__.size_used = idx__ * message_size;
                if (idx__ != 0) {
                    req_buf_queue->push(cont__);
                } else {
                    RequestRespond::ReturnDataSendBuffer(cont__);
                }
            },
            [&](){
                idx__ = 0;
                cont__ = RequestRespond::GetDataSendBuffer(DEFAULT_NIO_BUFFER_SIZE);
#ifdef OldMessageTransfer
                send_buff__ = (WritebackMessageWOOldMessage<T>*) cont__.data;
#else
                send_buff__ = (WritebackMessage<T>*) cont__.data;
#endif
            });
            tmp_tim.stop_timer(2);
        }
        tmp_tim.stop_timer(0);

#ifndef PERFORMANCE
        auto iden = idenelem<T>(op);
        for(int64_t i = from; i <= to; i++) {
            ALWAYS_ASSERT (prev_buff[i] == iden);
            //ALWAYS_ASSERT (OM_prev_buff[i] == iden);
        }
#endif

        SimpleContainer cont(NULL, 0, 0);
		req_buf_queue->push(cont);

		ALWAYS_ASSERT(std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].begin) == -1);
		ALWAYS_ASSERT(std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].end) != -1);
		std::atomic_store((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].end, (node_t) -1);
        
#ifdef REPORT_PROFILING_TIMERS
        fprintf(stdout, "[%s] GenerateMsgOV (%ld, %ld) %ld // [%ld, %ld] // %.4f %.4f %.4f secs // Overflow? %ld, Sent %ld msgs, Socket: %ld, %p, \n", vector->vector_name.c_str(), (int64_t) UserArguments::UPDATE_VERSION, (int64_t) UserArguments::SUPERSTEP, (int64_t) PartitionStatistics::my_machine_id(), vid_from, vid_to, tmp_tim.get_timer(0), tmp_tim.get_timer(1), tmp_tim.get_timer(2), (int64_t) (overflow? 1L: 0L), (int64_t) msg_cnts, TG_ThreadContexts::socket_id, buf);
#endif
    }

    void RequestOutputVectorMessagePush(T* buf, T* OM_buf, int partition_id, int64_t fromChunkID, int64_t chunkID,int64_t combined, int64_t edges_cnt, int64_t rand_write_idx) {
		ALWAYS_ASSERT (combined > 0);
		ALWAYS_ASSERT (buf != NULL);
		ALWAYS_ASSERT (rand_write_idx == 0 || rand_write_idx == 1);
		
        int backoff = 1;
		turbo_timer tmp_tim;

		tmp_tim.start_timer(0);
		for (int64_t subchunk_idx = 0; subchunk_idx < PartitionStatistics::num_subchunks_per_edge_chunk(); subchunk_idx++) {
			while (std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].begin) != -1) {
                usleep (backoff * 64);
                if (backoff <= 128) backoff *= 2;
			}
		}
		tmp_tim.stop_timer(0);

		tmp_tim.start_timer(1);
		for (int64_t subchunk_idx = 0; subchunk_idx < PartitionStatistics::num_subchunks_per_edge_chunk(); subchunk_idx++) {
			while (std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].end) != -1) {
                usleep (backoff * 64);
                if (backoff <= 128) backoff *= 2;
			}
			std::atomic_store((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[rand_write_idx][subchunk_idx].end, (node_t) -2);
		}
		tmp_tim.stop_timer(1);
		vector->rand_write_send_eom_count+= combined;

#ifdef REPORT_PROFILING_TIMERS
        fprintf(stdout, "RequestOvPush (%ld, %ld) %ld %ld secs %.4f %.4f\n", (int64_t) UserArguments::UPDATE_VERSION, (int64_t) UserArguments::SUPERSTEP, (int64_t) PartitionStatistics::my_machine_id(), (int64_t) partition_id, tmp_tim.get_timer(0), tmp_tim.get_timer(1));
#endif
    }

    /**
     * Wait for vector IO
     */
	void WaitForVectorIO() {
        if ((vector->CheckTargetForPull() && UserArguments::USE_PULL) || (!UserArguments::USE_PULL && (RandRDWR == RDWR || RandRDWR == WRONLY))) {
            /* Wait until request queue is empty */
			for (int idx = 0; idx < PartitionStatistics::num_machines(); idx++) {
				while (!write_rand_req_buf_queue[idx].empty()) {
					_mm_pause();
                }
			}
		}
        if(RandRDWR == WRONLY || RandRDWR == RDWR) {
          int64_t cnt1 = 0;
          int64_t cnt2 = 0;
          int64_t cnt3 = 0;
          int64_t cnt4 = 0;
          int64_t cnt5 = 0;
          for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
            cnt1 += per_thread_cnt[0][i].idx;
            per_thread_cnt[0][i].idx = 0;
          }
          for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
            cnt2 += per_thread_cnt[1][i].idx;
            per_thread_cnt[1][i].idx = 0;
          }
          for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
            cnt3 += per_thread_cnt[2][i].idx;
            per_thread_cnt[2][i].idx = 0;
          }
          for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
            cnt4 += per_thread_cnt[3][i].idx;
            per_thread_cnt[3][i].idx = 0;
          }
          for (int i = 0; i < UserArguments::NUM_THREADS; i++) {
            cnt5 += per_thread_cnt[4][i].idx;
            per_thread_cnt[4][i].idx = 0;
          }
          fprintf(stdout, "(%d, %d) cnt1 = %ld, cnt2 = %ld, cnt3 = %ld, cnt4 = %ld, cnt5 = %ld\n", UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, cnt1, cnt2, cnt3, cnt4, cnt5);
    }
	}

	/**
     * Send request for output vector pull
     */
    void SendRequestOutputVectorPull(int partition_id, int64_t chunkID) {
		OutputVectorReadRequest req;
		req.rt = OutputVectorRead;
		req.from = PartitionStatistics::my_machine_id();
		req.to = partition_id;
		req.vectorID = vector->vectorID;
		req.chunkID = chunkID;
		req.lv = UserArguments::CURRENT_LEVEL;

		RequestRespond::SendRequest(partition_id, &req);
	}

    // TODO not used
    void ReceiveOutputVectorPull(int partition_id, int64_t chunkID, int lv) {
	    LOG_ASSERT(false); // by syko (2019/11/06)
    }
	
    /**
     * Send request for input vector pull
     */
    void SendRequestInputVectorPull(int partition_id, int64_t chunkID) {
		InputVectorReadRequest req;
		req.rt = InputVectorRead;
		req.from = PartitionStatistics::my_machine_id();
		req.to = partition_id;
		req.vectorID = vector->vectorID;
		req.chunkID = chunkID;
		req.lv = UserArguments::CURRENT_LEVEL;

		RequestRespond::SendRequest(partition_id, &req);
	}
    
    /**
     * Receive input vector pull
     */
    void ReceiveInputVectorPull(int partition_id, int64_t chunkID, int lv) {
#ifdef OldMessageTransfer
		int64_t size_once = DEFAULT_NIO_BUFFER_SIZE / sizeof(WritebackMessageWOOldMessage<T>);
#else
		int64_t size_once = DEFAULT_NIO_BUFFER_SIZE / sizeof(WritebackMessage<T>);
#endif

        //fprintf(stdout, "[%ld] (%ld,%ld) [ReceiveInputVectorPull] receive from %ld\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, partition_id);

		bool run = true;
		int64_t recv_bytes;
		int64_t accm_recv_bytes = 0;
		turbo_timer tmr;
        std::queue<std::future<void>> reqs_to_wait;
		tmr.start_timer(0);

        ALWAYS_ASSERT(pull_receive_buff != NULL);
        ALWAYS_ASSERT(cur == 0 || cur == 1);
        ALWAYS_ASSERT(pull_receive_buff[cur] != NULL);
        T* pull_vector_buff_indexable = pull_receive_buff[cur] - PartitionStatistics::per_machine_first_node_id(partition_id);
        //T* OM_prev_lgb_vid_indexable = OM_prev_local_gather_buffer - PartitionStatistics::per_machine_first_node_id(partition_id);
        SimpleContainer cont;
#ifdef OldMessageTransfer
        WritebackMessageWOOldMessage<T>* recv_buff_;
#else
        WritebackMessage<T>* recv_buff_;
#endif
       
/*#ifndef PERFORMANCE
        TG_DistributedVectorBase::lgb_dirty_flags[cur].TwoLevelBitMap<int64_t>::InvokeIfMarked([&](node_t idx) {
            ALWAYS_ASSERT (OM_prev_local_gather_buffer[idx] == identity_element);
        });
        TG_DistributedVectorBase::lgb_dirty_flags[cur].ClearAll();
#endif*/
        
        /* Wait until IO finishes */
        while (run) {
            cont = RequestRespond::GetTempDataBuffer(DEFAULT_NIO_BUFFER_SIZE);
#ifdef OldMessageTransfer
            recv_buff_ = (WritebackMessageWOOldMessage<T>*) cont.data;
#else
            recv_buff_ = (WritebackMessage<T>*) cont.data;
#endif
            ALWAYS_ASSERT (recv_buff_ != nullptr);
            ALWAYS_ASSERT (cont.capacity == DEFAULT_NIO_BUFFER_SIZE);
            ALWAYS_ASSERT (chunkID == 0);
            tmr.start_timer(1);
            /** Receive data from server */
			recv_bytes = vector->client_sockets.recv_from_server((char*) recv_buff_, 0, partition_id);
            accm_recv_bytes += recv_bytes;
            tmr.stop_timer(1);
            if (recv_bytes == 0) {
                RequestRespond::ReturnTempDataBuffer(cont);
                break;
            }

#ifdef OldMessageTransfer
            ALWAYS_ASSERT(recv_bytes % sizeof(WritebackMessageWOOldMessage<T>) == 0);
			int64_t num_entries = recv_bytes / sizeof(WritebackMessageWOOldMessage<T>);
#else
            ALWAYS_ASSERT(recv_bytes % sizeof(WritebackMessage<T>) == 0);
			int64_t num_entries = recv_bytes / sizeof(WritebackMessage<T>);
#endif
            // XXX if enable below openmp, violate this condition "ALWAYS_ASSERT (TG_ThreadContexts::thread_id == omp_get_thread_num())"
//#pragma omp parallel for num_threads(4)
            reqs_to_wait.push(Aio_Helper::EnqueueAsynchronousTask(TG_DistributedVectorWindow::AccumulateMessageIntoLGB, cont, pull_vector_buff_indexable, num_entries, cur, partition_id));
        }
        while (!reqs_to_wait.empty()) {
            reqs_to_wait.front().get();
            reqs_to_wait.pop();
        }
		tmr.stop_timer(0);
        
        //fprintf(stdout, "DONE [%ld] (%ld,%ld) [ReceiveInputVectorPull] receive from %ld/ %.4f %.4f / %ld bytes / %.4f MB/sec\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, partition_id, tmr.get_timer(0), tmr.get_timer(1), accm_recv_bytes, (double) accm_recv_bytes / (1024 * 1024 * tmr.get_timer(1)));
#ifdef REPORT_PROFILING_TIMERS
        fprintf(stdout, "DONE [%ld] (%ld,%ld) [ReceiveInputVectorPull] receive from %ld/ %.4f %.4f / %ld bytes / %.4f MB/sec\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, partition_id, tmr.get_timer(0), tmr.get_timer(1), accm_recv_bytes, (double) accm_recv_bytes / (1024 * 1024 * tmr.get_timer(1)));
#endif
        
        vector->rand_write_recv_eom_count+= 1;  // syko
    }

    /**
     * Accumulate message into LGB
     */
    static void AccumulateMessageIntoLGB (SimpleContainer cont, T* pull_vector_buff_indexable, int64_t num_entries, int cur, int partition_id) {
#ifdef OldMessageTransfer
        WritebackMessageWOOldMessage<T>* recv_buff = (WritebackMessageWOOldMessage<T>*) cont.data;
#else
        WritebackMessage<T>* recv_buff = (WritebackMessage<T>*) cont.data;
#endif
        for(int64_t i = 0 ; i < num_entries; i++) {
            node_t vid = recv_buff[i].dst_vid;
            T value = recv_buff[i].message;
            pull_vector_buff_indexable[vid] = value;

            TG_DistributedVectorBase::lgb_dirty_flags[cur].TwoLevelBitMap<int64_t>::Set_Atomic_IfNotSet(vid - PartitionStatistics::per_machine_first_node_id(partition_id));
        }
        //RequestRespond::ReturnDataRecvBuffer(cont);
        RequestRespond::ReturnTempDataBuffer(cont);
    }

    // APIs

    // Called by [PULL_ReadStoredMessage]
    inline T PULL_ReadStoredMessage(node_t vid) {
		ALWAYS_ASSERT (cur_dst_vector_vid_range.contains(vid));
        return pull_vector_buffer_vid_indexable[vid];
        //return OM_cur_local_gather_buffer_vid_indexable[vid];
    }

#ifdef OldMessageTransfer
    /**
     * Output vector write
     */
    inline void OvWrite(node_t vid, T value, T old_value) {
			ALWAYS_ASSERT (PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(cur_dst_vector_part_machine, cur_dst_vector_part_chunk).contains(vid));
			ALWAYS_ASSERT (PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(cur_dst_vector_part_machine, cur_dst_vector_part_chunk).GetBegin() == cur_dst_vector_first_vid);
			ALWAYS_ASSERT (cur_dst_vector_vid_range.contains(vid));
			ALWAYS_ASSERT (!UserArguments::USE_PULL);

			int tid = TG_ThreadContexts::thread_id;
			ALWAYS_ASSERT (tid >= 0 && tid < UserArguments::NUM_THREADS);
			if (per_thread_idx[cur_rand_write_idx][tid].idx == per_thread_write_vector_capacity) {
#ifdef PER_THREAD_LGB
        if (vid - cur_dst_vector_first_vid < per_thread_lgb_num_nodes) {
          //_AccumulateNewAndOldMessagesIntoTGB(per_thread_cur_local_gather_buffer_vid_indexable[tid], vid, value, old_value);
          AccumulateNewAndOldMessagesIntoTGB<T, op>(&per_thread_cur_local_gather_buffer_vid_indexable[tid][vid], value, old_value);
          //Operation<T, op>(&per_thread_cur_local_gather_buffer_vid_indexable[tid][vid], value);
          //if (old_value != identity_element) Operation<T, op>(&per_thread_OM_cur_local_gather_buffer_vid_indexable[tid][vid], old_value);
          cur_target_flag->TwoLevelBitMap<int64_t>::Set_Atomic_IfNotSet(vid - cur_dst_vector_first_vid);
        } else {
          //_AccumulateNewAndOldMessagesIntoTGB(vid - cur_dst_vector_first_vid, value, old_value);
          AtomicAccumulateNewAndOldMessagesIntoTGB<T, op>(&cur_target_gather_buffer[vid - cur_dst_vector_first_vid], value, old_value);
          //AtomicOperation<T, op>(&cur_target_gather_buffer_vid_indexable[vid], value);
          //if (old_value != identity_element) AtomicOperation<T, op>(&OM_cur_target_gather_buffer_vid_indexable[vid], old_value);
          cur_target_flag->TwoLevelBitMap<int64_t>::Set_Atomic_IfNotSet(vid - cur_dst_vector_first_vid);
        }
#else
				//AtomicOperation<T, op>(&cur_target_gather_buffer_vid_indexable[vid], value);
				//if (old_value != identity_element) AtomicOperation<T, op>(&OM_cur_target_gather_buffer_vid_indexable[vid], old_value);
				//_AccumulateNewAndOldMessagesIntoTGB(vid - cur_dst_vector_first_vid, value, old_value);
				AtomicAccumulateNewAndOldMessagesIntoTGB<T, op>(&cur_target_gather_buffer[vid - cur_dst_vector_first_vid], value, old_value);
				//cur_target_flag->TwoLevelBitMap<int64_t>::Set_Atomic(vid - cur_dst_vector_first_vid);
				cur_target_flag->TwoLevelBitMap<int64_t>::Set_Atomic_IfNotSet(vid - cur_dst_vector_first_vid);
				//fprintf(stdout, "\tOvWrite.. tid %ld, vid %ld, %ld\n", tid, vid, cur_dst_vector_first_vid);
#endif
			} else {
				int64_t idx = per_thread_idx[cur_rand_write_idx][tid].idx;
				per_thread_write_vector[cur_rand_write_idx][tid][idx].dst_vid = vid;
				per_thread_write_vector[cur_rand_write_idx][tid][idx].message = value;
				per_thread_write_vector[cur_rand_write_idx][tid][idx].old_message = old_value;
				++per_thread_idx[cur_rand_write_idx][tid].idx;
				// XXX remove below code line --> I will check it
				//cur_target_flag->TwoLevelBitMap<int64_t>::Set_Atomic_IfNotSet(vid - cur_dst_vector_first_vid);
			}
		}
#else

    /**
     * Output vector write
     */
	inline void OvWrite(node_t vid, T value) {
		ALWAYS_ASSERT (PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(cur_dst_vector_part_machine, cur_dst_vector_part_chunk).contains(vid));
		ALWAYS_ASSERT (PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(cur_dst_vector_part_machine, cur_dst_vector_part_chunk).GetBegin() == cur_dst_vector_first_vid);
		ALWAYS_ASSERT (cur_dst_vector_vid_range.contains(vid));
        ALWAYS_ASSERT (!UserArguments::USE_PULL);

		int tid = TG_ThreadContexts::thread_id;
		ALWAYS_ASSERT (tid >= 0 && tid < UserArguments::NUM_THREADS);
		if (per_thread_idx[cur_rand_write_idx][tid].idx == per_thread_write_vector_capacity) {
			AtomicOperation<T, op>(&cur_target_gather_buffer_vid_indexable[vid], value);
		} else {
			int64_t idx = per_thread_idx[cur_rand_write_idx][tid].idx;
			per_thread_write_vector[cur_rand_write_idx][tid][idx].dst_vid = vid;
			per_thread_write_vector[cur_rand_write_idx][tid][idx].message = value;
			++per_thread_idx[cur_rand_write_idx][tid].idx;
		}
	}

#endif
    
    /**
     * Initialize vid as val
     */
    inline void IvInitialize(node_t vid, T val) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
        /* Set value */
#ifndef GB_VERSIONED_ARRAY
        seq_read_vector_buff[seq_cur][lid] = val;
#else
        seq_read_vector_buff[0][lid] = val;
#endif
	}
    
    /**
     * Initialize vid as val in previous version
     */
    inline void IvInitializePrev(node_t vid, T val) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
        /* Set value */
#ifndef GB_VERSIONED_ARRAY
        prev_seq_read_vector_buff[seq_cur][lid] = val;
#else
        prev_seq_read_vector_buff[0][lid] = val;
#endif
	}

    /**
     * Write vid value val
     */
    inline void IvWrite(node_t vid, T val) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
        /* Mark dirty */
#ifndef NO_LOGGING_VERSIONED_ARRAY
        versioned_iv_array.MarkDirty(lid);
#else
      if (!UserArguments::QUERY_ONGOING)
        versioned_iv_array.MarkDirty(lid);
#endif
        /* Check value == val and mark changed/flush if different */
#ifndef GB_VERSIONED_ARRAY
		ALWAYS_ASSERT(seq_read_vector_buff[0] != nullptr && seq_read_vector_buff[1] != nullptr);
        if (seq_read_vector_buff[seq_cur][lid] != val) {
#ifndef NO_LOGGING_VERSIONED_ARRAY
            versioned_iv_array.MarkChanged(lid);
            versioned_iv_array.MarkDiscard(lid);
            versioned_iv_array.MarkToFlush(lid);
#else
          if (!UserArguments::QUERY_ONGOING) {
            versioned_iv_array.MarkChanged(lid);
            versioned_iv_array.MarkDiscard(lid);
            versioned_iv_array.MarkToFlush(lid);
          }
#endif
            /* Update value */
            seq_read_vector_buff[seq_cur][lid] = val;
        }
#else
		ALWAYS_ASSERT(seq_read_vector_buff[1] != nullptr);
        //if (seq_read_vector_buff[1][lid] != val) {
            versioned_iv_array.MarkChanged(lid);
            versioned_iv_array.MarkToFlush(lid);
            seq_read_vector_buff[1][lid] = val;
        //}
#endif
	}
    
    inline void IvWriteUnsafe(node_t vid, T val) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
        /* Mark dirty */
#ifndef NO_LOGGING_VERSIONED_ARRAY
        versioned_iv_array.MarkDirtyUnsafe(lid);
#else
      if (!UserArguments::QUERY_ONGOING) {
        versioned_iv_array.MarkDirtyUnsafe(lid);
      }
#endif
        /* Check value == val and mark changed/flush if different */
#ifndef GB_VERSIONED_ARRAY
        if (seq_read_vector_buff[seq_cur][lid] != val) {
#ifndef NO_LOGGING_VERSIONED_ARRAY
            //versioned_iv_array.MarkChanged(lid);
            //versioned_iv_array.MarkDiscard(lid);
            versioned_iv_array.MarkToFlushUnsafe(lid);
#else
          if (!UserArguments::QUERY_ONGOING) {
            versioned_iv_array.MarkToFlushUnsafe(lid);
          }
#endif
        /* Update value */
            seq_read_vector_buff[seq_cur][lid] = val;
        }
#else
        if (seq_read_vector_buff[1][lid] != val) {
            seq_read_vector_buff[1][lid] = val;
        }
#endif
	}
    
    /**
     * Mark vid as dirty at input vector
     */
    inline void IvMarkDirty(node_t vid) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
        versioned_iv_array.MarkDirty(lid);
	}
    
    /**
     * Mark vid as dirty at input vector (unsafe)
     */
    inline void IvMarkDirtyUnsafe(node_t vid) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
        versioned_iv_array.MarkDirtyUnsafe(lid);
	}
    
    /**
     * Read vid value of version t-1, superstep u from input vector
     */
    inline T& IvReadPrev(node_t vid) {
		if (prev_seq_read_vector_buff[seq_cur] == nullptr) {
            fprintf(stdout, "%s don't have prev_buf\n", vector->vector_name.c_str());
        }
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
#ifndef GB_VERSIONED_ARRAY
		ALWAYS_ASSERT(prev_seq_read_vector_buff[seq_cur] != nullptr);
		return prev_seq_read_vector_buff[seq_cur][lid];    // 0: current
#else
		ALWAYS_ASSERT(prev_seq_read_vector_buff[0] != nullptr);
		return prev_seq_read_vector_buff[0][lid];    // 0: current
#endif
    }
    
    /**
     * Read vid value of version t superstep u-1 from input vector
     */
    inline T& IvReadPrevSS(node_t vid) {
		ALWAYS_ASSERT(prev_ss_seq_read_vector_buff[seq_cur] != nullptr);
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		return prev_ss_seq_read_vector_buff[seq_cur][lid];
    }
    
    /**
     * Read vid value of version t-1, superstep u-1 from input vector
     */
    inline T& IvReadPrevSSSN(node_t vid) {
		ALWAYS_ASSERT(prev_sssn_seq_read_vector_buff[seq_cur] != nullptr);
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		return prev_sssn_seq_read_vector_buff[seq_cur][lid];
    }
	
    /**
     * Read vid value of version t, superstep u from input vector
     */
    inline T& IvRead(node_t vid) {
		ALWAYS_ASSERT(cur_src_vector_vid_range.contains(vid));
		node_t lid = vid - cur_src_vector_vid_range.GetBegin();
		ALWAYS_ASSERT(lid >= 0 && lid < max_entry);
		ALWAYS_ASSERT(seq_cur == 0 || seq_cur == 1);
#ifndef GB_VERSIONED_ARRAY
		ALWAYS_ASSERT(seq_read_vector_buff[0] != nullptr && seq_read_vector_buff[1] != nullptr);
		return seq_read_vector_buff[seq_cur][lid];    // 0: current
#else
		ALWAYS_ASSERT(seq_read_vector_buff[0] != nullptr);
		return seq_read_vector_buff[0][lid];    // 0: current
#endif
	}

    /**
     * Returns if input vector has changed for vid
     */
    virtual bool IsIvChanged(node_t vid) {
        return versioned_iv_array.IsChanged(vid - PartitionStatistics::my_first_internal_vid()); 
    }
    /**
     * Mark vid as changed in input vector
     */
    virtual void MarkIvChanged(node_t vid) { 
        versioned_iv_array.MarkChanged(vid - PartitionStatistics::my_first_internal_vid()); 
    }
    /**
     * Returns if input vector is dirty for vid
     */
    virtual bool IsIvDirty(node_t vid) { 
        return versioned_iv_array.IsDirty(vid - PartitionStatistics::my_first_internal_vid()); 
    }
    /**
     * Mark vid as dirty in input vector
     */
    virtual void MarkIvDirty(node_t vid) { 
        versioned_iv_array.MarkDirty(vid - PartitionStatistics::my_first_internal_vid()); 
    }
    /**
     * Clear vid dirty flag in input vector
     */
    virtual void ClearIvDirty(node_t vid) { 
        versioned_iv_array.ClearDirty(vid - PartitionStatistics::my_first_internal_vid()); 
    }
    
    /**
     * Get input vector dirty map
     */
    virtual TwoLevelBitMap<node_t>* GetIvDirtyBitMap() { return versioned_iv_array.GetDirtyFlags(); }
    /**
     * Get input vector changed map
     */
    virtual TwoLevelBitMap<node_t>* GetIvChangedBitMap() { return versioned_iv_array.GetChangedFlags(); }

    /**
     * Clear input vector flags
     */
    virtual void ClearFlagsForIncrementalProcessing(bool clear_dirty, bool clear_changed) {
        if (clear_dirty) versioned_iv_array.ClearAllDirty();
        if (clear_changed) versioned_iv_array.ClearAllChanged();
    }
    
    /**
     * Returns number of input vector supersteps for version u. Called
     * from TG_DistributedVector::GetNumIvSuperstepVersions().
     */
    virtual int64_t GetNumIvSuperstepVersions(int u) { return versioned_iv_array.GetNumberOfSuperstepVersions(u); }
    
    /**
     * Set current version of input vector as version u and superstep s.
     * Called from TG_DistributedVector::SetCurrentVersion().
     */
    virtual void SetCurrentVersion(int u, int s) {
        versioned_iv_array.SetCurrentVersion(u, s); 
        if (UserArguments::UPDATE_VERSION >= 1) {
            /* Update previous version as well */
            versioned_iv_array_prev.SetCurrentVersion(u - 1, s);
        }
    }
    
    /**
     * Asynchronously call to construct version for version u, superstep s
     */
    virtual void CallConstructNextSuperStepArrayAsync(int u, int s, bool run_static_processing) {
        versioned_iv_array.CallConstructNextSuperStepArrayAsync(u, s, run_static_processing); 
    }
    
    /**
     * Process pending async IO requests. Called from
     * TG_DistributedVector::ProcessPendingAioRequests()
     */
    virtual void ProcessPendingAioRequests() {
        versioned_iv_array.WaitForIoRequests(true, true); 
    }
    

  public:
	MemoryMappedArray<T>* cur_iv_mmap;
    std::vector<MemoryMappedArray<T>*> iv_mmaps;

#ifdef GB_VERSIONED_ARRAY
    GBVersionedArray<T, op> versioned_iv_array;
    GBVersionedArray<T, op> versioned_iv_array_prev;
#else
    VersionedArray<T, op> versioned_iv_array;
    VersionedArray<T, op> versioned_iv_array_prev;  /* Versioned IV array of the previous snapshot */
#endif

	T* prev_seq_read_vector_buff[2];    /* Input vector buffer for previous snapshot for each superstep */
	T* prev_ss_seq_read_vector_buff[2];
	T* prev_sssn_seq_read_vector_buff[2];
	T* seq_read_vector_buff[2];
	T* seq_write_vector_buff[2];

    T* pull_receive_buff[2];
    T* pull_send_buff;
    T* pull_vector_buffer_vid_indexable;

	T* cur_read_random_vector_buff;
	T* fut_read_random_vector_buff;

	T* cur_local_gather_buffer_vid_indexable;
	T* cur_local_gather_buffer;
	T* prev_local_gather_buffer;

    T* OM_cur_local_gather_buffer_vid_indexable;
    T* OM_cur_local_gather_buffer;
    T* OM_prev_local_gather_buffer;
	
    // per-thread lgb
    int64_t per_thread_lgb_num_nodes;
    T** per_thread_cur_local_gather_buffer_vid_indexable;
	T** per_thread_cur_local_gather_buffer;
    
    T** per_thread_OM_cur_local_gather_buffer_vid_indexable;
    T** per_thread_OM_cur_local_gather_buffer;

    // gather target buffer & bitmap pointer
    T* cur_target_gather_buffer;
    T* OM_cur_target_gather_buffer;
    T* cur_target_gather_buffer_vid_indexable;
    T* OM_cur_target_gather_buffer_vid_indexable;
    TwoLevelBitMap<int64_t>* cur_target_flag;

  private:
	int my_level;   /* Level of the window */
	int vectorID;   /* Vector ID of the window's vector */
    /* Identity element for given operation. Refer to Typedef.hpp
       to see supported operations */
	T identity_element;

	RDWRMODE SeqRDWR;   /* Read/write permission for sequential mode */
	RDWRMODE RandRDWR;  /* Read/write permission for random mode */

	int64_t cur_src_vector_part;
	int64_t cur_src_vector_part_machine;
	int64_t cur_src_vector_part_chunk;

	int64_t cur_dst_vector_part;
	int64_t cur_dst_vector_part_machine;
	int64_t cur_dst_vector_part_chunk;

	Range<node_t> cur_src_vector_vid_range;
	Range<node_t> cur_dst_vector_vid_range;
	node_t cur_dst_vector_first_vid;

	// Rand Write
	WritebackMessage<T> ** per_thread_write_vector[2];

	int64_t per_thread_write_vector_capacity;
	PaddedIdx* per_thread_idx[2];
	Range<node_t>* cur_dst_vector_part_writeback_tasks[2];
	std::future<void> write_rand_thread[2];
	int64_t cur_rand_write_idx;

	std::future<void> read_seq_thread;
	std::future<void> read_rand_thread;

	tbb::concurrent_queue<SimpleContainer>* write_rand_req_buf_queue;

	int64_t rand_read_combined;
	int64_t rand_write_combined;


	// Vector
	TG_DistributedVectorBase* vector;

	// Auxiliary Member Variables
	int seq_cur;
	int cur;
	int64_t max_entry;
	int64_t alloc_num_entries;

    bool create_read_only;
    bool maintain_prev_ss;

    PaddedIdx* per_thread_cnt[5];

  public:
    /**
     * Get size of vector window 
     */
    int64_t GetSizeOfVW() {
        int64_t bytes = 0;
        if(SeqRDWR == RDONLY || SeqRDWR == RDWR) {
            if (my_level == 0) {
                //fprintf(stdout, "[%ld] %s get memory usage\n", PartitionStatistics::my_machine_id(), vector->vector_name.c_str());
                bytes += versioned_iv_array.GetMemoryUsage();
                bytes += versioned_iv_array_prev.GetMemoryUsage();
                if (op == MIN || op == MAX) { // Allocate pull vector for MIN/MAX op
                    bytes += (3 * sizeof(T) * ((PartitionStatistics::max_internal_nodes() + UserArguments::VECTOR_PARTITIONS - 1) / UserArguments::VECTOR_PARTITIONS + UserArguments::VECTOR_PARTITIONS));
                }
            } else {
                bytes += 2 * sizeof(T) * PartitionStatistics::my_num_internal_nodes();
            }
        }
        return bytes;
    }
    /**
     * Return size of LGB for this window 
     */
    int64_t GetSizeOfLGB() {
        int64_t bytes = 0;
        if(RandRDWR == WRONLY || RandRDWR == RDWR) {
            // cur_lgb, prev_lgb, OM_cur_lgb, OM_prev_lgb
            bytes += 4 * sizeof(T) * PartitionStatistics::my_num_internal_nodes();
        }
        return bytes;
    }

    /**
     * Get total dalta size stored in disk for versioned_iv_array
     */
    virtual int64_t GetTotalDeltaSizeInDisk(Version from, Version to) {
        if (!(SeqRDWR == RDONLY || SeqRDWR == RDWR)) return 0;
        if (my_level != 0) return 0;
        return versioned_iv_array.GetTotalDeltaSizeInDisk(from, to);
    }

    /* Get vector names */
    void getvectornames(std::vector<json11::Json>& vector_names) { vector_names.push_back(versioned_iv_array.GetArrayName()); }
    /* Write versioned array IO wait time for input vector */
    void writeVersionedArrayIOWaitTime(JsonLogger*& json_logger) { versioned_iv_array.writeVersionedArrayIOWaitTime(json_logger); }
    /* Log versioned array IO wait time for input vector */
    void logVersionedArrayIOWaitTime() { versioned_iv_array.logVersionedArrayIOWaitTime(); }
    /* Aggregate read IO for input vector  */
    void aggregateReadIO(JsonLogger*& json_logger) { versioned_iv_array.aggregateReadIO(json_logger); }
    /* Aggregate write IO for input vector  */
    void aggregateWriteIO(JsonLogger*& json_logger) { versioned_iv_array.aggregateWriteIO(json_logger); }
    /* Aggregate non-overlapped time for input vector  */
    void aggregateNonOverlappedTime(JsonLogger*& json_logger) { versioned_iv_array.aggregateNonOverlappedTime(json_logger); }
	
    // TODO below four methods are never used

    /* Call receive output vector pull for given window */
	static void callReceiveOutputVectorPull(TG_DistributedVectorWindow<T, op>* vec, int partition_id, int64_t chunkID, int lv) {
		vec->ReceiveOutputVectorPull(partition_id, chunkID, lv);
	}
    /* Call receive input vector pull for given window */
	static void callReceiveInputVectorPull(TG_DistributedVectorWindow<T, op>* vec, int partition_id, int64_t chunkID, int lv) {
		vec->ReceiveInputVectorPull(partition_id, chunkID, lv);
    }
    /* Call request output vector message push for given window */
    static void callRequestOutputVectorMessagePush(TG_DistributedVectorWindow<T, op>* vec, T* buf, T* OM_buf, int partition_id, int64_t fromChunkID, int64_t chunkID, int64_t combined, int64_t edges_cnt, int64_t rand_write_idx) {
		vec->RequestOutputVectorMessagePush(buf, OM_buf, partition_id, fromChunkID, chunkID, combined, edges_cnt, rand_write_idx);
	}
    /* Call output vector write message generation for given window */
	static void callOutputVectorWriteMessageGeneration (TG_DistributedVectorWindow* vec, char* buf, char* OM_buf, int64_t lgb_dirty_flag_toggle, int64_t from, int64_t to, Range<node_t> send_range, int64_t rand_write_idx, int64_t subchunk_idx, tbb::concurrent_queue<SimpleContainer>* req_buffer_queue, int64_t overflow) {
		vec->OutputVectorWriteMessageGeneration(buf, OM_buf, lgb_dirty_flag_toggle, from, to, send_range, rand_write_idx, subchunk_idx, req_buffer_queue, overflow);
	}
};

/**
 * Initialize output vector push for the current window.
 */
template <typename T, Op op>
void TG_DistributedVectorWindow<T, op>::_InitializeOutputVectorPush(int64_t start_part_id, int64_t part_id, int64_t next_part_id) {
    ALWAYS_ASSERT (!UserArguments::USE_PULL);
    if (!(RandRDWR == WRONLY || RandRDWR == RDWR)) return;
    
    int machine_id = part_id / UserArguments::VECTOR_PARTITIONS;
    int64_t chunk_id = part_id % UserArguments::VECTOR_PARTITIONS;
    int next_machine_id = next_part_id / UserArguments::VECTOR_PARTITIONS;
    int64_t next_chunk_id = next_part_id % UserArguments::VECTOR_PARTITIONS;
    int64_t idx_to_put_future = (cur + 1) % 2;

    turbo_timer tim;
    tim.start_timer(0);
    tim.start_timer(1);
    /* For each subchunks, wait for writeback tasks to finish load data */
    for (int64_t i = 0; i < PartitionStatistics::num_subchunks_per_edge_chunk(); i++) {
        int64_t edge_subchunk_id = cur_dst_vector_part_machine * UserArguments::VECTOR_PARTITIONS * PartitionStatistics::num_subchunks_per_edge_chunk() + cur_dst_vector_part_chunk * PartitionStatistics::num_subchunks_per_edge_chunk() + i;
        ALWAYS_ASSERT (cur_dst_vector_part_writeback_tasks[idx_to_put_future][i].GetBegin() == -1);
        while (std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[idx_to_put_future][i].end) != -2) {
            _mm_pause();
        }

        ALWAYS_ASSERT(std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[idx_to_put_future][i].begin) == -1);
        ALWAYS_ASSERT(std::atomic_load((std::atomic<node_t>*) &cur_dst_vector_part_writeback_tasks[idx_to_put_future][i].end) == -2);

        cur_dst_vector_part_writeback_tasks[idx_to_put_future][i] = TurboDB::GetVidRangeByEdgeSubchunkId(edge_subchunk_id);
        ALWAYS_ASSERT (cur_dst_vector_vid_range.contains(cur_dst_vector_part_writeback_tasks[idx_to_put_future][i]));
    }
    tim.stop_timer(1);

    tim.start_timer(2);
    OutputVectorWriteMessageRequest req;
    req.rt = OutputVectorWriteMessage;
    req.from = PartitionStatistics::my_machine_id();
    req.to = machine_id;
    req.vectorID = vector->vectorID;
    req.fromChunkID = cur_src_vector_part_chunk;
    req.chunkID = cur_dst_vector_part_chunk;
    req.send_num = 1;
    req.tid = 0;
    req.idx = 0;
    req.lv = UserArguments::CURRENT_LEVEL;

    req.combined = rand_write_combined;
    rand_write_combined = 1;

    /** Wait for IO queue to be empty */
    while (!write_rand_req_buf_queue[cur_dst_vector_part_machine].empty()) {
        _mm_pause();
    }
    INVARIANT (write_rand_req_buf_queue[cur_dst_vector_part_machine].empty());

    /** Send async task for data write */
    RequestRespond::SendRequestAsynchronously(machine_id, req);
    Aio_Helper::EnqueueAsynchronousTask(RequestRespond::SendDataInBuffer, &write_rand_req_buf_queue[cur_dst_vector_part_machine], &(vector->client_sockets), cur_dst_vector_part_machine, -1, cur_dst_vector_part_chunk, req.lv, PartitionStatistics::num_subchunks_per_edge_chunk(), false);
    tim.stop_timer(2);
    tim.stop_timer(0);
    
#ifdef REPORT_PROFILING_TIMERS
    fprintf(stdout, "[%ld] InitializeOV (%ld, %ld) %ld %ld secs %.4f %.4f %.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, PartitionStatistics::my_machine_id(), machine_id, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2));
#endif

#ifndef PERFORMANCE
#pragma omp parallel for
    for (int64_t i = 0; i < max_entry; i++) {
        ALWAYS_ASSERT (cur_local_gather_buffer[i] == idenelem<T>(op));
        ALWAYS_ASSERT (OM_cur_local_gather_buffer[i] == idenelem<T>(op));
    }
#endif
}

/**
 * Initialize output vector of the current window.
 */
template <typename T, Op op>
void TG_DistributedVectorWindow<T, op>::InitializeOutputVector(int64_t start_part_id, int64_t part_id, int64_t next_part_id) {
    turbo_timer tim;
    tim.start_timer(0);

    cur_dst_vector_part = part_id;
    cur_dst_vector_part_machine = part_id / UserArguments::VECTOR_PARTITIONS;
    cur_dst_vector_part_chunk = part_id % UserArguments::VECTOR_PARTITIONS;
    cur_dst_vector_vid_range = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(cur_dst_vector_part_machine, cur_dst_vector_part_chunk);
    cur_dst_vector_first_vid = cur_dst_vector_vid_range.GetBegin();

    //if (cur_dst_vector_part_machine == PartitionStatistics::my_machine_id()) {
#ifdef TEMP_SINGLE_MACHINE
    if (PartitionStatistics::num_machines() == 1) {
#else
    if (false) {
#endif
        /**
         * Case when TEMP_SINGLE_MACHINE is on and is actually using single machine
         */
        if (UserArguments::INC_STEP != 2) {
            cur_target_gather_buffer = (T*) vector->GetGGB();
            OM_cur_target_gather_buffer = (T*) vector->GetOMGGB();
            cur_target_gather_buffer_vid_indexable = ((T*) vector->GetGGB()) - cur_dst_vector_first_vid;
            OM_cur_target_gather_buffer_vid_indexable = ((T*) vector->GetOMGGB()) - cur_dst_vector_first_vid;
        } else {
            cur_target_gather_buffer = (T*) vector->GetOMGGB();
            OM_cur_target_gather_buffer = nullptr;
            cur_target_gather_buffer_vid_indexable = ((T*) vector->GetOMGGB()) - cur_dst_vector_first_vid;
            OM_cur_target_gather_buffer_vid_indexable = nullptr;
        }
        cur_target_flag = vector->GetGgbMsgReceivedFlags();
    } else {
        /**
         * Case when not using single machine
         */
        cur_target_gather_buffer = cur_local_gather_buffer;
        OM_cur_target_gather_buffer = OM_cur_local_gather_buffer;
        cur_target_gather_buffer_vid_indexable = cur_local_gather_buffer - cur_dst_vector_first_vid;
        OM_cur_target_gather_buffer_vid_indexable = OM_cur_local_gather_buffer - cur_dst_vector_first_vid;
        cur_target_flag = vector->GetLgbDirtyFlag(TG_DistributedVectorBase::lgb_toggle);
        //fprintf(stdout, "%s Cur_target_flag toggle %ld\n", vector->vector_name.c_str(), (int64_t) TG_DistributedVectorBase::lgb_toggle);
    }
#ifdef PER_THREAD_LGB
#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
    {
        int i = omp_get_thread_num();
        per_thread_cur_local_gather_buffer_vid_indexable[i] = per_thread_cur_local_gather_buffer[i] - cur_dst_vector_first_vid;
        //per_thread_OM_cur_local_gather_buffer_vid_indexable[i] = per_thread_OM_cur_local_gather_buffer[i] - cur_dst_vector_first_vid;
    }
#endif

    int machine_id = part_id / UserArguments::VECTOR_PARTITIONS;
    int64_t chunk_id = part_id % UserArguments::VECTOR_PARTITIONS;
    int next_machine_id = next_part_id / UserArguments::VECTOR_PARTITIONS;
    int64_t next_chunk_id = next_part_id % UserArguments::VECTOR_PARTITIONS;

    int64_t idx_to_wait = cur;
    int64_t idx_to_put_future = (cur + 1) % 2;

    cur_rand_write_idx = idx_to_put_future;
    ALWAYS_ASSERT (idx_to_put_future == 0 || idx_to_put_future == 1);
    
    if (UserArguments::USE_PULL) {

    } else {
        _InitializeOutputVectorPush(start_part_id, part_id, next_part_id);
    }
}


#endif
