#ifndef __REQUESTRESPOND_H
#define __REQUESTRESPOND_H

/*
 * Design of the RequestRespond
 *
 * This class implements a request-response mechanism that enables machines on
 * a network to communicate with each other. Various request types are provided
 * such as reading DistributedVector, writing messages, reading materialized
 * adjacency list, etc.
 *
 * In the system initialization step, a thread is spawned to probe incoming 
 * requests from other machines and respond to them. The probing step can be
 * handled using MPI_Probe() API. According to the type of request, the
 * corresponding task is enqueued into the asynchronous thread pool (Please
 * refer ReceiveRequest() function).
 *
 * In particular, there are two important functions handled by this class. One
 * is RespondAdjListBatchIO(), and the other is SendDataInBuffer().
 *     - RespondAdjListBatchIO: It handles I/O requests for materialized 
 *     adjacency lists. To achieve the goal, we first find the pages that needs
 *     to be read from disk (by FindPageIdsToReadFrom function). After that,
 *     we spawn a thread that materializes full adjacency lists from pages
 *     (by MaterializeAndEnqueue function). The materialized adjacency lists 
 *     are put into a queue (req_data_queue), and a pre-spawned thread sends 
 *     the adjacency list in the queue to the requesting machine over the 
 *     network.
 *     - SendDataInBuffer: It sends data in network I/O buffer. Usually, this
 *     function is used by TG_DistributedVectorWindow class. In that class,
 *     we store the locally aggregated messages in a buffer and put them into
 *     a queue. In the SendDataInBuffer() function, the buffer is continuously 
 *     popped from the queue and the data in the buffer is sent over the network.
 *     This process continues until it meets the EOM.
 */

#include <thread>
#include <unistd.h>
#include <mutex>
#include <cstring>
#include <algorithm>
#include <omp.h>
#include <mpi.h>

#include "common/assert.hpp"
#include "common/common.hpp"
#include "common/types.hpp"
#include "parallel/concurrentqueue.h"
#include "turbo_tcp.hpp"
#include "parallel/VariableSizedMemoryAllocatorWithCircularBuffer.hpp"
#include "parallel/ThreadPool.hpp"
#include "parallel/util.hpp"
#include "tbb/concurrent_queue.h"
#include "TwoLevelBitMap.hpp"
#include "cache/disk_aio/eXDB_dist_internal.hpp"
#include "parallel/Aio_Helper.hpp"
#include "parallel/graph_partition.hpp"

#define DEFAULT_NUM_GENERAL_TCP_CONNECTIONS 8 //This was in Global.h but I moved to here.
#define PER_THREAD_BUFF_SIZE (840*1024)
#define DEFAULT_NIO_BUFFER_SIZE (11*105*1024)
#define DEFAULT_NIO_THREADS 24


enum RequestType {
    //TODO: change this to our request type.
    
	// AdjListBatchIo = 3,
	// AdjListBatchIoData = 4,
	// UserCallback = 5,
	// UserCallbackData = 6,
	// OutputVectorRead = 7,
	// InputVectorRead = 8,
	// OutputVectorWriteMessage = 10,
	// NewAdjListBatchIo = 11,
	// NewAdjListBatchIoData = 12,
	// SequentialVectorRead = 15,
	PartitionedExtentReadMessage = 16,
	Exit = 99999999,

};

class Request {
  public:
	Request() {}

	RequestType rt;
	int from;
	int to;
    int dummmmm;
};

class PartitionedExtentReadRequest: public Request {
	public: 
	int64_t size; //Size of Extent + metadata.
};

class Respond {
  public:
	RequestType rt;
	int from;
	int to;
};

class ExitRequest: public Request {
  public:
	int64_t dummy1;
};


void* TurboMalloc(int64_t sz);
void TurboFree(void* ptr, int64_t sz);


class MaterializedAdjacencyLists;
//use tag number 1 when sending request
class RequestRespond {
  public:
	static RequestRespond rr_;

	ThreadPool<int> async_req_pool;
	ThreadPool<int> async_resp_pool;
	ThreadPool<bool> async_udf_pool;
	// per_thread_lazy<diskaio::DiskAioInterface**> per_thread_aio_interface;

	static int64_t total_request_buffer_size;
	static int64_t max_request_size;
	static int64_t num_request_buffer;
	static char* request_buffer;

	static int64_t total_data_buffer_size;
	static int64_t small_data_size;
	static int64_t large_data_size;
	static int64_t num_small_data_buffer;
	static int64_t num_large_data_buffer;
	static char* data_buffer;

	// static std::atomic<int64_t> matadj_hit_bytes;
	// static std::atomic<int64_t> matadj_miss_bytes;

	static turbo_tcp server_sockets;
	static turbo_tcp client_sockets;
	static turbo_tcp general_server_sockets[DEFAULT_NUM_GENERAL_TCP_CONNECTIONS];
	static turbo_tcp general_client_sockets[DEFAULT_NUM_GENERAL_TCP_CONNECTIONS];
	static std::vector<bool> general_connection_pool;

	static VariableSizedMemoryAllocatorWithCircularBuffer circular_temp_data_buffer;
	static VariableSizedMemoryAllocatorWithCircularBuffer circular_stream_send_buffer;
	static VariableSizedMemoryAllocatorWithCircularBuffer circular_stream_recv_buffer;
	
	static int64_t core_counts_;
    static __thread int64_t my_core_id_;

	RequestRespond() {}
	~RequestRespond() {}

	static void Initialize(int64_t trbs, int64_t mrs, int64_t tdbs, int64_t mds, int64_t nio_threads, int64_t udf_threads) {
        rr_.async_req_pool.SpawnThreadsToCpuSet(nio_threads, udf_threads, NumaHelper::num_cores() - 1);
		rr_.async_resp_pool.SpawnThreadsToCpuSet(nio_threads, udf_threads, NumaHelper::num_cores() - 1);
        rr_.async_udf_pool.SpawnThreads(udf_threads, 0, udf_threads - 1);

		mutex_for_end.lock();
		end = false;
		#pragma omp parallel num_threads(core_id::core_counts())
		{
			per_thread_container_.get(omp_get_thread_num()).capacity = 1024;
			per_thread_container_.get(omp_get_thread_num()).data = new char[1024];
		}
		mutex_for_end.unlock();

		total_request_buffer_size = trbs;
		max_request_size = mrs;
		num_request_buffer = trbs / mrs;
		request_buffer = new char[total_request_buffer_size];
		std::memset ((void*) request_buffer, 0, total_request_buffer_size);

		#pragma omp parallel for
		for (int64_t i = 0; i < num_request_buffer; i++) {
			char* tmp = request_buffer + max_request_size * i;
			req_buffer_queue_.enqueue(tmp);
		}

		total_data_buffer_size = tdbs;
		data_buffer = (char*) NumaHelper::alloc_numa_interleaved_memory(total_data_buffer_size);

		small_data_size = mds;
		large_data_size = 2 * small_data_size;
		num_small_data_buffer = (50 * tdbs) / (100 * small_data_size);
		num_large_data_buffer = (50 * tdbs) / (100 * large_data_size);

#pragma omp parallel for
        for (int64_t i = 0; i < num_small_data_buffer; i++) {
            char* tmp = data_buffer + small_data_size * i;
            std::memset ((void*) tmp, 0, small_data_size);
            SimpleContainer cont;
            cont.data = tmp;
            cont.capacity = small_data_size;
            small_data_buffer_queue_.enqueue(cont);
        }

#pragma omp parallel for
		for (int64_t i = 0; i < num_large_data_buffer; i++) {
			char* tmp = data_buffer + small_data_size * num_small_data_buffer + large_data_size * i;
			std::memset ((void*) tmp, 0, large_data_size);
			SimpleContainer cont;
			cont.data = tmp;
			cont.capacity = large_data_size;
			large_data_buffer_queue_.enqueue(cont);
		}
		max_dynamic_buffer_size = 1 * 1024 * 1024 * 1024L; // by default 1GB
		max_dynamic_recv_buffer_size = max_dynamic_buffer_size / 2;
		max_dynamic_send_buffer_size = max_dynamic_buffer_size / 2;

        InitializePerThreadBufferQueue();

		LocalStatistics::register_mem_alloc_info("RequestResond", (trbs + tdbs + per_thread_buffer_size_) / (1024 * 1024L));

        turbo_tcp::establish_all_connections(&server_sockets, &client_sockets);
        general_connection_pool.resize(DEFAULT_NUM_GENERAL_TCP_CONNECTIONS + 1, true);
        general_connection_pool[0] = false;
        general_connection_pool[1] = false;
        for (int i = 0; i < DEFAULT_NUM_GENERAL_TCP_CONNECTIONS; i++) {
            turbo_tcp::establish_all_connections(&general_server_sockets[i], &general_client_sockets[i]);
        }
    }

	static void Close() {
		D_ASSERT (bytes_dynamically_allocated.load() == 0);
		D_ASSERT (recv_buffer_bytes_dynamically_allocated.load() == 0);
		D_ASSERT (send_buffer_bytes_dynamically_allocated.load() == 0);
		//D_ASSERT (small_data_buffer_queue_[0].size_approx() == num_small_data_buffer / 2);
		//D_ASSERT (small_data_buffer_queue_[1].size_approx() == num_small_data_buffer / 2);
		D_ASSERT (small_data_buffer_queue_.size_approx() == num_small_data_buffer);
		D_ASSERT (large_data_buffer_queue_.size_approx() == num_large_data_buffer);

		rr_.async_req_pool.Close();
		rr_.async_resp_pool.Close();
		rr_.async_udf_pool.Close();

		TwoLevelBitMap<node_t>* tmp = nullptr;
        for (int queue_idx = 0; queue_idx < 2; queue_idx++) {
            while (temp_bitmap_queue_[queue_idx].try_dequeue(tmp)) {
                D_ASSERT(tmp != nullptr);
                delete tmp;
                tmp = nullptr;
            }
        }
		#pragma omp parallel num_threads(core_id::core_counts())
		{
			delete[] per_thread_container_.get(omp_get_thread_num()).data;
			per_thread_container_.get(omp_get_thread_num()).data = nullptr;
		}

		delete[] request_buffer;
		NumaHelper::free_numa_memory(data_buffer, total_data_buffer_size);
		LocalStatistics::register_mem_alloc_info("RequestResond", 0);

        D_ASSERT(circular_temp_data_buffer.GetSize() == circular_temp_data_buffer.GetRemainingBytes());
        D_ASSERT(circular_stream_send_buffer.GetSize() == circular_stream_send_buffer.GetRemainingBytes());
        D_ASSERT(circular_stream_recv_buffer.GetSize() == circular_stream_recv_buffer.GetRemainingBytes());

		circular_temp_data_buffer.Close(true);
		circular_stream_send_buffer.Close(true);
		circular_stream_recv_buffer.Close(true);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularTempDataBuffer)", 0);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularSendBuffer)", 0);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularRecvBuffer)", 0);


        char* tmp_buf;
        int64_t num_entries_in_per_thread_buffer_queue = per_thread_buffer_size_ / PER_THREAD_BUFF_SIZE;
		while (per_thread_buffer_queue_.try_dequeue(tmp_buf)) {
            --num_entries_in_per_thread_buffer_queue;
        }
        D_ASSERT(num_entries_in_per_thread_buffer_queue == 0);
		NumaHelper::free_numa_memory(per_thread_buffer_, per_thread_buffer_size_);

		client_sockets.close_socket();
		server_sockets.close_socket();
        for (int i = 0; i < DEFAULT_NUM_GENERAL_TCP_CONNECTIONS; i++) {
            general_client_sockets[i].close_socket();
            general_server_sockets[i].close_socket();
        }
	}

    static void ReceiveRequest();

	static void InitializeCoreIds();
	// static void InitializeIoInterface();
    static void InitializeTempBitMapQueue(int64_t num_entries, int64_t num_buffers_for_sender, int64_t num_buffers_for_receiver);

	template<typename RequestType>
	static void SendRequestAsynchronously(int partition_id, RequestType req_buffer);

	template<typename RequestType>
	static void SendRequest(int partition_id, RequestType req_buffer);

	template<typename RequestType>
	static void SendRequest(int partition_id, RequestType* req, int num_reqs = 1);

	template<typename RequestType, typename ObjectType, typename FcnType>
	static void SendRequest(RequestType* req, int num_reqs, bool wait, ObjectType* obj, FcnType* fcn);

	template<typename Request>
	static void SendRequest(Request* req, int num_reqs, char* data_buffer[], int64_t data_size[], int64_t num_data_buffers);
	
    // template<typename Request>
	// static void SendRequestViaTcp(Request* req, int num_reqs, char* data_buffer, int64_t data_size);

	template<typename Request>
	static void SendRequestReturnContainer(Request req, char* data_buffer, int64_t data_size, SimpleContainer cont_to_return);

    static void SendSynchronously(int partition_id, char* buf, int64_t sz, int tag);

	static void SendData(char* buf, int64_t count, int partition_id, int tag, SimpleContainer cont, bool return_container);
	static void SendDataInBuffer(tbb::concurrent_queue<SimpleContainer>* req_buffer_queue, turbo_tcp* vector_client_sockets, int partition_id, int tag, int chunk_idx, int lv, int64_t eom_cnt, bool delete_req_buffer_queue = true);
	// static void SendDataInBufferWithUdfRequest(tbb::concurrent_queue<UdfSendRequest>* req_buffer_queue, tbb::concurrent_queue<UdfSendRequest>* stoped_req_buffer_queue,int to, Range<node_t> vid_range, BitMap<node_t>* vids, int tag, int chunk_idx, int lv, int64_t MaxBytesToSend, int64_t* stop_flag);
    // template <typename T, Op op>
    // static void TurboAllReduceSync(char* buf, int count, int size_in_bits);
    // template <typename T, Op op>
    // static void TurboAllReduceInPlaceAsync(char* buf, int count, int size_in_bytes, int queue_idx=0);
    // template <typename T, Op op>
    // static void TurboAllReduceInPlace(turbo_tcp* server_socket, turbo_tcp* client_socket, int socket_idx, char* buf, int count, int size_in_bits);

    // static void TurboBarrier(turbo_tcp* server_socket, turbo_tcp* client_socket);
    // static void Barrier();
    // static void WaitAllreduce();
    // static void PopAllreduce();

	template <typename RequestType>
	static void Respond(RequestType* req, int num_reqs = 1);
	
	static void PartitionedExtentRead(int32_t size, int from);

	// static void RespondSequentialVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv);
	// static void RespondInputVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv);
	// static void RespondOutputVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv);
	// static void RespondOutputVectorWriteMessage(int32_t vectorID, int64_t fromChunkID, int64_t chunkID, int tid, int idx, int from, int64_t send_num, int64_t combined, int lv);
	// static void RespondAdjListBatchIO(char* buf);
	// static void RespondAdjListBatchIoPartialList(char* buf);
	// static void RespondAdjListBatchIoFullList(AdjListRequestBatch* req, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, Range<int> version_range, EdgeType e_type, DynamicDBType d_type=INSERT);
	// static void RespondAdjListBatchIoFullListFromCache(int win_lv, AdjListRequestBatch* req, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, int64_t& bytes_served_from_memory, node_t& total_num_vids_cnts, node_t& num_vertices_served_from_memory, Range<int> version_range);
    // static void RespondAdjListBatchIoFullListParallel(AdjListRequestBatch* req, Range<node_t> vid_range, Range<node_t> bitmap_vid_range, TwoLevelBitMap<node_t>* requested_vertices, TwoLevelBitMap<node_t>* vertices_from_cache, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);
	// static int64_t ComputeMaterializedAdjListsSize(Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, int64_t MaxBytesToSend);
    // static void FindPageIdsToReadFrom(Range<int> version_range, Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, bool is_initialized, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, Range<node_t> ProcessedVertices);
    // static void FindFullListPageIdsToReadFrom(Range<int> version_range, Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, bool is_initialized, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, Range<node_t> ProcessedVertices);
    // static void CopyMaterializedAdjListsFromCache(MaterializedAdjacencyLists& mat_adj, SimpleContainer& cont, int64_t adjlist_data_size_cache, int64_t slot_data_size_cache, FixedSizeVector<node_t>& vertices, Range<node_t> vid_range, int64_t size_to_request_cache);

	// static void MatAdjlistSanitycheck(char* data_buffer, int64_t data_size, int64_t capacity, int tag);

	// static void GetMatAdjBufferHitMissBytes(int64_t& hit, int64_t& miss) {
	// 	hit = matadj_hit_bytes.load();
	// 	miss = matadj_miss_bytes.load();
    //     return;
	// }

	static void EndRunning() {
		ExitRequest req;
		req.from = PartitionStatistics::my_machine_id();
		req.to = PartitionStatistics::my_machine_id();
		req.rt = RequestType::Exit;

		SendRequest(req.to, req);

		mutex_for_end.lock();
		end = true;
		mutex_for_end.unlock();
	}

	static void WaitRequestRespond(bool need_barrier) {
	    turbo_timer tim;
    
        tim.start_timer(0);
        tim.stop_timer(0);
        tim.start_timer(1);
        rr_.async_req_pool.WaitAll();
        tim.stop_timer(1);

        tim.start_timer(2);
        if (need_barrier)
			PartitionStatistics::wait_for_all();	// BARRIER, 불필요한 경우 안하도록 처리해야함
        tim.stop_timer(2);
		
        tim.start_timer(3);
		rr_.async_resp_pool.WaitAll();
        tim.stop_timer(3);
	
#ifdef REPORT_PROFILING_TIMERS
        fprintf(stdout, "[%ld] (%ld, %ld) WaitRequestRespond\t%.4f\t%.4f\t%.4f\t%.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3));
#endif
    }

    // static int64_t WaitMyPendingDiskIO(diskaio::DiskAioInterface* my_io) {
    //     D_ASSERT(my_io != NULL);
    //     int num_to_complete = my_io->GetNumOngoing();
	// 	int backoff = 1;
	// 	while (my_io->GetNumOngoing() > 0) {
	// 		usleep (backoff * 1024);
	// 		my_io->WaitForResponses(0);
	// 		if (backoff <= 16 * 1024) backoff *= 2;
	// 	}
    //     D_ASSERT(my_io->GetNumOngoing() == 0);
	// 	return num_to_complete;
	// }

	// static int64_t WaitMyPendingDiskIO(diskaio::DiskAioInterface** my_io = GetMyDiskIoInterface()) {
    //     D_ASSERT(my_io != NULL);
    //     int num_total_to_complete = 0;
    //     std::list<IOMode> io_modes = { WRITE_IO, READ_IO };
    //     for (auto& io_mode: io_modes) {
    //         int num_to_complete = my_io[io_mode]->GetNumOngoing();
    //         int backoff = 1;
    //         while (my_io[io_mode]->GetNumOngoing() > 0) {
    //             usleep (backoff * 1024);
    //             my_io[io_mode]->WaitForResponses(0);
    //             if (backoff <= 16 * 1024) backoff *= 2;
    //         }
    //         num_total_to_complete += num_to_complete;
    //     }
	// 	return num_total_to_complete;
	// }

    static void InitializePerThreadBufferQueue() {
        //per_thread_buffer_size_ = 8 * 1024 * 1024 * 1024L;
        per_thread_buffer_size_ = 2 * 1024 * 1024 * 1024L;
        //per_thread_buffer_size_ = 1 * 1024 * 1024 * 1024L;
        int64_t total_nums = per_thread_buffer_size_ / PER_THREAD_BUFF_SIZE;
        
        per_thread_buffer_ = (char*) NumaHelper::alloc_numa_interleaved_memory(per_thread_buffer_size_);
        for (int64_t i = 0; i < total_nums; i++) {
			char* tmp = per_thread_buffer_ + PER_THREAD_BUFF_SIZE * i;
			std::memset ((void*) tmp, 0, PER_THREAD_BUFF_SIZE);
            ReturnPerThreadBuffer(tmp);
        }
    }
    static double GetRemainingBuffersInPerThreadBufferInPortion() {
        double portion = (double) per_thread_buffer_queue_.size_approx() / ((double) per_thread_buffer_size_ / PER_THREAD_BUFF_SIZE);

        if (portion < 0.1f) {
            //fprintf(stdout, "remaining buffer portion = %.4f\n", portion);
        }
        return portion;
    }
    static int64_t GetRemainingBuffersInPerThreadBuffer() {
        return per_thread_buffer_queue_.size_approx();
    }
    static void ReturnPerThreadBuffer(char* buf) {
        per_thread_buffer_queue_.enqueue(buf);
    }
    static char* GetPerThreadBuffer(const std::function<void()>& fcn_to_call_when_waiting = {}) {
        char* buf = NULL;
        int backoff = 1;
        while (!per_thread_buffer_queue_.try_dequeue(buf)) {
            if (fcn_to_call_when_waiting) {
                fcn_to_call_when_waiting();
            }
            usleep(backoff * 1);
            backoff= 2 * backoff;
            if (backoff >= 1024) backoff = 1024;
        }
        D_ASSERT(buf != NULL);
        return buf;
    }

	// static diskaio::DiskAioInterface**& GetMyDiskIoInterface();
	// static diskaio::DiskAioInterface*& GetMyDiskIoInterface(IOMode mode);

	static char* GetTempReqReceiveBuffer();
	static SimpleContainer GetTempDataBuffer(int64_t req_size);
	static TwoLevelBitMap<node_t>* GetTempNodeBitMap(bool sender_or_receiver, int64_t backoff_limit = -1);
	static void ReturnTempReqReceiveBuffer(char* tmp_buffer);
	static void ReturnTempDataBuffer(SimpleContainer cont);
	static void ReturnTempNodeBitMap(bool sender_or_receiver, TwoLevelBitMap<node_t>* tmp_bitmap);

	static std::mutex mutex_for_end;
	static bool end; // use mutex_for_end when using this

	static per_thread_lazy<SimpleContainer> per_thread_container_;
	static moodycamel::ConcurrentQueue<char*> req_buffer_queue_;
	static moodycamel::ConcurrentQueue<SimpleContainer> small_data_buffer_queue_;
	static moodycamel::ConcurrentQueue<SimpleContainer> large_data_buffer_queue_;
	static moodycamel::ConcurrentQueue<TwoLevelBitMap<node_t>*> temp_bitmap_queue_[2];
    static std::queue<std::future<void>> reqs_to_wait_allreduce[2];
	
    static moodycamel::ConcurrentQueue<char*> per_thread_buffer_queue_;
    static char* per_thread_buffer_;
    static int64_t per_thread_buffer_size_;

	static int64_t max_permanent_buffer_size;
	static std::atomic<int64_t> bytes_permanently_allocated;

	static int64_t max_dynamic_buffer_size;
	static std::atomic<int64_t> bytes_dynamically_allocated;

	static int64_t max_dynamic_recv_buffer_size;
	static std::atomic<int64_t> recv_buffer_bytes_dynamically_allocated;
	static int64_t max_dynamic_send_buffer_size;
	static std::atomic<int64_t> send_buffer_bytes_dynamically_allocated;
    
	static int64_t GetMaxPermanentBufferSize () {
		return RequestRespond::max_permanent_buffer_size;
	}
	static void SetMaxPermanentBufferSize (int64_t max) {
		RequestRespond::max_permanent_buffer_size = max;
	}
	static int64_t GetMaxDynamicBufferSize () {
		return RequestRespond::max_dynamic_buffer_size;
	}
	static void SetMaxDynamicBufferSize (int64_t max) {
		RequestRespond::max_dynamic_buffer_size = max;
		circular_temp_data_buffer.Initialize(max);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularTempDataBuffer)", max / (1024 * 1024L));
	}
	static int64_t GetMaxDynamicRecvBufferSize () {
		return RequestRespond::max_dynamic_recv_buffer_size;
	}
	static void SetMaxDynamicRecvBufferSize (int64_t max) {
		RequestRespond::max_dynamic_recv_buffer_size = max;
		circular_stream_recv_buffer.Initialize(max);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularRecvBuffer)", max / (1024 * 1024L));
	}
	static int64_t GetMaxDynamicSendBufferSize () {
		return RequestRespond::max_dynamic_send_buffer_size;
	}
	static void SetMaxDynamicSendBufferSize (int64_t max) {
		RequestRespond::max_dynamic_send_buffer_size = max;
		circular_stream_send_buffer.Initialize(max);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularSendBuffer)", max / (1024 * 1024L));
	}

	static SimpleContainer GetDataSendBuffer (int64_t req_buf_size);
	static SimpleContainer GetDataRecvBuffer (int64_t req_buf_size);
	static void ReturnDataSendBuffer (SimpleContainer cont);
	static void ReturnDataRecvBuffer (SimpleContainer cont);

  public:

	// inline virtual void CallbackTask(diskaio::DiskAioRequestUserInfo &user_info) {
	// 	D_ASSERT (user_info.do_user_cb);
	// }

	// static void UserCallbackSanityCheck(char* data_buffer, int64_t data_size, RequestType rt);
};





#endif
