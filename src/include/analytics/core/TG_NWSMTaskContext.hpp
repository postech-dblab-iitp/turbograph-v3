#pragma once

#include "TypeDef.hpp"
#include "atom.hpp"

#include "concurrentqueue/concurrentqueue.h"

class TG_NWSMTaskContext {
  public:
	typedef moodycamel::ConcurrentQueue<PageID> task_page_queue_t;

	TG_NWSMTaskContext() {
		overflow = 0;
		num_threads = 0;
	}

	volatile bool is_valid;
	volatile bool DONE;
	bool IO_DONE;
	bool has_been_scheduled;

	int64_t master_thread_id;
	int64_t master_socket_id;

	int64_t task_id;
	int64_t version_id;
	DynamicDBType db_type;

	int64_t edge_chunk_src_id;
	int64_t edge_chunk_dst_id;

	int64_t edge_dst_subchunk_id;

	int64_t num_threads;
	int64_t overflow;

	task_page_queue_t* task_page_queue;
	task_page_queue_t* processed_page_queue;

	PageID num_pages_to_process;
	PageID num_pages_processed;

	PageID start_page_id;
	PageID last_page_id;


	Range<node_t> dst_vid_range;

	CACHE_PADOUT;
};
