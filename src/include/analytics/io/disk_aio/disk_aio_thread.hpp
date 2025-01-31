#ifndef __DISK_AIO_THREAD_H__
#define __DISK_AIO_THREAD_H__

#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/param.h>
#include <fcntl.h>
#include <stdlib.h>
#include <libaio.h>

#include "thread.h"
#include "disk_aio_request.hpp"
#include "disk_aio_util.hpp"

#include "tbb/concurrent_unordered_set.h"


namespace diskaio
{

struct DiskAioThreadStats {
	size_t num_reads = 0;
	size_t num_read_bytes = 0;
	size_t num_writes = 0;
	size_t num_write_bytes = 0;
};

class DiskAioThread : public thread
{
private:
	int max_num_ongoing_; /* the maximum number of ongoing aio requests*/
	int num_ongoing_; /* the number of ongoing aio requests */
	void* system_; /* a pointer to the aio file system */

	io_context_t ctx_;
	DiskAioQueue<DiskAioRequest*> buf_queue_; /* a queue for the completed aio requests */
	struct io_event* events_;
	DiskAioRequest** requests_; /* pointers to the aio requests */
	tbb::concurrent_unordered_set<void*> interfaces_; /* registerd interfaces */
	tbb::concurrent_unordered_set<void*>::iterator hand_; /* an iterator for the interfaces */

public:

	/*
	* Construct the aio thread
	*/
	DiskAioThread(int id, void* system, int max_num_ongoing)
		: thread(std::string("DiskAioThread"), 0, true)
		, buf_queue_(max_num_ongoing)
	{
		system_ = system;
		max_num_ongoing_ = max_num_ongoing;
		num_ongoing_ = 0;
		hand_ = interfaces_.begin();

		memset(&ctx_, 0, sizeof(ctx_));
		int ret = io_queue_init(max_num_ongoing, &ctx_);
		if (ret < 0) assert(false);

		requests_ = new DiskAioRequest*[max_num_ongoing];
		events_ = new struct io_event[max_num_ongoing];
	}
	
	/*
	* Deconstruct the aio thread
	*/
	~DiskAioThread() {
		delete requests_;
		delete events_;
	}
private:
	int FetchRequests(int num);
	void SubmitToKernel(int num);
	int WaitKernel(struct timespec* to, int num);
	int Complete(int num);
	void run();

	DiskAioThreadStats stats_;

public:
	void RegisterInterface(void* interface);

	/*
	* Return the counters for statistics
	*/
	void GetStats (DiskAioThreadStats &stats) {
		stats = stats_;
	}
	/*
	* Resetthe counters for statistics
	*/
	void ResetStats () {
        stats_.num_reads = 0;
        stats_.num_read_bytes = 0;
        stats_.num_writes = 0;
        stats_.num_write_bytes = 0;
    }

};

}

#endif

