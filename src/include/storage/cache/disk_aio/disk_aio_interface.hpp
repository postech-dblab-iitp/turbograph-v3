#ifndef __DISK_AIO_INTERFACE_H__
#define __DISK_AIO_INTERFACE_H__

#include <string>
#include <iostream>
#include <sys/param.h>
#include <fcntl.h>
#include <stdlib.h>
#include <system_error>
#include <assert.h>

#include "storage/cache/disk_aio/disk_aio_thread.hpp"
#include "storage/cache/disk_aio/disk_aio_request.hpp"
#include "disk_aio_util.hpp"

#ifndef DISK_AIO_READ
#define DISK_AIO_READ 0
#endif
#ifndef DISK_AIO_WRITE
#define DISK_AIO_WRITE 1
#endif
#ifndef DISK_AIO_APPEND
#define DISK_AIO_APPEND 2
#endif

namespace diskaio
{

static const int PAGE_SIZE = 4096;
#ifndef ROUNDUP_PAGE
#define ROUNDUP_PAGE(off) (((long) off + PAGE_SIZE - 1) & (~((long) PAGE_SIZE - 1)))
#endif

typedef void (*a_callback_t) (DiskAioRequest* req);

class DiskAioInterface
{
public:
	DiskAioQueue<DiskAioRequest*> request_allocator_;
	DiskAioQueue<DiskAioRequest*> complete_queue_;
	DiskAioQueue<DiskAioRequest*> request_queue_;

	int max_num_ongoing_;
	int num_ongoing_;
	int num_write_ongoing_;
	int reqs_pointer;
	void* func_;
	void* system_;

	DiskAioThread* disk_aio_thread_;
	DiskAioRequest** reqs_;
	DiskAioRequest* reqs_data_;

	DiskAioInterface(void* system, int max_num_ongoing, DiskAioThread* disk_aio_thread = 0)
		: request_allocator_(max_num_ongoing)
		, complete_queue_(max_num_ongoing)
		, request_queue_(max_num_ongoing)
	{
		system_ = system;
		disk_aio_thread_ = disk_aio_thread;
		max_num_ongoing_ = max_num_ongoing;
		num_ongoing_ = 0;
        num_write_ongoing_ = 0;
		func_ = 0;
        reqs_pointer = 0;
		reqs_ = new DiskAioRequest*[max_num_ongoing];
		reqs_data_ = new DiskAioRequest[max_num_ongoing];
		for (int i = 0; i < max_num_ongoing_; ++i) {
			DiskAioRequest* req = reqs_data_ + i;
			request_allocator_.push(&req, 1);
		}
		if (disk_aio_thread_)
			disk_aio_thread_->RegisterInterface((void*)this);
	}

	~DiskAioInterface() {
		delete reqs_;	
		delete reqs_data_;	
	}

	void Register(DiskAioThread* disk_io_thread);
	int ProcessResponses();
	DiskAioRequest* PackRequest(int fd, off_t offset, ssize_t iosize, char* buf, int io_type);
	bool Request(int fid, off_t offset, ssize_t iosize, char* buf, int io_type, void* func=0);
	bool Request(int fid, off_t offset, ssize_t ioszie, char* buf, int io_type, struct DiskAioRequestUserInfo& user_info);
	int GetNumOngoing();
	int WaitForResponses();
	int WaitForResponses(int num);

	void SetFunc(void* func) {
		func_ = func;
	}
};

}
#endif
