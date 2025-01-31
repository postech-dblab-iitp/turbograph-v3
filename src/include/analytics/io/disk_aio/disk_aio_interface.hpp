#ifndef __DISK_AIO_INTERFACE_H__
#define __DISK_AIO_INTERFACE_H__

/*
* Design of a disk asynchronous I/O interface
*
* This is an interface for a disk aio system
*
* It has two queues: request_queue and complete_queue
*
* For an asynchronous I/O request,
*   1) A thread t_a pushes an aio request to the request_queue of the interface
*   2) An aio thread t_b in the aio system pops the aio request from the request_queue
*   3) After I/O, t_b pushes the completed aio request to the complete_queue
*   4) The thread t_a pops the completed aio request and execute the callback function*
*
*  aio interfaces and aio threads have a N:1 mapping relationship
*/



#include <string>
#include <iostream>
#include <sys/param.h>
#include <fcntl.h>
#include <stdlib.h>
#include <system_error>
#include <assert.h>

#include "disk_aio_thread.hpp"
#include "disk_aio_request.hpp"
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

static const int PAGE_SIZE = 4096; /* It assumes that page size is 4KB */
#ifndef ROUNDUP_PAGE
#define ROUNDUP_PAGE(off) (((long) off + PAGE_SIZE - 1) & (~((long) PAGE_SIZE - 1)))
#endif

typedef void (*a_callback_t) (DiskAioRequest* req);

class DiskAioInterface
{
public:
	DiskAioQueue<DiskAioRequest*> request_allocator_; /* an allocator for aio requests */
	DiskAioQueue<DiskAioRequest*> complete_queue_; /* a queue for completed aio requests */
	DiskAioQueue<DiskAioRequest*> request_queue_; /* a queue for aio requests */

	int max_num_ongoing_; /* the maximum number of aio requests that can be processed at the same time */
	int num_ongoing_; /* the number of aio requests */
	int num_write_ongoing_; /* the number of aio rwrite requests */
	int reqs_pointer;
	void* func_; /* a default callback function */
	void* system_; /* a pointer to the aio file system */

	DiskAioThread* disk_aio_thread_; /* a pointer to a thread that executes asynchronous I/O */
	DiskAioRequest** reqs_; /* pointers to aio requests */
	DiskAioRequest* reqs_data_; /* pointer to a memory space for aio requests */

	/*
	* Construct the interface 
	*/
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
		/* Pre-allocate the aio requests */
		reqs_ = new DiskAioRequest*[max_num_ongoing];
		reqs_data_ = new DiskAioRequest[max_num_ongoing];
		for (int i = 0; i < max_num_ongoing_; ++i) {
			DiskAioRequest* req = reqs_data_ + i;
			request_allocator_.push(&req, 1);
		}
		if (disk_aio_thread_)
			disk_aio_thread_->RegisterInterface((void*)this);
	}

	/*
	* Deconstruct the interace
	*/
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
