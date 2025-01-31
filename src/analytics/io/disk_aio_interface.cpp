#include "disk_aio_interface.hpp"
#include "disk_aio.hpp"
#include <thread>

namespace diskaio
{

/*
* Register the interface to the aio thread
*/
void DiskAioInterface::Register(DiskAioThread* disk_aio_thread) {
	disk_aio_thread_ = disk_aio_thread;
	disk_aio_thread_->RegisterInterface((void*) this);
}

/*
* Process the callback function of completed aio requests
*/
int DiskAioInterface::ProcessResponses() {
	/* Fetch completed aio requests from complete_queue_ */
    int n = complete_queue_.fetch(reqs_, max_num_ongoing_);
    if (n == 0) {
		/* 
		* If there was no completed aio request, 
		* yield this thread to do not waste computing resources
		*/
		std::this_thread::yield(); 
	} 
    for (int i = 0; i < n; ++i) {
        a_callback_t func = (a_callback_t) reqs_[i]->GetFunc();
	    assert(func || func_);
		if (func) {
			/* Process the callback function of the completed aio request */
			func(reqs_[i]);
			continue;
		}
		if (func_) {
			/* Process the default callback function */
			func = (a_callback_t) func_;
			func (reqs_[i]);
			continue;
		}
    }
    if (n > 0) {
        request_allocator_.push(reqs_, n);
        num_ongoing_ -= n;
    }

    return n;
}

/*
* Allocate an aio request and set the necessary information of the request
*/
DiskAioRequest* DiskAioInterface::PackRequest(
		int fid, off_t offset, ssize_t iosize, char* buffer, int io_type) 
{
	DiskAioRequest* req;
	request_allocator_.fetch(&req, 1);
	int fd = ((DiskAio *)system_)->Getfd(fid);
	if (fd < 0) assert(false);

	if (io_type == DISK_AIO_READ) io_prep_pread(&(req->cb), fd, buffer, iosize, offset);
    else io_prep_pwrite(&(req->cb), fd, buffer, iosize, offset);

	req->cb.data = (void*) this;
	return req;
}

/*
* Request an aio request
*/
bool DiskAioInterface::Request(
		int fid, off_t offset, ssize_t iosize, 
		char* buffer, int io_type, void* func) 
{
	if (!disk_aio_thread_) return false;

	/* If there is no free aio request, then process completed aio requests */
	while (request_allocator_.num_entries() == 0) {
		ProcessResponses();
	}
	/* Prepare an aio request and push it to the request_queue */
	DiskAioRequest* req = PackRequest(fid, offset, iosize, buffer, io_type);
	req->SetFunc(func);
	request_queue_.push(&req, 1);
	disk_aio_thread_->activate();
	num_ongoing_++;
	if(req->user_info.do_user_only_req_cb){
		num_write_ongoing_++;
	}
	return true;
}

/*
* Request an aio request
*/
bool DiskAioInterface::Request(
		int fid, off_t offset, ssize_t iosize, 
		char* buffer, int io_type, struct DiskAioRequestUserInfo& user_info) 
{
	if (!disk_aio_thread_) return false;

	/* If there is no free aio request, then process completed aio requests */
	while (request_allocator_.num_entries() == 0) {
		ProcessResponses();
	}
	/* Prepare an aio request and push it to the request_queue */
	DiskAioRequest* req = PackRequest(fid, offset, iosize, buffer, io_type);
	req->SetUserInfo(user_info);
	request_queue_.push(&req, 1);
	disk_aio_thread_->activate();
	num_ongoing_++;
	if(req->user_info.do_user_only_req_cb){
		num_write_ongoing_++;
	}
	return true;
}

/*
* Get the number of ongoing aio requests
*/
int DiskAioInterface::GetNumOngoing() {
	return num_ongoing_;
}

/*
* Process completed aio requests until there is no ongoing aio request
*/
int DiskAioInterface::WaitForResponses() {
	int num_ongoing = num_ongoing_;
	while (num_ongoing_ > 0) {
		ProcessResponses();
	}
	return num_ongoing;
}

/*
* Process the given number of completed aio requests
*/
int DiskAioInterface::WaitForResponses(int num) {
	int num_ongoing = num_ongoing_;
	int min = num < num_ongoing ? num : num_ongoing;
	int completed;
	do {
		ProcessResponses();
		completed = num_ongoing - num_ongoing_;
	} while (completed < min);
	return completed;
}

}
