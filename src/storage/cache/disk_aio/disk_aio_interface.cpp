#include "storage/cache/disk_aio/disk_aio_interface.hpp"
#include "storage/cache/disk_aio/disk_aio.hpp"
#include <thread>

namespace diskaio
{

void DiskAioInterface::Register(DiskAioThread* disk_aio_thread) {
	disk_aio_thread_ = disk_aio_thread;
	disk_aio_thread_->RegisterInterface((void*) this);
}

int DiskAioInterface::ProcessResponses() {
    int n = complete_queue_.fetch(reqs_, max_num_ongoing_);
	// fprintf(stdout, "ProcessResponses %d\n", n);
    if (n == 0) { std::this_thread::yield(); } 
    for (int i = 0; i < n; ++i) {
        a_callback_t func = (a_callback_t) reqs_[i]->GetFunc();
	    //assert(func || func_); //Possible?
		if (func) {
			func(reqs_[i]);
			continue;
		}
		if (func_) {
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

DiskAioRequest* DiskAioInterface::PackRequest(
		int fid, off_t offset, ssize_t iosize, char* buffer, int io_type) 
{
	DiskAioRequest* req;
	request_allocator_.fetch(&req, 1);
	int fd = ((DiskAio *)system_)->Getfd(fid);
	if (fd < 0) assert(false);

	if (io_type == DISK_AIO_READ) {
		// fprintf(stdout, "io_prep_pread %p %d %ld %ld\n", buffer, fd, iosize, offset);
		io_prep_pread(&(req->cb), fd, buffer, iosize, offset);
	} else {
		// fprintf(stdout, "io_prep_pwrite %p %d %ld %ld, %p %d %d\n",
		// 	buffer, fd, iosize, offset, req->cb.data, req->cb.key, req->cb.aio_fildes);
		io_prep_pwrite(&(req->cb), fd, buffer, iosize, offset);
	}

	req->cb.data = (void*) this;
	return req;
}

bool DiskAioInterface::Request(
		int fid, off_t offset, ssize_t iosize, 
		char* buffer, int io_type, void* func) 
{
	if (!disk_aio_thread_)
		return false;
	while (request_allocator_.num_entries() == 0) {
		ProcessResponses();
	}
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

bool DiskAioInterface::Request(
		int fid, off_t offset, ssize_t iosize, 
		char* buffer, int io_type, struct DiskAioRequestUserInfo& user_info) 
{
	if (!disk_aio_thread_)
		return false;
	while (request_allocator_.num_entries() == 0) {
		ProcessResponses();
	}
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

int DiskAioInterface::GetNumOngoing() {
	return num_ongoing_;
}

int DiskAioInterface::WaitForResponses() {
	int num_ongoing = num_ongoing_;
	while (num_ongoing_ > 0) {
		ProcessResponses();
	}
	return num_ongoing;
}

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
