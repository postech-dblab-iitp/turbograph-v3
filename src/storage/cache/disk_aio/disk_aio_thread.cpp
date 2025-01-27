#include "storage/cache/disk_aio/disk_aio_thread.hpp"
#include "storage/cache/disk_aio/disk_aio_interface.hpp"
#include <chrono>

namespace diskaio
{

void DiskAioThread::RegisterInterface(void* interface) {
	interfaces_.insert(interface);
}

int DiskAioThread::FetchRequests(int num) {
	if (num == 0) return 0;
	int num_search = interfaces_.size();
	int remain = num;
	DiskAioRequest** requests = requests_;
	while (remain > 0 && num_search > 0) { 
		if (hand_ == interfaces_.end() && (hand_ = interfaces_.begin()) == interfaces_.end())
			break;
		DiskAioInterface* interface = (DiskAioInterface*) (*hand_);
		hand_++;
		int n = interface->request_queue_.fetch(requests, remain);
		requests += n; remain -= n; num_search--;
	} while (remain > 0 && num_search > 0);
	return num - remain;
}

void DiskAioThread::SubmitToKernel(int num) {
	int rc = io_submit(ctx_, num, (struct iocb**) requests_);
	if (rc < 0) assert (false);
}

int DiskAioThread::WaitKernel(struct timespec* to, int num) {
	struct io_event* ep = events_;
	int ret;
	do {
		ret = io_getevents(ctx_, num, max_num_ongoing_, ep, to);
	} while (ret == -EINTR);

	// fprintf(stdout, "io_getevents %d, %p, %ld, %lu, %p\n",
	// 	ret, ep->data, ep->res, ep->res2, ep->obj->data);
	if (((long long)(ep->res2)) < 0) assert(false);

	if (ret < 0) assert(false);

	for (int i = 0; i < ret; ep++, i++) {
		DiskAioRequest* req = (DiskAioRequest*) ep->obj;
		buf_queue_.push(&req, 1);
	}
	return ret;
}

int DiskAioThread::Complete(int num) {
	int min = buf_queue_.num_entries();
	min = min < num ? min : num;
	for (int i = 0; i < min; ++i) {
		DiskAioRequest* req;
		buf_queue_.fetch(&req, 1);
		if (req->cb.aio_lio_opcode == IO_CMD_PREAD) {
			stats_.num_reads++;
			stats_.num_read_bytes += req->cb.u.c.nbytes;
			// fprintf(stdout, "num_reads %ld, read_bytes %ld\n", stats_.num_reads, stats_.num_read_bytes);
		} else {
			stats_.num_writes++;
			stats_.num_write_bytes += req->cb.u.c.nbytes;
			// fprintf(stdout, "num_writes %ld, num_write_bytes %ld\n", stats_.num_writes, stats_.num_write_bytes);
		}
		//assert (n == 1);
		int c = req->Complete();
		if (c <= 0) assert(false);
	}
	return min;
}

void DiskAioThread::run() {
	int available = max_num_ongoing_ - num_ongoing_;
	int fetched = FetchRequests(available);
	struct timespec tspec;
	tspec.tv_sec = tspec.tv_nsec = 0;
	while (num_ongoing_ > 0 || fetched > 0) {
		if (fetched > 0) {
			// fprintf(stdout, "fetched request %d\n", fetched);
			SubmitToKernel(fetched);
			num_ongoing_ += fetched;
		}
		if (num_ongoing_ > 0) {
			// fprintf(stdout, "num_ongoing_ %d, max_num_ongoing_ %d\n", num_ongoing_, max_num_ongoing_);
			if (max_num_ongoing_ == num_ongoing_) { WaitKernel(NULL, 1); }
			else { WaitKernel(&tspec, 0); }
		}
		num_ongoing_ -= Complete(max_num_ongoing_);
		available = max_num_ongoing_ - num_ongoing_;
		fetched = FetchRequests(available);
	}
}

}

