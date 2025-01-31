#include "disk_aio_thread.hpp"
#include "disk_aio_interface.hpp"
#include "timer.hpp"
#include <chrono>

namespace diskaio
{

/*
* Register the given interface
*/
void DiskAioThread::RegisterInterface(void* interface) {
	interfaces_.insert(interface);
}

/*
* Fetch the given number of aio requests from the request queues of registered interfaces in clockwise manner
*/
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

/*
* Submit the aio requests to kernel
*/
void DiskAioThread::SubmitToKernel(int num) {
	int rc = io_submit(ctx_, num, (struct iocb**) requests_);
	if (rc < 0) assert (false);
}

/*
* Wait kernel until it completes the given number of aio requests
*/
int DiskAioThread::WaitKernel(struct timespec* to, int num) {
	struct io_event* ep = events_;
	int ret;
	do {
		ret = io_getevents(ctx_, num, max_num_ongoing_, ep, to);
	} while (ret == -EINTR);

	if (ret < 0) assert(false);
	/* Push completed aio requests to buf_queue_ */
	for (int i = 0; i < ret; ep++, i++) {
		DiskAioRequest* req = (DiskAioRequest*) ep->obj;
		buf_queue_.push(&req, 1);
	}
	return ret;
}

/*
* Post-process the given number of completed aio requests
*/
int DiskAioThread::Complete(int num) {
	int min = buf_queue_.num_entries();
	min = min < num ? min : num;
	for (int i = 0; i < min; ++i) {
		DiskAioRequest* req;
		buf_queue_.fetch(&req, 1);
		/* Update counters */
		if (req->cb.aio_lio_opcode == IO_CMD_PREAD) {
			stats_.num_reads++;
			stats_.num_read_bytes += req->cb.u.c.nbytes;
		} else {
			stats_.num_writes++;
			stats_.num_write_bytes += req->cb.u.c.nbytes;
		}
		//assert (n == 1);
		/* push the completed aio request to the complete_queue_ of the interface */
		int c = req->Complete();
		if (c <= 0) assert(false);
	}
	return min;
}

void DiskAioThread::run() {

    //turbo_timer tim;
	int available = max_num_ongoing_ - num_ongoing_;
    //tim.start_timer(0);
	int fetched = FetchRequests(available);
    //tim.stop_timer(0);
	struct timespec tspec;
	tspec.tv_sec = tspec.tv_nsec = 0;
	while (num_ongoing_ > 0 || fetched > 0) {
        //tim.start_timer(1);
		if (fetched > 0) {
			SubmitToKernel(fetched);
			num_ongoing_ += fetched;
		}
        //tim.stop_timer(1);
        //tim.start_timer(2);
		if (num_ongoing_ > 0) {
			if (max_num_ongoing_ == num_ongoing_) { WaitKernel(NULL, 1); }
			else { WaitKernel(&tspec, 0); }
		}
        //tim.stop_timer(2);
        //tim.start_timer(3);
		num_ongoing_ -= Complete(max_num_ongoing_);
		available = max_num_ongoing_ - num_ongoing_;
		fetched = FetchRequests(available);
        //tim.stop_timer(3);
	}
    //fprintf(stdout, "Dio Thread Run %.4f %.4f %.4f %.4f\n", tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3));
}

}

