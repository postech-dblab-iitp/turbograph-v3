#include "disk_aio_interface.hpp"
#include "disk_aio_request.hpp"

using namespace diskaio;

int DiskAioRequest::Complete() {
	DiskAioInterface* interface = (DiskAioInterface*) cb.data;
	DiskAioRequest* itself = (DiskAioRequest*) this;
	int ret = interface->complete_queue_.push(&itself, 1);
	return ret;
}
