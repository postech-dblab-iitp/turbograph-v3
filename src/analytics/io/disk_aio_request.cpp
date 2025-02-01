#include "analytics/io/disk_aio/disk_aio_interface.hpp"
#include "analytics/io/disk_aio/disk_aio_request.hpp"

using namespace diskaio;

/*
* Push the completed aio request to the complete_queue_ of the aio interface
*/
int DiskAioRequest::Complete() {
	DiskAioInterface* interface = (DiskAioInterface*) cb.data;
	DiskAioRequest* itself = (DiskAioRequest*) this;
	int ret = interface->complete_queue_.push(&itself, 1);
	return ret;
}
