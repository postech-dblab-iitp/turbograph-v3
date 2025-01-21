#ifndef __DISK_AIO_REQUEST_H__
#define __DISK_AIO_REQUEST_H__

#include <chrono>
#include <libaio.h>
namespace diskaio
{

struct DiskAioRequestDBInfo {
    int32_t version_id=-1;
    int8_t e_type=-1;
    int8_t d_type=-1;
	
    int32_t page_id=-1;
    int32_t file_id=-1;
    int32_t table_page_id=-1;
};

struct DiskAioRequestUserInfo {
	int32_t frame_id=-1;
	int32_t task_id=-1;
	int32_t file_id=-1;
	void* caller=0;
	void* func = NULL;
	bool do_user_cb=false;
	bool do_user_only_req_cb=false;
	char* read_buf=0;
	void* read_my_io=NULL;
	
	DiskAioRequestUserInfo(int32_t pid=-1, int32_t fid=-1, int32_t tid=-1, void* c=0, void* f=0, bool d=false, int32_t vid=-1) {
		frame_id = fid;
		task_id = tid;
		caller = c;
		func = f;
		do_user_cb = d;
	}
};

class DiskAioRequest
{
public:
	struct iocb cb;
	struct DiskAioRequestUserInfo user_info;	

	inline void* GetBuf() { return cb.u.c.buf; }
	inline int64_t GetIoSize() { return cb.u.c.nbytes; }
	inline void* GetFunc() { return user_info.func; }
	inline void SetFunc(void * func) { user_info.func = func; }
	inline void SetUserInfo(DiskAioRequestUserInfo& uinfo) { user_info = uinfo; }
	int Complete();
};

}
#endif
