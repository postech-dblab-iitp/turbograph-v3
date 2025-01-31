#ifndef TURBO_CALLBACK_H
#define TURBO_CALLBACK_H

#include "page.hpp"
#include "TG_NWSMTaskContext.hpp"
#include "disk_aio_request.hpp"

class TG_NWSMCallback;

class turbo_callback {
  public:
	static void (*AsyncCallbackUpdateDirectTableFunc)(diskaio::DiskAioRequestUserInfo &user_info);
	static void (*AsyncCallbackUserFunction)(diskaio::DiskAioRequestUserInfo &user_info);
	static void (*AsyncCallbackUserReadFunction)(diskaio::DiskAioRequest *req);
};

class TG_NWSMCallback {
  public:
	virtual void CallbackTask(diskaio::DiskAioRequestUserInfo &user_info) = 0;
};

static void InvokeUserCallback(diskaio::DiskAioRequest* req) {
	diskaio::DiskAioRequestUserInfo user_info = req->user_info;

	ALWAYS_ASSERT(user_info.db_info.page_id >= 0);
	ALWAYS_ASSERT(user_info.frame_id >= 0);

	turbo_callback::AsyncCallbackUpdateDirectTableFunc(user_info);

	if (user_info.do_user_cb) {
		turbo_callback::AsyncCallbackUserFunction(user_info);
	}
}

static void InvokeReadReq(diskaio::DiskAioRequest* req)
{
	diskaio::DiskAioRequestUserInfo user_info = req->user_info;
	
	ALWAYS_ASSERT(user_info.db_info.page_id >= 0);
	ALWAYS_ASSERT(user_info.frame_id >= 0);
	turbo_callback::AsyncCallbackUserReadFunction(req);
}

#endif
