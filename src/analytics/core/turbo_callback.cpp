#include "analytics/core/turbo_callback.hpp"

/*
static void InvokeUserCallback(diskaio::DiskAioRequest* req) {
	diskaio::DiskAioRequestUserInfo user_info = req->user_info;
	ALWAYS_ASSERT(user_info.db_info.page_id >= 0);
	ALWAYS_ASSERT(user_info.frame_id >= 0);

	turbo_callback::AsyncCallbackUpdateDirectTableFunc(user_info);
	if (user_info.do_user_cb) {
		turbo_callback::AsyncCallbackUserFunction(user_info);
	}
}

static void InvokeReadReq(diskaio::DiskAioRequest* req) {
	diskaio::DiskAioRequestUserInfo user_info = req->user_info;
	ALWAYS_ASSERT(user_info.db_info.page_id >= 0);
	ALWAYS_ASSERT(user_info.frame_id >= 0);
	turbo_callback::AsyncCallbackUserReadFunction(req);
}
*/
