#include "storage/cache/disk_aio/util.hpp"

/*__thread int64_t TG_ThreadContexts::thread_id = -1;
__thread int64_t TG_ThreadContexts::core_id = -1;
__thread int64_t TG_ThreadContexts::socket_id = -1;
__thread bool TG_ThreadContexts::per_thread_buffer_overflow = false;
__thread bool TG_ThreadContexts::run_delta_nwsm = false;
__thread PageID TG_ThreadContexts::pid_being_processed = -1;
__thread TG_NWSMTaskContext* TG_ThreadContexts::ctxt = NULL;*/

int64_t NumaHelper::sockets = 0;
int64_t NumaHelper::cores = 0;
int64_t NumaHelper::cores_per_socket = 0;
int64_t NumaHelper::numa_policy = 0;
NumaHelper NumaHelper::numa_helper;
