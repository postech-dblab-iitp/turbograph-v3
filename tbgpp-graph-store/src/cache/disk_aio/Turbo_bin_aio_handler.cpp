#include "Turbo_bin_aio_handler.hpp"

per_thread_lazy<diskaio::DiskAioInterface*> Turbo_bin_aio_handler::per_thread_aio_interface_read;
per_thread_lazy<diskaio::DiskAioInterface*> Turbo_bin_aio_handler::per_thread_aio_interface_write;
__thread int64_t Turbo_bin_aio_handler::my_core_id_ = -1;
int64_t Turbo_bin_aio_handler::core_counts_ = 0;
