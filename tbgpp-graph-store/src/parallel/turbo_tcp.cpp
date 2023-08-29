#include "parallel/turbo_tcp.hpp"

int turbo_tcp::connection_count = 0;
int turbo_tcp::total_fail_count = 0;
std::atomic<int64_t> turbo_tcp::accum_bytes_sent(0);
std::atomic<int64_t> turbo_tcp::accum_bytes_received(0);
char** turbo_tcp::hostname_list = NULL;
