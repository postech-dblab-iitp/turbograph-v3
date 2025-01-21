#include "util.hpp"

int64_t NumaHelper::sockets = 0;
int64_t NumaHelper::cores = 0;
int64_t NumaHelper::cores_per_socket = 0;
int64_t NumaHelper::numa_policy = 0;
NumaHelper NumaHelper::numa_helper;
