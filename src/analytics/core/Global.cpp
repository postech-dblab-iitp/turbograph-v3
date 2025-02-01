#include "analytics/core/Global.hpp"

std::string UserArguments::WORKSPACE_PATH;
std::string UserArguments::RAWDATA_PATH;
int64_t UserArguments::NUM_THREADS;
int64_t UserArguments::NUM_TOTAL_CPU_CORES;
int64_t UserArguments::NUM_CPU_SOCKETS;
int64_t UserArguments::NUM_DISK_AIO_THREADS;

int64_t UserArguments::VECTOR_PARTITIONS;
int64_t UserArguments::NUM_SUBCHUNKS_PER_EDGE_CHUNK;
int64_t UserArguments::NUM_ITERATIONS;
int64_t UserArguments::PHYSICAL_MEMORY;
int64_t UserArguments::SCHEDULE_TYPE;
int64_t UserArguments::MAX_LEVEL = -1;
int64_t UserArguments::CURRENT_LEVEL = -1;
bool UserArguments::USE_DEGREE_ORDER_REPRESENTATION = false;
int64_t UserArguments::DEGREE_THRESHOLD = 0;
IteratorMode_t UserArguments::ITERATOR_MODE = PARTIAL_LIST;
int64_t UserArguments::UPDATE_VERSION = 0;
int64_t UserArguments::SUPERSTEP = 0;
bool UserArguments::BUILD_DB = false;
bool UserArguments::LOAD_DELETEDB = false;
bool UserArguments::INSERTION_WORKLOAD = true;
int64_t UserArguments::MAX_VERSION = 0;
int64_t UserArguments::BEGIN_VERSION = 0;
int64_t UserArguments::END_VERSION = 0;
bool UserArguments::QUERY_ONGOING = false;

bool UserArguments::IS_GRAPH_DIRECTED_GRAPH_OR_NOT = true;
bool UserArguments::INCREMENTAL_PROCESSING = true;
bool UserArguments::USE_INCREMENTAL_SCATTER = false;
bool UserArguments::USE_INCREMENTAL_APPLY = false;
bool UserArguments::USE_PULL = false;
bool UserArguments::RUN_SUBQUERIES = false;
bool UserArguments::RUN_PRUNING_SUBQUERIES = false;
bool UserArguments::OV_FLUSH = true;
bool UserArguments::GRAPH_IN_MEMORY = false;

int UserArguments::INC_STEP = 0;
int UserArguments::PRUNING_BFS_LV = 0;
EdgeType UserArguments::EDGE_DB_TYPE = OUTEDGE;
DynamicDBType UserArguments::DYNAMIC_DB_TYPE = INSERT;

bool UserArguments::USE_DELTA_NWSM = false;
bool UserArguments::USE_FULLIST_DB = false;
bool UserArguments::ONLY_FIRST_SUPERSTEP = false;
int64_t UserArguments::tmp = -1;

int UserArguments::LATENT_FACTOR = 1;
int64_t UserArguments::LATENT_RANDOM_NUM_DECIMAL_PLACE = 1000;
int64_t UserArguments::LATENT_RANDOM_MAX_VALUE = 1;

int UserArguments::NUM_TOTAL_SUBQUERIES = 0;
int UserArguments::CURRENT_SUBQUERY_IDX = 0;
