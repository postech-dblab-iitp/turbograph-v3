#ifndef CACHE_COMMON_H
#define CACHE_COMMON_H

#include <sys/stat.h>

#define MAX_IO_SIZE_PER_RW (64 * 1024 * 1024L)

inline bool check_file_exists (const std::string& name) {
    struct stat buffer;   
    return (stat (name.c_str(), &buffer) == 0); 
}

enum ReturnStatus {
    NOERROR=0,
    DONE=1,
};

// Column chunk cache
extern const char* SHM_NAME;
extern std::string EVICTION_ALGORITHM;
const uint64_t MAX_TOTAL_TABLE_SIZE =
    100ul * 1024 * 1024 * 1024;  // Assume at most 100GB table size.
const size_t COLUMN_CHUNK_ALIGN_SIZE =
    4096;  // In order to use madvise, the size should be aligned to page size.

#endif // CACHE_COMMON_H
