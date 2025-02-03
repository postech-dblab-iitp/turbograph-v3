#ifndef COMMON_H
#define COMMON_H

#include <sys/stat.h>

#define MAX_IO_SIZE_PER_RW (64 * 1024 * 1024L)

inline bool check_file_existence (const std::string& name) {
    struct stat buffer;   
    return (stat (name.c_str(), &buffer) == 0); 
}

#endif // COMMON_H
