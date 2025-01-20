#pragma once

#include "common/constants.hpp"
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace s62 {

inline void MkDir(string &path, bool delete_if_exist) {
	if (delete_if_exist) remove(path.c_str());
	mode_t old_umask;
	old_umask = umask(0);
	int is_error = mkdir((path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
	if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
		fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (path).c_str(), errno);
		umask(old_umask);
		return;
	}
	umask(old_umask);
}

} // namespace s62
