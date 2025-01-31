#ifndef __DISK_AIO_H__
#define __DISK_AIO_H__

#include "disk_aio_interface.hpp"
#include "disk_aio_request.hpp"
#include "disk_aio_thread.hpp"
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <vector>
#include <string>
#include <iostream>
#include "tbb/concurrent_vector.h"

namespace diskaio {

struct DiskAioStats {
	size_t num_reads = 0;
	size_t num_read_bytes = 0;
	size_t num_writes = 0;
	size_t num_write_bytes = 0;
};

class DiskAio {
  public:

	/*
	* Get the files under the given directory
	*/
	static int GetAioFileList(std::string& dir_path, std::vector<std::string>& file_list) {
		DIR *dp;
		struct dirent *dirp;
		if((dp  = opendir(dir_path.c_str())) == NULL) {
			std::cout << "Error(" << errno << ") opening " << dir_path << "\n";
			return errno;
		}
		while ((dirp = readdir(dp)) != NULL) {
			std::string fname = std::string(dirp->d_name);
			if (fname == "." || fname == "..")
				continue;
			file_list.push_back(std::string(dirp->d_name));
		}
		closedir(dp);
		return 0;
	}

	/*
	* Create a directory
	*/
	static bool CreateDir(std::string& dir_path, bool recursive) {
		if (!recursive) {
			int ret = mkdir(dir_path.c_str(), 0755);
			return ret == 0;
		}
		std::vector<std::string> strs;
		split_string(dir_path, '/', strs);
		std::string curr_dir;
		for (unsigned i = 0; i < strs.size(); i++) {
			curr_dir += strs[i] + "/";
			int ret = mkdir(curr_dir.c_str(), 0755);
			if (ret < 0 && errno != EEXIST) {
				perror("mkdir");
				return false;
			}
		}
		return true;
	}

  private:

  public:
	/*
	* Open the file
	*/
	int OpenAioFile(std::string path, int flag) {
		int fd = open(path.data(), flag, S_IRWXU | S_IRWXG | S_IRWXO);
		if (fd < 0) {
            fprintf(stdout, "Failed to create file %s\n", path.c_str());
            perror("Disk_aio OpenAioFile");
            return -1;
        }
		/* Give a hint to the OS that the access pattern is random */
		int ret = posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
		if (ret < 0) std::cout << "cannot fadvise";
        return fd;
	}

	/*
	* Get the file descriptor of the given file
	*/
	inline int Getfd(int fid) {
        return fid;
	}

	/*
	* Close the file
	*/
	void CloseAioFile(int fid) {
        close(fid);
	}

	std::vector<DiskAioThread*> disk_aio_threads_;

	/*
	* Construct the aio file system
	*/
	DiskAio(int num_aio_threads, int io_depth=32) {
		disk_aio_threads_.resize(num_aio_threads);
		for (int i = 0; i < num_aio_threads; ++i) {
			disk_aio_threads_[i] = new DiskAioThread(0, (void*) this, io_depth);
		}
	}

	/*
	* Start the aio threads in the file system
	*/
	void start() {
		for (auto it = disk_aio_threads_.begin(); it != disk_aio_threads_.end(); ++it) {
			(*it)->start();
		}
	}

	/*
	* Stop the aio threads in the file system
	*/
	void stop() {
		for (auto it = disk_aio_threads_.begin(); it != disk_aio_threads_.end(); ++it) {
			(*it)->stop();
		}
	}

	/*
	* Create aio interface
	*/
	DiskAioInterface* CreateAioInterface(int max_num_ongoing=0, int tid=0) {
        return new DiskAioInterface((void*) this, max_num_ongoing, disk_aio_threads_[tid]);
	}

	/*
	* Aggregate the counters of aio threads and return the aggregated values (e.g., the number of reads/writes)
	*/
	DiskAioStats GetStats() {
		DiskAioStats stats;
		for (auto it = disk_aio_threads_.begin(); it != disk_aio_threads_.end(); ++it) {
			DiskAioThreadStats thread_stats;
			(*it)->GetStats(thread_stats);
			stats.num_reads += thread_stats.num_reads;
			stats.num_read_bytes += thread_stats.num_read_bytes;
			stats.num_writes += thread_stats.num_writes;
			stats.num_write_bytes += thread_stats.num_write_bytes;
		}
		return stats;
	}

	/*
	* Reset the counters of aio threads
	*/
    void ResetStats() {
		for (auto it = disk_aio_threads_.begin(); it != disk_aio_threads_.end(); ++it) {
			(*it)->ResetStats();
        }
    }
};

}
#endif
