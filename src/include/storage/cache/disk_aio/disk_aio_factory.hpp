#ifndef __DISK_AIO_FACTORY_H_
#define __DISK_AIO_FACTORY_H_

#include <iostream>
#include <ctime>
#include <omp.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "common/typedef.hpp"
#include "disk_aio.hpp"
#include "storage/cache/disk_aio/disk_aio_request.hpp"
#include "storage/cache/disk_aio/eXDB_dist_internal.hpp"

struct AioRequest {
	int64_t start_pos;
	int64_t io_size;
	char* buf;
	diskaio::DiskAioRequestUserInfo user_info;
};

class DiskAioFactory {
  private:
	static DiskAioFactory* ptr;
  public:
	static DiskAioFactory*& GetPtr() {
		return ptr;
	}
	static int GetAioFileList(std::string& dir_path, std::vector<std::string>& file_list) {
		return diskaio::DiskAio::GetAioFileList(dir_path, file_list);
	}
	static bool CreateDir(std::string& dir_path, bool recursive=true) {
		return diskaio::DiskAio::CreateDir(dir_path, recursive);
	}

  private:
	diskaio::DiskAio* daio;
  public:
	DiskAioFactory(int& res, int num_aio_disk_threads, int io_depth=64);
	~DiskAioFactory();

	int OpenAioFile(const char* file_path, int flag);
	void RemoveAioFile(int file_id=0);
	void CloseAioFile(int file_id=0, bool rm=false);
	std::size_t GetAioFileSize(int file_id=0);
    int Getfd(int file_id);
  private:
	per_thread_lazy<diskaio::DiskAioInterface*> per_thread_aio_interface;
  public:
	diskaio::DiskAioInterface* CreateAioInterface(int max_num_ongoing=PER_THREAD_MAXIMUM_ONGOING_DISK_AIO * 2, int tid = 0);
    diskaio::DiskAioInterface* GetAioInterface(int tid);
	void CreateAioInterfaces(int max_num_ongoing=PER_THREAD_MAXIMUM_ONGOING_DISK_AIO * 2);
	int ARead(AioRequest& req, diskaio::DiskAioInterface* my_io=0);
	int AWrite(AioRequest& req, diskaio::DiskAioInterface* my_io=0);
	int AAppend(AioRequest& req, diskaio::DiskAioInterface* my_io=0);
	int WaitForAllResponses(diskaio::DiskAioInterface** my_io=0);
	int GetNumOngoing(diskaio::DiskAioInterface** my_io=0);
	diskaio::DiskAioStats GetStats();
	void ResetStats();
};


#endif
