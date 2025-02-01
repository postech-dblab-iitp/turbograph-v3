#ifndef _TURBO_BIN_LOADER_H
#define _TURBO_BIN_LOADER_H

/*
 * Design of the Turbo_bin_mmapper
 *
 * This class provides APIs that can memory map and access to binary files. The
 * function mmap64() is used to map files into memory. Two parameters should be set:
 *     The prot parameter describes memory protection of the mapping. We only consider
 *     two options: 
 *         - PROT_READ: Pages may be read
 *         - PROT_WRITE: Pages may be written
 *     By default, we assume that memory mapped files will be read. Depending on
 *     whether you want to write to a file or not, PROT_WRITE flag will be determined.
 *     The flags parameter determines whether updates to a memory mapped file are
 *     visible to other processes mapping the same region, and whether updates are
 *     carried through to the underlying file. Possible options are as follows:
 *         - MAP_SHARED: Share the mapping with other processes
 *         - MAP_PRIVATE: Create a private copy-on-write mapping
 *     By default, we use only MAP_SHRED option for the flags parameter.
 * Users of this class can open a file, map it into memory, and use the data() API to 
 * access the data in the file just like an array in memory.
 */

#include <streambuf>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "analytics/core/TypeDef.hpp"

#define END_OF_FILE	-1

class Turbo_bin_mmapper {

  public:

	Turbo_bin_mmapper(): file_descriptor(-1), file_size_(0), bytes_read_(0), pFileMap_(NULL) {

	}

	Turbo_bin_mmapper(const char* file_name, bool write_enabled = false)
		: file_descriptor(-1), file_size_(0), bytes_read_(0), pFileMap_(NULL) {
		OpenFileAndMemoryMap(file_name, write_enabled);
	}

	~Turbo_bin_mmapper() {
		Close(false);
	}

	void Close(bool rm = false) {
		if (file_descriptor == -1) return;
		munmap(pFileMap_, file_size_);
		close(file_descriptor);
		pFileMap_ = NULL;
		file_descriptor = -1;
		if (rm == true) {
			int is_error = remove(file_path.c_str());
			if (is_error != 0) {
				fprintf(stderr, "[Turbo_bin_mmapper] The file (%s) cannot be deleted; ErrorCode=%d\n", file_path.c_str(), errno);
			}
		}
	}

	ReturnStatus OpenFileAndMemoryMap(const char* file_name, bool write_enabled = false) {
		if (pFileMap_) {
			munmap(pFileMap_,file_size_);
		}
		if (file_descriptor != -1) {
			close(file_descriptor);
		}
		pFileMap_ = NULL;
		file_descriptor = -1;

		file_path = std::string(file_name);
		int open_flag, mmap_prot, mmap_flag;
		if(write_enabled) {
			open_flag = O_RDWR;
			mmap_prot = PROT_READ | PROT_WRITE;
			mmap_flag = MAP_SHARED;
			//mmap_flag = MAP_SHARED | MAP_HUGETLB;
		} else {
			open_flag = O_RDONLY;
			mmap_prot = PROT_READ;
			mmap_flag = MAP_SHARED;
			//mmap_flag = MAP_SHARED | MAP_HUGETLB;
		}

		file_descriptor = open(file_name, open_flag);
		if(file_descriptor == -1) {
			perror("error while opening");
			std::cout << "MMAP FILE OPEN ERROR : " << file_name << std::endl << std::flush;
			return FAIL;
		}

		file_size_ = lseek64(file_descriptor, 0, SEEK_END);
		ALWAYS_ASSERT(file_size_ != -1);

		if(file_size_ > 0) {
			pFileMap_ = (char *) mmap64(NULL, file_size_, mmap_prot, mmap_flag, file_descriptor, 0);
			if(pFileMap_ == MAP_FAILED) {
				std::cout << "MMAP FILE MAPPING FAILED WHEN OPENING : " << file_name <<" FILE SIZE :"<<file_size_<< ", ErrorCode=" << errno << std::endl << std::flush;
				return FAIL;
			}
		} else {
			pFileMap_ = NULL;
			return FAIL;
		}

		return OK;
	}

	ReturnStatus CreateFileAndMemoryMap(const char* file_name, std::size_t file_size) {
		if (pFileMap_) {
			munmap(pFileMap_,file_size_);
		}
		if (file_descriptor != -1) {
			close(file_descriptor);
		}
		pFileMap_ = NULL;
		file_descriptor = -1;

		file_path = std::string(file_name);
		int is_error = unlink(file_path.c_str());
		if (is_error != 0 && errno != ENOENT) {
			fprintf(stderr, "[Turbo_bin_mmapper] The file (%s) cannot be deleted; ErrorCode=%d\n", file_path.c_str(), errno);
		}

		int open_flag, mmap_prot, mmap_flag;
		open_flag = O_RDWR | O_CREAT |O_TRUNC ;
		mmap_prot = PROT_READ | PROT_WRITE;
		mmap_flag = MAP_SHARED;

		file_descriptor = open(file_name, open_flag, S_IRWXU | S_IRWXG | S_IRWXO);
		if(file_descriptor == -1) {
			perror("mmap open");
			std::cout << "MMAP FILE OPEN ERROR : " << file_name << std::endl << std::flush;
			return FAIL;
		}

		file_size_ = file_size;

		int err = fallocate64(file_descriptor, 0,0,file_size_);
		if(err == -1) {
			std::cout << "ERR:"<<errno << std::endl;
			std::cout << "size: "<<file_size_ << std::endl;
		}
		ALWAYS_ASSERT(err!=-1);

		pFileMap_ = (char *) mmap64(NULL, file_size_, mmap_prot, mmap_flag, file_descriptor, 0);
		if(pFileMap_ == MAP_FAILED) {
			std::cout << "MMAP FILE MAPPING FAILED : " << file_name << std::endl << std::flush;
			return FAIL;
		}


		return OK;
	}

	ReturnStatus Read(std::size_t offset, std::size_t size_to_read, char*& pData) {
		if (size_to_read >= 0 && size_to_read + offset <= file_size_) {
			pData =  pFileMap_ + offset;
			return OK;
		} else {
			pData = NULL;
			return FAIL;
		}
		return OK;
	}

	ReturnStatus ReadNext(std::size_t size_to_read, char*& pData) {
		if (size_to_read >= 0 && size_to_read + bytes_read_ < file_size_) {
			pData = pFileMap_ + bytes_read_;
			bytes_read_ += size_to_read;
			return OK;
		} else if (size_to_read + bytes_read_ == file_size_) {
			pData = pFileMap_ + bytes_read_;
			bytes_read_ += size_to_read;
			return DONE;
		} else {
			pData = NULL;
			return FAIL;
		}
		return OK;
	}

	char* data() {
		if (file_descriptor == -1) {
			return NULL;
		}
		ALWAYS_ASSERT(pFileMap_ != NULL);
		return pFileMap_;
	}

	std::size_t file_size() {
		if (file_descriptor == -1) {
			return 0;
		}
		return file_size_;
	}


  protected:

	std::string file_path;

	int file_descriptor;

	std::size_t file_size_;	// file size in bytes
	std::size_t bytes_read_;	// bytes that are read till now
	char* pFileMap_;
};

#endif
