#ifndef _TURBO_BIN_AIO_HANDLER_H
#define _TURBO_BIN_AIO_HANDLER_H

#include <streambuf>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <iterator>

#include "storage/cache/common.h"
#include "storage/cache/disk_aio/disk_aio_factory.hpp"

class Turbo_bin_aio_handler {
  public:

  Turbo_bin_aio_handler() : file_descriptor(-1), is_reserved(false), delete_when_close(false) {
    file_mmap = NULL;
  }

  Turbo_bin_aio_handler(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false, bool o_direct = false)
    : file_descriptor(-1), is_reserved(false), delete_when_close(false) {
        OpenFile(file_name, create_if_not_exist, write_enabled, delete_if_exist, o_direct);
  }

  ~Turbo_bin_aio_handler() {
    if (file_id >= 0) {
      DiskAioFactory::GetPtr()->CloseAioFile(file_id);
      file_id = -1;
    }
  }

  static void InitializeCoreIds() {
    if (my_core_id_ == -1) {
        my_core_id_ = std::atomic_fetch_add( (std::atomic<int64_t>*) &(core_counts_), 1L);
    }        
    assert(my_core_id_ >= 0);
    assert(my_core_id_ < MAX_NUM_PER_THREAD_DATASTRUCTURE);
  }

  static void InitializeIoInterface() {
    InitializeCoreIds();
    int max_num_ongoing = PER_THREAD_MAXIMUM_ONGOING_DISK_AIO * 2;
    int aio_tid = my_core_id_ % DiskAioParameters::NUM_DISK_AIO_THREADS;
    if (per_thread_aio_interface_read.get(my_core_id_) == nullptr) {
      per_thread_aio_interface_read.get(my_core_id_) = DiskAioFactory::GetPtr()->CreateAioInterface(max_num_ongoing, aio_tid);
    }
    if (per_thread_aio_interface_write.get(my_core_id_) == nullptr) {
      per_thread_aio_interface_write.get(my_core_id_) = DiskAioFactory::GetPtr()->CreateAioInterface(max_num_ongoing, aio_tid);
    }
  }
  
  static diskaio::DiskAioInterface*& GetMyDiskIoInterface(bool read) {
    InitializeIoInterface();
    assert (my_core_id_ != -1);
    assert (my_core_id_ >= 0 && my_core_id_ < MAX_NUM_PER_THREAD_DATASTRUCTURE);
    if (read) {
      return per_thread_aio_interface_read.get(my_core_id_);
    } else {
      return per_thread_aio_interface_write.get(my_core_id_);
    }
  }
    
  static diskaio::DiskAioInterface*& GetMyDiskIoInterface(bool read, int core_id_) {
    InitializeCoreIds();
    assert (core_id_ != -1);
    assert (core_id_ >= 0 && core_id_ < MAX_NUM_PER_THREAD_DATASTRUCTURE);
    if (read) {
      return per_thread_aio_interface_read.get(core_id_);
    } else {
      return per_thread_aio_interface_write.get(core_id_);
    }
  }

  static void WaitForAllPendingDiskIoOfAllInterfaces(bool read, bool write) {
    for (int core_id = 0; core_id < MAX_NUM_PER_THREAD_DATASTRUCTURE; core_id++) {
      if (read) {
        diskaio::DiskAioInterface* my_io = per_thread_aio_interface_read.get(core_id);
        if (my_io != NULL) {
          WaitMyPendingDiskIO(my_io, my_io->GetNumOngoing());
        }
      }
      if (write) {
        diskaio::DiskAioInterface* my_io = per_thread_aio_interface_write.get(core_id);
        if (my_io != NULL) {
          WaitMyPendingDiskIO(my_io, my_io->GetNumOngoing());
        }
      }
    }
  }
    
  static void WaitMyPendingDiskIO(bool read, int num_to_complete) {
    diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(read);
    WaitMyPendingDiskIO(my_io, num_to_complete);
  }
    
  static void WaitMyPendingDiskIO(diskaio::DiskAioInterface* my_io, int num_to_complete) {
    int backoff = 1;
    num_to_complete = (num_to_complete > my_io->GetNumOngoing()) ? my_io->GetNumOngoing() : num_to_complete;
    do {
      num_to_complete -= my_io->WaitForResponses(0);
      if (num_to_complete > 0) {
        usleep (backoff * 1024);
        if (backoff <= 16 * 1024) backoff *= 2;
      }
    } while (num_to_complete > 0);
  }
    
  static int64_t WaitMyPendingDiskIO(diskaio::DiskAioInterface* my_io) {
    assert(my_io != NULL);
    int num_to_complete = my_io->GetNumOngoing();
    int backoff = 1;
    while (my_io->GetNumOngoing() > 0) {
      usleep (backoff * 1024);
      my_io->WaitForResponses(0);
      if (backoff <= 16 * 1024) backoff *= 2;
    }
    assert(my_io->GetNumOngoing() == 0);
    return num_to_complete;
  }

  static void WaitAllPendingDiskIO(bool read) {
#pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
    {
      diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(read);
      if (my_io != NULL) {
        WaitMyPendingDiskIO(my_io);
      }
    }
  }

  void Close(bool rm = false) {
    if (file_id >= 0) {
      assert(DiskAioFactory::GetPtr() != NULL);
      DiskAioFactory::GetPtr()->CloseAioFile(file_id);
      file_id = -1;
    }
    if (delete_when_close) {
      if (check_file_exists(file_path)) {
        int status = remove(file_path.c_str());
        assert(status == 0);
      }
    } else {
      if (rm && check_file_exists(file_path)) {
        int status = remove(file_path.c_str());
        assert(status == 0);
      }
    }
  }

  void Truncate(int64_t length) {
    assert (file_descriptor != -1);
    int status = ftruncate64(file_descriptor, length);
    assert(status != 0);
    file_size_ = length;
    //assert (file_size() == length);
  }

  std::string GetFilePath() {
    return file_path;
  }

  ReturnStatus OpenFile(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false, bool o_direct = false) {
    int flag = O_RDWR | O_CREAT;
    if (o_direct) flag = flag | O_DIRECT;
    file_id = DiskAioFactory::GetPtr()->OpenAioFile(file_name, flag);
    if (file_id < 0) {
      fprintf(stdout, "[Turbo_bin_aio_handler::OpenFile] Fail to open file %s\n", file_name);
      // TODO throw Exception
    }
    assert(file_id >= 0);
    file_size_ = DiskAioFactory::GetPtr()->GetAioFileSize(file_id);
    // assert(file_size_ == 0);
    file_descriptor = DiskAioFactory::GetPtr()->Getfd(file_id);
    file_mmap = NULL;
    //OpenFileTemp(file_name, false, false, false, o_direct);
    file_path = std::string(file_name);
    assert(file_id >= 0);
    
    return NOERROR;
  }
  
  /*void OpenFileTemp(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false, bool o_direct = false) {
    int file_descriptor_temp = file_descriptor;
    mode_t old_umask;
    old_umask = umask(0);
    if (delete_if_exist) {
      remove(file_name);
    }

    if(create_if_not_exist && write_enabled && o_direct)
      file_descriptor = open(file_name, O_RDWR | O_CREAT | O_DIRECT, 0666);
    else if(create_if_not_exist && write_enabled)
      file_descriptor = open(file_name, O_RDWR | O_CREAT, 0666);
    else if(write_enabled && o_direct)
      file_descriptor = open(file_name, O_RDWR | O_DIRECT, 0666);
    else if(write_enabled)
      file_descriptor = open(file_name, O_RDWR, 0666);
    else
      file_descriptor = open(file_name, O_RDONLY, 0666);
    umask(old_umask);

    if (file_descriptor == -1) {
      fprintf(stdout, "Fail to open file %s\n", file_name);
    }
    PCHECK(file_descriptor != -1);

    off64_t f = lseek64(file_descriptor, 0, SEEK_END);
    file_size_ = f;
    assert(f != -1);
    file_path = std::string(file_name);
    //assert(file_descriptor == file_descriptor_temp);
    return OK;
  }*/
    
  void Append(std::int64_t size_to_append, char* data, void* func, const std::function<void(diskaio::DiskAioInterface*)>& wait_cb = {}) {
    InitializeIoInterface();
    assert(size_to_append % 512 == 0);
    assert(file_size() % 512 == 0);
    assert(((uintptr_t)data) % 512 == 0);
    diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(false);
    AioRequest req;
    req.buf = data;
    req.start_pos = file_size_; 
    req.io_size = size_to_append;
    req.user_info.file_id = file_id;
    //req.user_info.do_user_cb = true;
    req.user_info.caller = NULL;
    req.user_info.func = func;
    Turbo_bin_aio_handler::WaitMyPendingDiskIO(my_io, 0);
    bool success = DiskAioFactory::GetPtr()->AAppend(req, my_io);
    if (wait_cb) {
      wait_cb(my_io);
    }

    if (is_reserved) {
      is_reserved = false;
    } else {
      file_size_ += size_to_append;
    }
  }

  // TODO - remove buf_to_construct, construct_next, change API to get templated user-defined request
  void Read(int64_t offset_to_read, int64_t size_to_read, char* data, void* caller, void* func, diskaio::DiskAioInterface* my_io) {
    if (is_reserved) return;

    assert (size_to_read > 0);
    assert (size_to_read % 512 == 0);
    assert (offset_to_read % 512 == 0);
    assert (((uintptr_t)data) % 512 == 0);
    // fprintf(stdout, "Read %ld\n", ((uintptr_t)data) % 512);

    AioRequest req;
    req.buf = data;
    req.start_pos = offset_to_read;
    req.io_size = size_to_read;
    req.user_info.file_id = file_id;
    req.user_info.do_user_cb = false;
    req.user_info.caller = caller;
    req.user_info.func = func;

    // XXX read_buf variable is not for this purpose, but just use it
    // fprintf(stdout, "ARead my_io %p, %p, %ld, %ld, %d\n", my_io, req.buf, req.start_pos, req.io_size, req.user_info.file_id);
    bool success = DiskAioFactory::GetPtr()->ARead(req, my_io);
  }
    
  void Read(int64_t offset_to_read, int64_t size_to_read, char* data, void* caller, void* func) {
    InitializeIoInterface();
    diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(true);
    Read(offset_to_read, size_to_read, data, caller, func, my_io);
  }

  // TODO - remove buf_to_construct, construct_next, change API to get templated user-defined request
  void ReadWithSplittedIORequest(int64_t offset_to_read, int64_t size_to_read, char* data, void* caller, void* func, diskaio::DiskAioInterface* my_io) {
    if (is_reserved) return;

    assert (size_to_read > 0);
    assert (size_to_read % 512 == 0);
    assert (offset_to_read % 512 == 0);
    assert (((uintptr_t)data) % 512 == 0);
    // fprintf(stdout, "Read %ld\n", ((uintptr_t)data) % 512);

    size_t cur_io_size;
    while (size_to_read != 0) {
      cur_io_size = size_to_read > MAX_IO_SIZE_PER_RW ? MAX_IO_SIZE_PER_RW : size_to_read;

      AioRequest req;
      req.buf = data;
      req.start_pos = offset_to_read;
      req.io_size = cur_io_size;
      req.user_info.file_id = file_id;
      req.user_info.do_user_cb = false;
      req.user_info.caller = caller;
      req.user_info.func = func;

      bool success = DiskAioFactory::GetPtr()->ARead(req, my_io);
      size_to_read -= cur_io_size;
      offset_to_read += cur_io_size;
      data += cur_io_size;
    }
  }
    
  void ReadWithSplittedIORequest(int64_t offset_to_read, int64_t size_to_read, char* data, void* caller, void* func) {
    InitializeIoInterface();
    diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(true);
    ReadWithSplittedIORequest(offset_to_read, size_to_read, data, caller, func, my_io);
  }

  void Write(int64_t offset_to_write, int64_t size_to_write, char* data) {
    InitializeIoInterface();
    assert(size_to_write % 512 == 0);
    assert(file_size() % 512 == 0);
    assert(((uintptr_t)data) % 512 == 0);
    diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(false);
    AioRequest req;
    req.buf = data;
    req.start_pos = offset_to_write; 
    req.io_size = size_to_write;
    req.user_info.file_id = file_id;
    //req.user_info.do_user_cb = true;
    req.user_info.caller = NULL;
    
    bool success = DiskAioFactory::GetPtr()->AWrite(req, my_io);

    if (is_reserved) {
      is_reserved = false;
    } else {
    }
  }

  void FlushAll() {
    InitializeIoInterface();
    assert(file_size() % 512 == 0);
    assert(((uintptr_t)aligned_data_ptr) % 512 == 0);
    diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(false);
    AioRequest req;
    req.buf = (char*) aligned_data_ptr;
    req.start_pos = 0;
    req.io_size = file_size();
    req.user_info.file_id = file_id;
    //req.user_info.do_user_cb = true;
    req.user_info.caller = NULL;
    
    bool success = DiskAioFactory::GetPtr()->AWrite(req, my_io);
    // fprintf(stdout, "Write File %d size %ld, %p, my_io %p, %s\n",
    //   file_id, file_size(), aligned_data_ptr, my_io, success ? "True" : "False");

    if (is_reserved) {
      is_reserved = false;
    } else {
    }
  }

  void FlushAllBlocking() {
    InitializeIoInterface();
    assert(file_size() % 512 == 0);
    assert(((uintptr_t)aligned_data_ptr) % 512 == 0);
    diskaio::DiskAioInterface *my_io = GetMyDiskIoInterface(false);

    size_t size_to_flush = file_size();
    size_t cur_io_size;
    size_t cur_start_pos = 0;
    
    while (size_to_flush != 0) {
      cur_io_size = size_to_flush > MAX_IO_SIZE_PER_RW ? MAX_IO_SIZE_PER_RW : size_to_flush;

      AioRequest req;
      req.buf = (char *)(aligned_data_ptr + cur_start_pos);
      req.start_pos = cur_start_pos;
      req.io_size = cur_io_size;
      req.user_info.file_id = file_id;
      //req.user_info.do_user_cb = true;
      req.user_info.caller = NULL;

      bool success = DiskAioFactory::GetPtr()->AWrite(req, my_io);
      WaitAllPendingDiskIO(false);

      size_to_flush -= cur_io_size;
      cur_start_pos += cur_io_size;
    }

    if (is_reserved) {
      is_reserved = false;
    } else {
    }
  }

  char* CreateMmap(bool write_enabled) {
    assert (file_mmap == NULL);
    assert (file_descriptor != -1);
    int open_flag, mmap_prot, mmap_flag;
    if(write_enabled) {
      open_flag = O_RDWR;
      mmap_prot = PROT_READ | PROT_WRITE;
      mmap_flag = MAP_SHARED;
    } else {
      open_flag = O_RDONLY;
      mmap_prot = PROT_READ;
      mmap_flag = MAP_SHARED;
    }
    //int64_t file_size__ = lseek64(file_descriptor, 0, SEEK_END);
    if (file_size_ == 0) return NULL;
        
    assert (file_size_ > 0);
    file_mmap = (char *) mmap64(NULL, file_size_, mmap_prot, mmap_flag, file_descriptor, 0);
    assert(file_mmap != MAP_FAILED);
    return file_mmap;
  }

  void DestructMmap() {
    assert (file_descriptor != -1);
    //int64_t file_size_ = lseek64(file_descriptor, 0, SEEK_END);
    assert (file_size_ >= 0);
    int status = munmap(file_mmap, file_size_);
    assert(status == 0);
    file_mmap = NULL;
  }

  int64_t file_size() {
    return file_size_;
  }

  int64_t GetRequestedSize() {
    return requested_size_;
  }

  void SetRequestedSize(int64_t requested_size) {
    requested_size_ = requested_size;
  }

  int fdval() {
    return file_descriptor;
  }

  int GetFileID() {
    return file_id;
  }
      
  static void WaitForMyIoRequests(bool read, bool write) {
    InitializeIoInterface();
    if (read) {
      diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(true);
      my_io->WaitForResponses();
    }
    if (write) {
      diskaio::DiskAioInterface* my_io = GetMyDiskIoInterface(false);
      my_io->WaitForResponses();
    }
  }

  static void WaitForIoRequests(bool read, bool write) {
#pragma omp parallel num_threads(DiskAioParameters::NUM_THREADS)
    {
        WaitForMyIoRequests(read, write);
    }
  }

  void ReserveFileSize(size_t reserve_size) {
    file_size_ = reserve_size;
    is_reserved = true;
  }

  bool IsReserved() {
    return is_reserved;
  }

  void SetCanDestroy(bool can_destroy) {
    delete_when_close = can_destroy;
  }
  
  uint8_t* GetDataPtr() {
    return aligned_data_ptr;
  }

  void SetDataPtr(uint8_t* data_ptr_) {
    aligned_data_ptr = data_ptr_;
  }

private:
  int file_descriptor;
  int file_id;
  bool is_reserved = false;
  bool delete_when_close;
  char* file_mmap;
  uint8_t* aligned_data_ptr;
  std::string file_path;
  int64_t file_size_;
  int64_t requested_size_;
  static per_thread_lazy<diskaio::DiskAioInterface*> per_thread_aio_interface_read;
  static per_thread_lazy<diskaio::DiskAioInterface*> per_thread_aio_interface_write;
  static int64_t core_counts_;
  static __thread int64_t my_core_id_;
};

#endif
