#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <unordered_set>
#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#include "Turbo_bin_aio_handler.hpp"
#include "Turbo_bin_io_handler.hpp"
#include "cache/common.h"
#include "cache/cache_data_transformer.h"
#include "cache/chunk_cache_manager.h"
#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "common/types/string_type.hpp"
#include "icecream.hpp"

#ifndef USE_LIGHTNING_CLIENT
#include "cache/admission_eviction_manager.h"
#endif

namespace duckdb {

int recv_file_descriptor(int unix_sock)
{
    ssize_t size;
    struct msghdr msg;
    struct iovec iov;
    union {
        struct cmsghdr cmsghdr;
        char control[CMSG_SPACE(sizeof(int))];
    } cmsgu;
    struct cmsghdr *cmsg;
    char buf[2];
    int fd = -1;

    iov.iov_base = buf;
    iov.iov_len = 2;

    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsgu.control;
    msg.msg_controllen = sizeof(cmsgu.control);
    size = recvmsg(unix_sock, &msg, 0);
    if (size < 0) {
        std::cerr << "recvmsg error" << std::endl;
        return -1;
    }
    cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_len == CMSG_LEN(sizeof(int))) {
        if (cmsg->cmsg_level != SOL_SOCKET) {
            std::cerr << "invalid cmsg_level " << cmsg->cmsg_level << std::endl;
            return -1;
        }
        if (cmsg->cmsg_type != SCM_RIGHTS) {
            std::cerr << "invalid cmsg_type " << cmsg->cmsg_type << std::endl;
            return -1;
        }
        int *fd_p = (int *)CMSG_DATA(cmsg);
        fd = *fd_p;
    }
    else {
        fd = -1;
    }

    return (fd);
}

ChunkCacheManager *ChunkCacheManager::ccm;

void ChunkCacheManager::InitializeFileHandlersByIteratingDirectories(
    const char *path)
{
    std::string partition_path = std::string(path);
    for (const auto &partition_entry :
         std::filesystem::directory_iterator(partition_path)) {  // /path/
        std::string partition_entry_path = std::string(partition_entry.path());
        std::string partition_entry_name = partition_entry_path.substr(
            partition_entry_path.find_last_of("/") + 1);
        if (StringUtil::StartsWith(partition_entry_name, "part_")) {
            std::string extent_path = partition_entry_path + std::string("/");
            for (const auto &extent_entry : std::filesystem::directory_iterator(
                     extent_path)) {  // /path/part_/
                std::string extent_entry_path =
                    std::string(extent_entry.path());
                std::string extent_entry_name = extent_entry_path.substr(
                    extent_entry_path.find_last_of("/") + 1);
                if (StringUtil::StartsWith(extent_entry_name, "ext_")) {
                    std::string chunk_path =
                        extent_entry_path + std::string("/");
                    for (const auto &chunk_entry :
                         std::filesystem::directory_iterator(
                             chunk_path)) {  // /path/part_/ext_/
                        std::string chunk_entry_path =
                            std::string(chunk_entry.path());
                        std::string chunk_entry_name = chunk_entry_path.substr(
                            chunk_entry_path.find_last_of("/") + 1);
                        ChunkDefinitionID chunk_id =
                            (ChunkDefinitionID)std::stoull(
                                chunk_entry_name.substr(
                                    chunk_entry_name.find("_") + 1));

                        // Open File & Insert into file_handlers
                        D_ASSERT(file_handlers.find(chunk_id) ==
                                 file_handlers.end());
                        file_handlers[chunk_id] = new Turbo_bin_aio_handler();
                        ReturnStatus rs = file_handlers[chunk_id]->OpenFile(
                            chunk_entry_path.c_str(), false, true, false, true);
                        D_ASSERT(rs == NOERROR);

                        // Read First Block & SetRequestedSize
                        char *first_block;
                        int status =
                            posix_memalign((void **)&first_block, 512, 512);
                        if (status != 0)
                            throw InvalidInputException(
                                "posix_memalign fail");  // XXX wrong exception type

                        file_handlers[chunk_id]->Read(0, 512, first_block, this,
                                                      nullptr);
                        file_handlers[chunk_id]->WaitAllPendingDiskIO(true);

                        size_t requested_size = ((size_t *)first_block)[0];
                        file_handlers[chunk_id]->SetRequestedSize(
                            requested_size + 8);
                        free(first_block);
                    }
                }
            }
        }
    }
}

void ChunkCacheManager::InitializeFileHandlersUsingMetaInfo(const char *path)
{
    std::string meta_file_path = std::string(path) + "/" + file_meta_info_name;
    Turbo_bin_io_handler file_meta_info(meta_file_path.c_str(), false, false,
                                        false);
    size_t file_size = file_meta_info.file_size();
    uint64_t *meta_info = new uint64_t[file_size / sizeof(uint64_t)];
    file_meta_info.Read(0, file_size, (char *)meta_info);
    file_meta_info.Close();

    for (size_t i = 0; i < file_size / sizeof(uint64_t); i += 2) {
        ChunkDefinitionID chunk_id = (ChunkDefinitionID)meta_info[i];
        size_t requested_size = meta_info[i + 1];

        uint64_t extent_id = chunk_id >> 32;
        uint64_t partition_id = extent_id >> 16;
        std::string chunk_path = std::string(path) + "/part_" +
                                 std::to_string(partition_id) + "/ext_" +
                                 std::to_string(extent_id) + "/chunk_" +
                                 std::to_string(chunk_id);
        D_ASSERT(file_handlers.find(chunk_id) == file_handlers.end());
        file_handlers[chunk_id] = new Turbo_bin_aio_handler();
        ReturnStatus rs = file_handlers[chunk_id]->OpenFile(
            chunk_path.c_str(), false, true, false, true);
        D_ASSERT(rs == NOERROR);
        file_handlers[chunk_id]->SetRequestedSize(requested_size);
    }

    delete[] meta_info;
}

void ChunkCacheManager::FlushMetaInfo(const char *path)
{
    uint64_t *meta_info;
    int64_t num_total_files = file_handlers.size();
    meta_info = new uint64_t[num_total_files * 2];

    int64_t i = 0;
    for (auto &file_handler : file_handlers) {
        assert(file_handler.second != nullptr);
        meta_info[i] = file_handler.first;
        meta_info[i + 1] = file_handler.second->GetRequestedSize();
        i += 2;
    }

    std::string meta_file_path = std::string(path) + "/" + file_meta_info_name;
    Turbo_bin_io_handler file_meta_info(meta_file_path.c_str(), true, true,
                                        true);
    file_meta_info.Append(num_total_files * 2 * sizeof(uint64_t),
                          (char *)meta_info);
    file_meta_info.Close(false);

    delete[] meta_info;
}

Turbo_bin_aio_handler *ChunkCacheManager::GetFileHandler(ChunkID cid)
{
    auto file_handler = file_handlers.find(cid);
    D_ASSERT(file_handler != file_handlers.end());
    D_ASSERT(file_handler->second != nullptr);
    if (file_handler->second->GetFileID() == -1) {
        exit(-1);
        //TODO throw exception
    }
    return file_handler->second;
}

void ChunkCacheManager::ReadData(ChunkID cid, std::string file_path, void *ptr,
                                 size_t size_to_read, bool read_data_async)
{
    auto file_handler = file_handlers.find(cid);
    if (file_handlers[cid]->GetFileID() == -1) {
        exit(-1);
        //TODO throw exception
    }
    // file_handlers[cid]->Read(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
    file_handlers[cid]->ReadWithSplittedIORequest(
        0, (int64_t)size_to_read, (char *)ptr, nullptr, nullptr);
    if (!read_data_async)
        file_handlers[cid]->WaitForMyIoRequests(true, true);
}

void ChunkCacheManager::WriteData(ChunkID cid)
{
    D_ASSERT(file_handlers.find(cid) != file_handlers.end());
    //client->Flush(cid, file_handlers[cid]);
    return;
}

ReturnStatus ChunkCacheManager::CreateNewFile(ChunkID cid,
                                              std::string file_path,
                                              size_t alloc_size,
                                              bool can_destroy)
{
    D_ASSERT(file_handlers.find(cid) == file_handlers.end());
    file_handlers[cid] = new Turbo_bin_aio_handler();
    ReturnStatus rs = file_handlers[cid]->OpenFile(
        (file_path + std::to_string(cid)).c_str(), true, true, true, true);
    D_ASSERT(rs == NOERROR);

    // Compute aligned file size
    int64_t alloc_file_size =
        ((alloc_size + sizeof(size_t) - 1 + 512) / 512) * 512;

    file_handlers[cid]->SetRequestedSize(alloc_size + sizeof(size_t));
    file_handlers[cid]->ReserveFileSize(alloc_file_size);
    file_handlers[cid]->SetCanDestroy(can_destroy);
    return rs;
}

void *ChunkCacheManager::MemAlign(uint8_t **ptr, size_t segment_size,
                                  size_t required_memory_size,
                                  Turbo_bin_aio_handler *file_handler)
{
    void *target_ptr = (void *)*ptr;
    std::align(512, segment_size, target_ptr, required_memory_size);
    // pa_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1);
    if (target_ptr == nullptr) {
        exit(-1);
        // TODO throw exception
    }

    size_t real_requested_segment_size = segment_size - sizeof(size_t);
    memcpy(target_ptr, &real_requested_segment_size, sizeof(size_t));
    *ptr = (uint8_t *)target_ptr;
    file_handler->SetDataPtr(*ptr);
    *ptr = *ptr + sizeof(size_t);

    return target_ptr;
}

void ChunkCacheManager::UnswizzleFlushSwizzle(
    ChunkID cid, Turbo_bin_aio_handler *file_handler)
{
    uint8_t *ptr;
    size_t size;

    /**
   * TODO we need a write lock
   * We flush in-memory data format to disk format.
   * Thus, unswizzling is needed.
   * However, since change the data in memory, we need to swizzle again.
   * After implementing eviction and so one, this code should be changed.
   * 
   * Ideal code is like below:
   * 1. Extent Manager write unswizzled data into memory
   * 2. ChunkCacheManager flush the data into disk, and remove all objects from store
   * 3. Proceeding clients can read the data from disk, and swizzle it.
  */
    PinSegment(cid, file_handler->GetFilePath(), &ptr, &size, false, false);
    CacheDataTransformer::Unswizzle(ptr);
    file_handler->FlushAllBlocking();
    file_handler->WaitAllPendingDiskIO(false);
    CacheDataTransformer::Swizzle(ptr);
    file_handler->Close();
    UnPinSegment(cid);
}

// ChunkCacheManager APIs
#ifdef USE_LIGHTNING_CLIENT
ReturnStatus ChunkCacheManager::PinSegment(ChunkID cid, std::string file_path,
                                           uint8_t **ptr, size_t *size,
                                           bool read_data_async,
                                           bool is_initial_loading)
{
    auto file_handler = GetFileHandler(cid);
    size_t segment_size = file_handler->GetRequestedSize();
    size_t file_size = file_handler->file_size();
    size_t required_memory_size =
        file_size + 512;  // Add 512 byte for memory aligning
    D_ASSERT(file_size >= segment_size);

    // Pin Segment using Lightning Get()
    if (client->Get(cid, ptr, size) != 0) {
        // Get() fail: 1) object not found, 2) object is not sealed yet
        if (client->Create(cid, ptr, required_memory_size) == 0) {
            // Align memory
            void *file_ptr =
                MemAlign(ptr, segment_size, required_memory_size, file_handler);

            // Read data & Seal object
            // TODO: we fix read_data_async as false, due to swizzling. May move logic to iterator.
            // ReadData(cid, file_path, file_ptr, file_size, read_data_async);
            ReadData(cid, file_path, file_ptr, file_size, false);

            if (!is_initial_loading) {
                CacheDataTransformer::Swizzle(*ptr);
            }
            client->Seal(cid);
            // if (!read_data_async) client->Seal(cid); // WTF???
            *size = segment_size - sizeof(size_t);
        }
        else {
            // Create fail -> Subscribe object
            client->Subscribe(cid);

            int status = client->Get(cid, ptr, size);
            D_ASSERT(status == 0);

            // Align memory & adjust size
            MemAlign(ptr, segment_size, required_memory_size, file_handler);
            *size = segment_size - sizeof(size_t);
        }
        return NOERROR;
    }

    // Get() success. Align memory & adjust size
    MemAlign(ptr, segment_size, required_memory_size, file_handler);
    *size = segment_size - sizeof(size_t);

    return NOERROR;
}

ReturnStatus ChunkCacheManager::UnPinSegment(ChunkID cid)
{
    // Unpin Segment using Lightning Release()
    // client->Release(cid);
    return NOERROR;
}

ReturnStatus ChunkCacheManager::SetDirty(ChunkID cid)
{
    // TODO: modify header information
    if (client->SetDirty(cid) != 0) {
        // TODO: exception handling
        exit(-1);
    }
    return NOERROR;
}

ReturnStatus ChunkCacheManager::DestroySegment(ChunkID cid)
{
    // TODO: Check the reference count
    // If the count > 0, we cannot destroy the segment

    // Delete the segment from the buffer using Lightning Delete()
    D_ASSERT(file_handlers.find(cid) != file_handlers.end());
    client->Delete(cid);
    file_handlers[cid]->Close();
    file_handlers[cid] = nullptr;
    return NOERROR;
}

ReturnStatus ChunkCacheManager::FlushDirtySegmentsAndDeleteFromcache()
{
    std::cout << "Start to flush file! Total # files = " << file_handlers.size()
              << std::endl;
    for (auto &file_handler : file_handlers) {
        if (file_handler.second == nullptr)
            continue;

        bool is_dirty;
        client->GetDirty(file_handler.first, is_dirty);
        if (!is_dirty)
            continue;

        // std::cout << "Flush file: " << file_handler.second->GetFilePath() << ", size: " << file_handler.second->file_size() << std::endl;
        // TODO we need a write lock
        UnswizzleFlushSwizzle(file_handler.first, file_handler.second);
        client->ClearDirty(file_handler.first);
        client->Delete(file_handler.first);
    }
    file_handlers.clear();
    return NOERROR;
}

int ChunkCacheManager::GetRefCount(ChunkID cid)
{
    return client->GetRefCount(cid);
}
#else
// std::pair<uint64_t, uint64_t> ChunkCacheManager::GetAndPinColumnChunk(
ReturnStatus ChunkCacheManager::PinSegment(ChunkID cid, std::string file_path,
                                           uint8_t **ptr, size_t *size,
                                           bool read_data_async,
                                           bool is_initial_loading)
{
    std::unique_lock<std::mutex> lock(cache_manager_mutex);
    auto file_handler = GetFileHandler(cid);
    size_t segment_size = file_handler->GetRequestedSize();
    size_t file_size = file_handler->file_size();
    size_t required_memory_size =
        file_size + 512;  // Add 512 byte for memory aligning
    D_ASSERT(file_size >= segment_size);
    auto column_chunk_status_it =
        column_chunk_cache_status_map.find(cid);
    if (column_chunk_status_it == column_chunk_cache_status_map.end()) {
        // Required chunk is not loaded in memory
        // If we have an admission manager, we should check whether we can admit
        column_chunk_cache_status_map[cid].first = ColumnChunkStatus::CACHING;
        auto offset_size = GetColumnChunkOffsetSize(cid, required_memory_size);
        *ptr = reinterpret_cast<uint8_t *>((size_t)shared_memory_addr +
                                           (size_t)offset_size.first);
        lock.unlock();

        // Align memory
        void *file_ptr =
            MemAlign(ptr, segment_size, required_memory_size, file_handler);

        // Read (disk I/O) data from file
        ReadData(cid, file_path, file_ptr, file_size, false);

        std::unique_lock<std::mutex> lock2(cache_manager_mutex);
        if (admission_manager && !admission_manager->Admit(cid)) {}
        auto insert_success = false;
        do {
            insert_success = Insert(cid);
            if (!insert_success) {
                lock2.unlock();
                // TODO_tjyoon: This is not a good way to wait for deadlock avoidance.
                // Maybe we need to use condition variable.
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                lock2.lock();
            }
        } while (!insert_success);
        Touch(cid);
        auto ret = column_chunk_offset_size_map[cid];
        // num_misses++;
        // byte_misses += ret.second;
        lock2.unlock();
        column_chunk_cache_cv.notify_all();
        // return ret;
        return DONE;
    }
    else if (column_chunk_status_it->second.first ==
             ColumnChunkStatus::CACHED) {
        Touch(cid);
        Pin(cid);
        // num_hits++;
        // byte_hits += column_chunk_offset_size_map[cid].second;
        // return column_chunk_offset_size_map[cid];
        return DONE;
    }
    else if (column_chunk_status_it->second.first ==
             ColumnChunkStatus::CACHING) {
        column_chunk_cache_cv.wait(lock, [&] {
            return column_chunk_cache_status_map[cid].first ==
                   ColumnChunkStatus::CACHED;
        });
        Touch(cid);
        Pin(cid);
        // num_hits++;
        // byte_hits += column_chunk_offset_size_map[cid].second;
        // return column_chunk_offset_size_map[cid];
        return DONE;
    }
    else {
        // throw
        // D_ASSERT(false, "Invalid column chunk status");
        // return std::make_pair(0, 0);
        return DONE;
    }
}

ReturnStatus ChunkCacheManager::UnPinSegment(ChunkID cid)
{
    std::unique_lock<std::mutex> lock(cache_manager_mutex);
    UnPin(cid);
    return ReturnStatus::DONE;
}

ReturnStatus ChunkCacheManager::SetDirty(ChunkID cid)
{
    // TODO
    return NOERROR;
}

ReturnStatus ChunkCacheManager::DestroySegment(ChunkID cid)
{
    // TODO: Check the reference count
    return NOERROR;
}

ReturnStatus ChunkCacheManager::FlushDirtySegmentsAndDeleteFromcache()
{
    return NOERROR;
}

int ChunkCacheManager::GetRefCount(ChunkID cid)
{
    return 0;
}
#endif

ReturnStatus ChunkCacheManager::CreateSegment(ChunkID cid,
                                              std::string file_path,
                                              size_t alloc_size,
                                              bool can_destroy)
{
    // Create file for the segment
    return CreateNewFile(cid, file_path, alloc_size, can_destroy);
}

ReturnStatus ChunkCacheManager::FinalizeIO(ChunkID cid, bool read, bool write)
{
    file_handlers[cid]->WaitForMyIoRequests(read, write);
    return NOERROR;
}

#ifdef USE_LIGHTNING_CLIENT
ChunkCacheManager::ChunkCacheManager(const char *path)
{
    // Init LightningClient
    client = new LightningClient("/tmp/lightning", "password");

    // Initialize file handlers
    if (std::filesystem::exists(std::string(path) + file_meta_info_name)) {
        InitializeFileHandlersUsingMetaInfo(path);
    }
    else {
        InitializeFileHandlersByIteratingDirectories(path);
    }
}

ChunkCacheManager::~ChunkCacheManager()
{
    fprintf(stdout, "Deconstruct ChunkCacheManager\n");
    for (auto &file_handler : file_handlers) {
        if (file_handler.second == nullptr)
            continue;

        bool is_dirty;
        client->GetDirty(file_handler.first, is_dirty);
        if (!is_dirty)
            continue;

        // std::cout << "Flush file: " << file_handler.second->GetFilePath() << ", size: " << file_handler.second->file_size() << std::endl;
        // TODO we need a write lock
        UnswizzleFlushSwizzle(file_handler.first, file_handler.second);
        client->ClearDirty(file_handler.first);
    }
}

#else
struct ColumnChunkIdentifierEquals {
    bool operator()(const ColumnChunkIdentifier &lhs,
                    const ColumnChunkIdentifier &rhs) const
    {
        return lhs.start_offset == rhs.start_offset &&
               lhs.origin_file_path == rhs.origin_file_path &&
               lhs.length == rhs.length;
    }
};

std::unordered_map<ChunkID, ColumnChunkIdentifier>
    ColumnChunkIdentifier::column_chunk_identifier_map;
uint64_t ColumnChunkIdentifier::column_chunk_id_counter;
std::mutex ColumnChunkIdentifier::column_chunk_id_mutex;

ColumnChunkIdentifier ColumnChunkIdentifier::GetColumnChunkIdentifier(
    ChunkID column_chunk_id)
{
    std::unique_lock<std::mutex> lock(column_chunk_id_mutex);
    return column_chunk_identifier_map[column_chunk_id];
}

ChunkCacheManager::ChunkCacheManager(const char *path)
{
    InitializeSharedMemory("/tmp/lightning", "password");
    max_cache_size_ = 100ul * 1024 * 1024 * 1024; // TODO
    current_cache_size_ = 0;
    current_shared_memory_offset = 0;
    eviction_manager =
        EvictionManagerFactory::CreateEvictionManager(EVICTION_ALGORITHM);
    eviction_manager->Init(max_cache_size_);
    admission_manager = nullptr;
}

ChunkCacheManager::~ChunkCacheManager()
{
    fprintf(stdout, "Deconstruct ChunkCacheManager\n");
    // TODO
}

void ChunkCacheManager::InitializeSharedMemory(const std::string &store_socket,
                                               const std::string &password)
{
    int store_conn_;
    int store_fd_;
    size_t size_;
    pid_t pid_;

    // setup unix domain socket with storage // copied from client.cc
    store_conn_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (store_conn_ < 0) {
        perror("cannot socket");
        exit(-1);
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    strncpy(addr.sun_path, store_socket.c_str(), store_socket.size());
    addr.sun_family = AF_UNIX;
    int status = connect(store_conn_, (struct sockaddr *)&addr, sizeof(addr));
    if (status < 0) {
        perror("cannot connect to the store");
        exit(-1);
    }

    pid_ = getpid();

    int nbytes_sent = send(store_conn_, &pid_, sizeof(pid_), 0);
    if (nbytes_sent != sizeof(pid_)) {
        perror("error sending pid");
        exit(-1);
    }

    int nbytes_recv = recv(store_conn_, &size_, sizeof(size_), 0);
    if (nbytes_recv != sizeof(size_)) {
        perror("error receiving the size of the object store");
        exit(-1);
    }

    int password_length = password.length();
    nbytes_sent =
        send(store_conn_, &password_length, sizeof(password_length), 0);
    if (nbytes_sent != sizeof(pid_)) {
        perror("error sending password length");
        exit(-1);
    }

    nbytes_sent = send(store_conn_, password.c_str(), password_length, 0);
    if (nbytes_sent != password_length) {
        perror("error sending password");
        exit(-1);
    }

    bool ok = false;

    nbytes_recv = recv(store_conn_, &ok, sizeof(bool), 0);
    if (nbytes_recv != sizeof(bool)) {
        perror("error receiving the ok bit");
        exit(-1);
    }

    if (!ok) {
        std::cerr << "authorization failure!" << std::endl;
        exit(-1);
    }

    store_fd_ = recv_file_descriptor(store_conn_);
    if (store_fd_ < 0) {
        perror("cannot open shared memory");
        exit(-1);
    }

    // TODO correctness check
    shared_memory_addr = mmap(nullptr, MAX_TOTAL_TABLE_SIZE,
                              PROT_READ | PROT_WRITE, MAP_SHARED, store_fd_, 0);
    // shared_memory_addr = mmap((void *)LIGHTNING_MMAP_ADDR, size_,
    //                           PROT_READ | PROT_WRITE, MAP_SHARED, -1, 0);
    // D_ASSERT(shared_memory_addr != MAP_FAILED, "Failed to map shared memory");
}

bool ChunkCacheManager::IsCached(ChunkID column_chunk_id)
{
    std::unique_lock<std::mutex> lock(cache_manager_mutex);
    return column_chunk_cache_status_map[column_chunk_id].first ==
           ColumnChunkStatus::CACHED;
}

uint64_t ChunkCacheManager::GetNumberOfCachedColumnChunks()
{
    std::unique_lock<std::mutex> lock(cache_manager_mutex);
    return column_chunk_cache_status_map.size();
}

bool ChunkCacheManager::Insert(ChunkID column_chunk_id)
{
    auto column_chunk_offset_size_it =
        column_chunk_offset_size_map.find(column_chunk_id);
    // D_ASSERT(column_chunk_offset_size_it != column_chunk_offset_size_map.end(),
    //          "Column chunk size is not found from map, which should already be "
    //          "inserted.");
    int64_t evict_size =
        (current_cache_size_ + column_chunk_offset_size_it->second.second) -
        max_cache_size_;
    if (evict_size > 0) {
        auto evict_success = Evict(evict_size);
        if (!evict_success) {
            return false;
        }
        // D_ASSERT(current_cache_size_ <= max_cache_size_,
        //          "Cache size is still greater than max after eviction");
    }
    current_cache_size_ += column_chunk_offset_size_it->second.second;
    eviction_manager->Insert(column_chunk_id);
    // D_ASSERT(column_chunk_cache_status_map[column_chunk_id].first ==
    //              ColumnChunkStatus::CACHING,
    //          "Column chunk status should be CACHING");
    column_chunk_cache_status_map[column_chunk_id].first =
        ColumnChunkStatus::CACHED;
    column_chunk_cache_status_map[column_chunk_id].second = 1;
    if (admission_manager)
        admission_manager->Insert(column_chunk_id);
    // D_ASSERT(current_cache_size_ == eviction_manager->cache_->get_occupied_byte(
    //                                     eviction_manager->cache_),
    //          "Cache size is not consistent between cache manager and eviction "
    //          "manager");
    return true;
}

void ChunkCacheManager::Remove(ChunkID column_chunk_id)
{
    auto column_chunk_offset_size_it =
        column_chunk_offset_size_map.find(column_chunk_id);
    // D_ASSERT(column_chunk_offset_size_it != column_chunk_offset_size_map.end(),
    //          "Column chunk does not exist");
    current_cache_size_ -= column_chunk_offset_size_it->second.second;
    column_chunk_cache_status_map.erase(column_chunk_id);
    if (admission_manager)
        admission_manager->Remove(column_chunk_id);
    auto res =
        madvise((void *)((size_t)column_chunk_offset_size_it->second.first +
                         (size_t)shared_memory_addr),
                column_chunk_offset_size_it->second.second, MADV_DONTNEED);
    // D_ASSERT(res == 0, "madvise failed");
}

bool ChunkCacheManager::Evict(uint64_t size_to_evict)
{
    auto victim = eviction_manager->Evict(size_to_evict);
    for (auto &column_chunk_id : *victim) {
        if (column_chunk_id == -1) {
            return false;
        }
        // num_evictions++;
        // byte_evictions += column_chunk_offset_size_map[column_chunk_id].second;
        Remove(column_chunk_id);
    }
    return true;
}

void ChunkCacheManager::Pin(ChunkID column_chunk_id)
{
    if (admission_manager) {
        admission_manager->Pin(column_chunk_id);
    }
    eviction_manager->Pin(column_chunk_id);
    column_chunk_cache_status_map[column_chunk_id].second++;
}

void ChunkCacheManager::UnPin(ChunkID column_chunk_id)
{
    if (admission_manager) {
        admission_manager->UnPin(column_chunk_id);
    }
    eviction_manager->UnPin(column_chunk_id);
    column_chunk_cache_status_map[column_chunk_id].second--;
}

void ChunkCacheManager::Touch(ChunkID column_chunk_id)
{
    if (admission_manager) {
        admission_manager->Touch(column_chunk_id);
    }
    eviction_manager->Touch(column_chunk_id);
}

std::pair<uint64_t, uint64_t> ChunkCacheManager::GetColumnChunkOffsetSize(
    ChunkID column_chunk_id, uint64_t size)
{
    auto column_chunk_addr_offset_it =
        column_chunk_offset_size_map.find(column_chunk_id);
    if (column_chunk_addr_offset_it == column_chunk_offset_size_map.end()) {
        // round up to COLUMN_CHUNK_ALIGN_SIZE
        size = (size + COLUMN_CHUNK_ALIGN_SIZE - 1) / COLUMN_CHUNK_ALIGN_SIZE *
               COLUMN_CHUNK_ALIGN_SIZE;
        column_chunk_offset_size_map[column_chunk_id] =
            std::make_pair(current_shared_memory_offset, size);
        current_shared_memory_offset += size;
        return column_chunk_offset_size_map[column_chunk_id];
    }
    else {
        return column_chunk_offset_size_map[column_chunk_id];
    }
}

void ChunkCacheManager::DownloadColumnChunkIntoSharedMemory(
    ChunkID column_chunk_id, std::pair<uint64_t, uint64_t> offset_size)
{
    // auto column_chunk_identifier =
    //     ColumnChunkIdentifier::GetColumnChunkIdentifier(column_chunk_id);
    // auto file =
    //     GetOrOpenRandomAccessFile(column_chunk_identifier.origin_file_path);
    // // D_ASSERT(offset_size.second >= column_chunk_identifier.length,
    // //          "Reserved size is smaller than the column chunk size");
    // try {
    //     auto res =
    //         file->ReadAt(column_chunk_identifier.start_offset,
    //                      column_chunk_identifier.length,
    //                      reinterpret_cast<void *>((size_t)offset_size.first +
    //                                               (size_t)shared_memory_addr));
    //     // D_ASSERT(res.ok(), "Failed to read file");
    // }
    // catch (const std::exception &e) {
    //     // D_ASSERT(false, "Failed to read file");
    // }
}

// void ChunkCacheManager::PrintColumnChunkCacheStatistics()
// {
//     std::string log_file_name =
//         RequestHistoryLogger::getInstance().generateLogFileName();

//     std::ostringstream oss;
//     // Accumulate the data in the string stream
//     for (const auto &pair : column_chunk_request_history) {
//         oss << pair.first << "," << pair.second << "\n";
//     }
//     // Write the entire content to the file at once
//     std::ofstream file(log_file_name, std::ios::out | std::ios::binary);
//     file << EVICTION_ALGORITHM << "\n";
//     file << "Cache size: " << TOTAL_CACHE_SIZE << "\n";
//     file << ADDITIONAL_INFO << "\n";
//     file << "Num_hits: " << num_hits << "\n";
//     file << "Num_misses: " << num_misses << "\n";
//     file << "Num_evictions: " << num_evictions << "\n";
//     file << "Byte_hits: " << byte_hits << "\n";
//     file << "Byte_misses: " << byte_misses << "\n";
//     file << "Byte_evictions: " << byte_evictions << "\n";
//     file.write(oss.str().c_str(), oss.str().size());
//     file.close();
// }

// void ChunkCacheManager::PrintPinStatus()
// {
//     std::unique_lock<std::mutex> lock(cache_manager_mutex);
//     for (const auto &pair : column_chunk_cache_status_map) {
//         // if (pair.second.second == 0 && eviction_manager->IsPinned(pair.first)) {
//         //   printf(
//         //       "ERROR!! Column chunk %lu of size %lu is noted as pinned in "
//         //       "algorithm but not pinned in ChunkCacheManager\n",
//         //       pair.first, column_chunk_offset_size_map[pair.first].second);
//         // }
//         // if (pair.second.second > 0)
//         //   printf("Column chunk %lu of size %lu is pinned %d\n", pair.first,
//         //          column_chunk_offset_size_map[pair.first].second,
//         //          pair.second.second);
//         printf(
//             "Column chunk %lu of size %lu is pinned in CacheManager: %d, "
//             "ispinned: "
//             "%d\n",
//             pair.first, column_chunk_offset_size_map[pair.first].second,
//             pair.second.second, (int)eviction_manager->IsPinned(pair.first));
//     }
// }
#endif

}  // namespace duckdb
