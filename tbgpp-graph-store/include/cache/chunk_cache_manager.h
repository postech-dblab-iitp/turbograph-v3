#ifndef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>
#include <mutex>
#include <condition_variable>
#include "common/unordered_map.hpp"

#include "cache/client.h"
#include "cache/common.h"
#include "cache/disk_aio/Turbo_bin_aio_handler.hpp"
#include "common/constants.hpp"

namespace duckdb {
#define NUM_MAX_SEGMENTS 65536
#define USE_LIGHTNING_CLIENT  // if not defined, use vmcache
}  // namespace duckdb

#ifndef USE_LIGHTNING_CLIENT
namespace duckdb {
// MEMO
// - Why use both ColumnChunkIdentifier and ChunkID?
//   - For the replacemet algorithm side, the objects are identified by just a
//   single ID, thus minimizing the library code changes.
class ColumnChunkIdentifier {
   public:
    ColumnChunkIdentifier() = default;
    ColumnChunkIdentifier(std::string origin_file_path, uint64_t start_offset,
                          uint64_t length)
        : origin_file_path(origin_file_path),
          start_offset(start_offset),
          length(length)
    {}
    ColumnChunkIdentifier(const ColumnChunkIdentifier &column_chunk_id)
        : origin_file_path(column_chunk_id.origin_file_path),
          start_offset(column_chunk_id.start_offset),
          length(column_chunk_id.length)
    {}
    std::string origin_file_path;
    uint64_t start_offset;
    uint64_t length;

    static ColumnChunkIdentifier GetColumnChunkIdentifier(ChunkID);
    static std::unordered_map<ChunkID, ColumnChunkIdentifier>
        column_chunk_identifier_map;
    static uint64_t column_chunk_id_counter;
    static std::mutex column_chunk_id_mutex;
    bool operator==(const ColumnChunkIdentifier &other) const
    {
        return origin_file_path == other.origin_file_path &&
               start_offset == other.start_offset && length == other.length;
    }
};
}  // namespace duckdb

namespace std {
template <>
struct hash<duckdb::ColumnChunkIdentifier> {
    uint64_t operator()(
        const duckdb::ColumnChunkIdentifier &column_chunk_id) const
    {
        return 0; // TODO
        // return duckdb::CombineHashes(
        //     std::hash<std::string>{}(column_chunk_id.origin_file_path),
        //     duckdb::CombineHashes(
        //         std::hash<uint64_t>{}(column_chunk_id.length),
        //         std::hash<uint64_t>{}(column_chunk_id.start_offset)));
    }
};

}  // namespace std

#endif

namespace duckdb {

#ifndef USE_LIGHTNING_CLIENT
class AdmissionManager;
class EvictionManager;

// To avoid downlaoding the same column chunk twice, use CACHING status.
// When we wait for the column chunk to be cached, should release lock.
enum class ColumnChunkStatus { CACHED, CACHING, TEMPORAL };
#endif

class ChunkCacheManager {
   public:
    static ChunkCacheManager *ccm;

#ifndef USE_LIGHTNING_CLIENT
using ColumnChunkAddrSizeMap =
        std::unordered_map<ChunkID, std::pair<uint64_t, uint64_t>>;
    using ColumnChunkCacheStatusMap =
        std::unordered_map<ChunkID,
                           std::pair<ColumnChunkStatus, int32_t>>;
#endif

   public:
    ChunkCacheManager(const char *path);
    ~ChunkCacheManager();
    ChunkCacheManager(ChunkCacheManager const &) = delete;

    void InitializeFileHandlersByIteratingDirectories(const char *path);
    void InitializeFileHandlersUsingMetaInfo(const char *path);
    void FlushMetaInfo(const char *path);

    // ChunkCacheManager APIs
    ReturnStatus PinSegment(ChunkID cid, std::string file_path, uint8_t **ptr,
                            size_t *size, bool read_data_async = false,
                            bool is_initial_loading = false);
    ReturnStatus UnPinSegment(ChunkID cid);
    ReturnStatus SetDirty(ChunkID cid);
    ReturnStatus CreateSegment(ChunkID cid, std::string file_path,
                               size_t alloc_size, bool can_destroy);
    ReturnStatus DestroySegment(ChunkID cid);
    ReturnStatus FinalizeIO(ChunkID cid, bool read = true, bool write = true);
    ReturnStatus FlushDirtySegmentsAndDeleteFromcache();

    // APIs for Debugging purpose
    int GetRefCount(ChunkID cid);

    // ChunkCacheManager Internal Functions
    Turbo_bin_aio_handler *GetFileHandler(ChunkID cid);
    void ReadData(ChunkID cid, std::string file_path, void *ptr,
                  size_t size_to_read, bool read_data_async);
    void WriteData(ChunkID cid);
    ReturnStatus CreateNewFile(ChunkID cid, std::string file_path,
                               size_t alloc_size, bool can_destroy);
    void *MemAlign(uint8_t **ptr, size_t segment_size,
                   size_t required_memory_size,
                   Turbo_bin_aio_handler *file_handler);

    void UnswizzleFlushSwizzle(ChunkID cid,
                               Turbo_bin_aio_handler *file_handler);

#ifdef USE_LIGHTNING_CLIENT    
   public:
    // Member Variables
    LightningClient *client;
#else
public:
    // Thread safe methods.
    void InitializeSharedMemory(const std::string &store_socket,
                                 const std::string &password);
    // std::pair<uint64_t, uint64_t> GetAndPinColumnChunk(ChunkID);
    // void UnPinColumnChunk(ChunkID);
    bool IsCached(ChunkID);

    uint64_t GetMaxCacheSize() const { return max_cache_size_; }
    uint64_t GetCurrentCacheSize() const { return current_cache_size_; }
    uint64_t GetFreeCacheSize() const
    {
        return max_cache_size_ - current_cache_size_;
    }
    uint64_t GetNumberOfCachedColumnChunks();
    std::pair<uint64_t, uint64_t> GetColumnChunkOffsetSize(
        ChunkID column_chunk_id, uint64_t size);
    // Will be invoked when SIGUSR1 is received. Should be registered in the
    // crystal_main.cpp.
    // void PrintColumnChunkCacheStatistics();
    // void PrintPinStatus();
private:
    // Internal functions should not acquire lock.
    bool Insert(ChunkID);  // This can fail.
    void Remove(ChunkID);
    bool Evict(uint64_t);  // This can fail.
    void Pin(ChunkID);
    void UnPin(ChunkID);
    void Touch(ChunkID);
    void DownloadColumnChunkIntoSharedMemory(ChunkID,
                                             std::pair<uint64_t, uint64_t>);

    uint64_t max_cache_size_;
    uint64_t current_cache_size_ = 0;
    void *shared_memory_addr = nullptr;
    uint64_t current_shared_memory_offset = 0;
    // This stores a mapping from column chunk id to the address and size of the
    // column chunk in the shared memory. This is not erased after the column
    // chunk has been evicted.
    ColumnChunkAddrSizeMap column_chunk_offset_size_map;
    // This stores a mapping from column chunk id to a boolean value indicating
    // whether the column chunk is currently cached, not cached, or being
    // cached.
    ColumnChunkCacheStatusMap column_chunk_cache_status_map;
    std::condition_variable column_chunk_cache_cv;
    std::mutex cache_manager_mutex;
    std::shared_ptr<AdmissionManager> admission_manager = nullptr;
    std::shared_ptr<EvictionManager> eviction_manager = nullptr;
#if RECORD_STATISTICS
    std::vector<std::pair<uint64_t, uint64_t>> column_chunk_request_history;
    uint64_t num_hits = 0;
    uint64_t num_misses = 0;
    uint64_t num_evictions = 0;
    uint64_t byte_hits = 0;
    uint64_t byte_misses = 0;
    uint64_t byte_evictions = 0;
#endif
#endif

    unordered_map<ChunkID, Turbo_bin_aio_handler *> file_handlers;
    const std::string file_meta_info_name = ".file_meta_info";
    const std::string EVICTION_ALGORITHM = "LRU";
};

}  // namespace duckdb

#endif  // CHUNK_CACHE_MANAGER_H
