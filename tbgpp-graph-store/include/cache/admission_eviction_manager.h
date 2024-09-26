#ifndef _ADMISSION_EVICTION_MANAGER_H_
#define _ADMISSION_EVICTION_MANAGER_H_

#include "cache/chunk_cache_manager.h"
#include "cache/common.h"

#include "libCacheSim.h"

namespace duckdb {

#ifndef USE_LIGHTNING_CLIENT
// I expect admission and eviction decisions will be fast, so do not try to make
// concurrent code.
class AdmissionManager {
   public:
    virtual void Init() = 0;
    virtual bool Admit(ChunkID) = 0;
    virtual void Touch(ChunkID) = 0;
    virtual void Insert(ChunkID) = 0;
    virtual void Remove(ChunkID) = 0;
    virtual void Pin(ChunkID) = 0;
    virtual void UnPin(ChunkID) = 0;
};

enum class EvictionPolicy : uint8_t {
    LRU,
    ARC,
    LIRS,
    SIEVE,
    LECARE,
    LRB,
    GDSF,
    LHD,
    LFUDA,
    HYPERBOLIC,
    S4LRU,
    LFU,
    LRUSP,
    LRU2,
};

class EvictionManager {
   public:
    virtual void Init(uint64_t cache_size) = 0;
    virtual void Touch(ChunkID);                                  // find
    virtual void Insert(ChunkID);                                 // insert
    virtual std::shared_ptr<std::vector<ChunkID>> Evict(size_t);  // evict
    virtual void Pin(ChunkID);
    virtual void UnPin(ChunkID);
    virtual bool IsPinned(ChunkID);  // This might not work for some algorithms.
                                     //  protected:
    cache_t *cache_;
    EvictionPolicy policy_;
    uint64_t cache_size_;
};

class EvictionManagerFactory {
   public:
    static std::shared_ptr<EvictionManager> CreateEvictionManager(
        const std::string &eviction_manager_name);
};

void SetCommonRequestInfo(request_t &request);

class LRUEvictionManager : public EvictionManager {
   public:
    LRUEvictionManager() = default;
    void Init(uint64_t cache_size) override;
};

// class ARCEvictionManager : public EvictionManager {
//  public:
//   ARCEvictionManager() = default;
//   void Init() override;
// };

// class LIRSEvictionManager : public EvictionManager {
//  public:
//   LIRSEvictionManager() = default;
//   void Init() override;
// };

// class SieveEvictionManager : public EvictionManager {
//  public:
//   SieveEvictionManager() = default;
//   void Init() override;
// };

// class LeCaREvictionManager : public EvictionManager {
//  public:
//   LeCaREvictionManager() = default;
//   void Init() override;
// };

// class LRBEvictionManager : public EvictionManager {
//  public:
//   LRBEvictionManager() = default;
//   void Init() override;
//   void Insert(ChunkID) override;
// };

// class GDSFEvictionManager : public EvictionManager {
//  public:
//   GDSFEvictionManager() = default;
//   void Init() override;
// };

// class LHDEvictionManager : public EvictionManager {
//  public:
//   LHDEvictionManager() = default;
//   void Init() override;
// };

// class LFUDAEvictionManager : public EvictionManager {
//  public:
//   LFUDAEvictionManager() = default;
//   void Init() override;
// };

// class HyperbolicEvictionManager : public EvictionManager {
//  public:
//   HyperbolicEvictionManager() = default;
//   void Init() override;
// };

// class S4LRUEvictionManager : public EvictionManager {
//  public:
//   S4LRUEvictionManager() = default;
//   void Init() override;
//   void Pin(ChunkID) override;
//   void UnPin(ChunkID) override;
// };

// class LFUEvictionManager : public EvictionManager {
//  public:
//   LFUEvictionManager() = default;
//   void Init() override;
// };

// class LRUSPEvictionManager : public EvictionManager {
//  public:
//   LRUSPEvictionManager() = default;
//   void Init() override;
// };

// class LRU2EvictionManager : public EvictionManager {
//  public:
//   LRU2EvictionManager() = default;
//   void Init() override;
// };

#endif

}  // namespace duckdb
#endif