#ifndef CLIENT_H
#define CLIENT_H
#include <string>
#include <vector>

#include "config.h"
#include "storage/cache/log_disk.h"
#include "malloc.h"
#include "storage/cache/object_log.h"

class Turbo_bin_aio_handler;

class LightningClient {
public:
  LightningClient(const std::string &store_socket, const std::string &password, bool standalone=false);

  void InitializeWithDemon(const std::string &store_socket, const std::string &password);
  void InitializeStandalone();

  ~LightningClient();

  int MultiPut(uint64_t object_id, std::vector<std::string> fields,
               std::vector<int64_t> subobject_sizes,
               std::vector<uint8_t *> subobjects);

  int MultiGet(uint64_t object_id, std::vector<std::string> in_fields,
               std::vector<int64_t> *out_field_sizes,
               std::vector<uint8_t *> *out_fields,
               std::vector<int64_t> *subobject_sizes,
               std::vector<uint8_t *> *subobjects);

  int MultiUpdate(uint64_t object_id, std::vector<std::string> fields,
                  std::vector<int64_t> subobject_sizes,
                  std::vector<uint8_t *> subobjects);

  int Create(uint64_t object_id, uint8_t **ptr, size_t size);

  int Seal(uint64_t object_id);

  int SetDirty(uint64_t object_id);

  int ClearDirty(uint64_t object_id);

  int GetDirty(uint64_t object_id, bool& is_dirty);

  int Get(uint64_t object_id, uint8_t **ptr, size_t *size);

  int Release(uint64_t object_id);

  int Delete(uint64_t object_id);

  int Flush(uint64_t object_id, Turbo_bin_aio_handler* file_handler);

  int Subscribe(uint64_t object_id);

  // tslee added for debugging purpose
  int GetRefCount(uint64_t object_id);
  void PrintRemainingMemory();

  size_t GetRemainingMemory();

private:
  int store_conn_;
  int store_fd_;
  int log_fd_;

  LightningStoreHeader *header_;
  size_t size_;
  MemAllocator *allocator_;

  int64_t alloc_object_entry();
  void dealloc_object_entry(int64_t object_index);

  int64_t find_object(uint64_t object_id);
  int create_internal(uint64_t object_id, sm_offset *offset, size_t size);
  int get_internal(uint64_t object_id, sm_offset *ptr, size_t *size);
  int seal_internal(uint64_t object_id);
  int set_dirty_internal(uint64_t object_id);
  int clear_dirty_internal(uint64_t object_id);
  int get_dirty_internal(uint64_t object_id, bool& is_dirty);
  int get_refcount_internal(uint64_t object_id);
  int delete_internal(uint64_t object_id);
  int flush_internal(uint64_t object_id, Turbo_bin_aio_handler* file_handler);
  int subscribe_internal(uint64_t object_id, sem_t **sem, bool *wait);
  void init_mpk();

  uint64_t object_id_from_str(const std::string &s);

  uint8_t *base_;

  int object_log_fd_;
  uint8_t *object_log_base_;
  ObjectLog *object_log_;
  pid_t pid_;

  UndoLogDisk *disk_;
};

#endif // CLIENT_H
