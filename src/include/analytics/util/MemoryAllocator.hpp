#pragma once

class MemoryAllocator {
  public:

	MemoryAllocator() {}

	virtual void Close(bool need_free = true) = 0;
	virtual void Initialize (int64_t block_memory_size) = 0;
	virtual void Initialize (char* ptr, int64_t capacity) = 0;
	virtual char* Allocate (int64_t size) = 0;
	virtual char* TryAllocate (int64_t size) { return NULL; };
	virtual void Free (char* ptr) = 0;
	virtual void Free (char* ptr, int64_t size) = 0;
	virtual int64_t GetSize() = 0;
	virtual int64_t GetRemainingBytes() = 0 ;
  private:
};
