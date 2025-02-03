#ifndef BLOCKING_TURBO_BIN_IO_HANDLER_H
#define BLOCKING_TURBO_BIN_IO_HANDLER_H

/* 
 * Design of the Blocking_Turbo_bin_io_handler
 *
 * This class additionally implements the lock function in Turbo_bin_io_handler. 
 * It acquires a lock before executing each operation of Write, Read, and Append, 
 * and releases it after performing the operation. This class can be useful 
 * in situations in which several threads perform operations such as Append or
 * Write at the same time.
 *
 */

#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"
#include "analytics/util/atom.hpp"

class Blocking_Turbo_bin_io_handler {
  public:
	Blocking_Turbo_bin_io_handler() {
		handler = new Turbo_bin_io_handler();
	}

	Blocking_Turbo_bin_io_handler(Turbo_bin_io_handler* handler_) {
		handler = handler_;
	}
	
    Blocking_Turbo_bin_io_handler(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false) {
		handler = new Turbo_bin_io_handler(file_name, create_if_not_exist, write_enabled, delete_if_exist);
    }

	~Blocking_Turbo_bin_io_handler() {
		handler->Close();
	}

	void Close(bool rm = false) {
		handler->Close(rm);
	}

	ReturnStatus OpenFile(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false) {
		return handler->OpenFile(file_name, create_if_not_exist, write_enabled, delete_if_exist);
	}

	void Write(size_t offset_to_write, size_t size_to_write, char* data) {
		lock.lock();
		handler->Write(offset_to_write, size_to_write, data);
		lock.unlock();
	}

	void Read(size_t offset_to_read, size_t size_to_read, char* data) {
		lock.lock();
		handler->Read(offset_to_read, size_to_read, data);
		lock.unlock();
	}

	void Append(size_t size_to_append, char* data) {
		lock.lock();
		handler->Append(size_to_append, data);
		lock.unlock();
	}

	int64_t file_size() { return handler->file_size(); }
	char* CreateMmap(bool write_enabled) { return handler->CreateMmap(write_enabled); }
	void DestructMmap() { handler->DestructMmap(); }

  private:
	Turbo_bin_io_handler* handler;
	atom lock;
};

// template <typename T>
// class Fake_Blocking_Turbo_bin_io_handler {
//   public:
// 	Fake_Blocking_Turbo_bin_io_handler() {
// 		handler = new Fake_Turbo_bin_io_handler<T>();
// 	}

// 	Fake_Blocking_Turbo_bin_io_handler(Fake_Turbo_bin_io_handler<T>* handler_) {
// 		handler = handler_;
// 	}
	
//     Fake_Blocking_Turbo_bin_io_handler(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false) {
// 		handler = new Fake_Turbo_bin_io_handler<T>(file_name, create_if_not_exist, write_enabled, delete_if_exist);
//     }

// 	~Fake_Blocking_Turbo_bin_io_handler() {
// 		handler->Close();
// 	}

// 	void Close(bool rm = false) {
// 		handler->Close(rm);
// 	}

// 	ReturnStatus OpenFile(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false) {
// 		return handler->OpenFile(file_name, create_if_not_exist, write_enabled, delete_if_exist);
// 	}

// 	void Write(size_t offset_to_write, size_t size_to_write, char* data) {
// 		lock.lock();
// 		handler->Write(offset_to_write, size_to_write, data);
// 		lock.unlock();
// 	}

// 	void Read(size_t offset_to_read, size_t size_to_read, char* data) {
// 		lock.lock();
// 		handler->Read(offset_to_read, size_to_read, data);
// 		lock.unlock();
// 	}

// 	void Append(size_t size_to_append, char* data) {
// 		lock.lock();
// 		handler->Append(size_to_append, data);
// 		lock.unlock();
// 	}

// 	int64_t file_size() { return handler->file_size(); }
// 	char* CreateMmap(bool write_enabled) { return handler->CreateMmap(write_enabled); }
// 	void DestructMmap() { handler->DestructMmap(); }

//   private:
// 	Fake_Turbo_bin_io_handler<T>* handler;
// 	atom lock;
// };

#endif

