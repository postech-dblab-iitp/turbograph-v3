#pragma once

/*
 * Design of the ParallelBinaryHandlers
 *
 * The classes in this file provides functions that perform I/O on binary files 
 * and return their values. In order to perform I/O efficiently, double buffering 
 * is used. While input binary data is loaded into one buffer and processed, the 
 * other buffer performs I/O asynchronously. After processing all the loaded data, 
 * it waits for all I/Os to be performed on the other buffer. This process is 
 * performed repeatedly, alternating between the two buffers.
 *
 * There are three classes in this file. The differences are as follows:
 *     BinaryDispenser: Open binary file using ifstream.
 *     BinaryListDispenser: Open binary file using Splittable_Turbo_bin_io_handler.
 *     BinaryCollector: The above two classes are used for reading input file.
 *                      The BinaryCollector class is used for writing output file.
 */

#include <vector>
#include <fstream>
#include <string>
#include "Splittable_Turbo_bin_io_handler.hpp"
#include "turbo_dist_internal.hpp"
#include "tbb/atomic.h"
#include "tbb/task_group.h"

template <typename T>
class BinaryDispenser {
  public:
	/*
	* Construct this dispenser for the given file
	*/
	BinaryDispenser(std::string filename, size_t unit_size,
	                bool delete_after_read) : file_name(filename),
		buffer_size(unit_size),
		delete_original(delete_after_read),
		remaining(0) {
		buffer.resize(buffer_size * BUFFER_COUNT);
		buf_begin = buffer.data();
		buf_end = buffer.data() + buffer.size();
		buf_checkpoint = buffer.data();
		position = buf_begin - 1;
		output = &position;

		/* Make the reader thread to read the data from the file */
		read_tasks.run([&] {
			file.open(file_name, std::ios::in | std::ios::binary);
			file.read((char*)buf_begin, buffer_size * sizeof(T));
			remaining += file.gcount() / sizeof(T);
		});
	}
	/* Deconstruct this dispenser */
	~BinaryDispenser() {
		close();
	}

	/* 
	* Move to the next item 
	*/
	bool forward() {
		/* If the buffer is empty, then wait read tasks */
		if (remaining == 0) {
			read_tasks.wait();
			if (remaining == 0)
				return false;
		}
		remaining--;
		position++;
		if (position == buf_end) position = buf_begin;
		/* If the postion is at the checkpoint, then makes the reader thread to read next data */
		if (position == buf_checkpoint) read_tasks.run([&] { read_next(); });
		return true;
	}

	/*
	* Close this dispenser
	*/
	bool close() {
		read_tasks.wait();
		file.close();
		if (delete_original) remove(file_name.c_str());
	}

	/*
	* Get the pointer to the data
	*/
	T** get_output() const {
		return output;
	}
  private:

	/*
	* Read the data from file and write the data to the buffer
	*/ 
	void read_next() {
		if (file.eof()) return;
		T* save_target = buf_checkpoint + buffer_size;
		if (save_target == buf_end) save_target = buf_begin;
		file.read((char*)save_target, buffer_size * sizeof(T));
		buf_checkpoint = save_target;
		remaining += file.gcount() / sizeof(T);
	}

	std::string file_name; /* the file name that this dispenser accesses */
	std::ifstream file; 
	std::vector<T> buffer; /* the buffer */
	T* buf_begin;
	T* buf_end;
	T* position; /* the position of the data that dispenser can read */
	T** output; /* the pointer to the data that dispenser can read */
	tbb::atomic<T*> buf_checkpoint;
	tbb::atomic<size_t> remaining; /* the size of remaining data */
	tbb::task_group read_tasks;
	bool delete_original;
	size_t buffer_size;
	static const size_t BUFFER_COUNT = 2; //DO_NOT_CHANGE
};

template <typename T>
class BinaryListDispenser {
  public:

	/* Construct this dispenser for the given file */
	BinaryListDispenser(std::string filename, size_t unit_size,
	                    bool delete_after_read) : buffer_size(unit_size),
		file_name(filename),
		delete_original(delete_after_read),
		offset(0),
		remaining(0) {
		
		/* Prepare the buffer */
		buffer.resize(buffer_size * BUFFER_COUNT);
		buf_begin = buffer.data();
		buf_end = buffer.data() + buffer.size();
		buf_checkpoint = buffer.data();
		position = buf_begin - 1;
		output = &position;

		/* Make a reader thread to read data from the file and write to the buffer */
		read_tasks.run([&] {
			file.OpenFile(file_name.c_str());
			auto read = file.Read_upto(offset, buffer_size * sizeof(T), (char*)buf_begin, delete_original);

			offset += read;
			remaining += read / sizeof(T);
		});
	}

	/* Deconstruct this dispenser */
	~BinaryListDispenser() {
		close();
	}

	/* 
	* Move the pointer to next data
	*/
	bool forward() {
		if (remaining == 0) {
			read_tasks.wait();
			if (remaining == 0)
				return false;
		}
		remaining--;
		position++;
		if (position == buf_end) position = buf_begin;
		if (position == buf_checkpoint) read_tasks.run([&] { read_next(); });
		return true;
	}

	/*
	* Close this dispenser
	*/
	bool close() {
		read_tasks.wait();
		file.Close(delete_original);
	}

	/*
	* Get the pointer to the data that this dispenser reads
	*/
	T** get_output() const {
		return output;
	}
  private:
	/*
	* Read the next data from the file and write to the buffer
	*/
	void read_next() {
		if (offset >= file.file_size()) return;
		T* save_target = buf_checkpoint + buffer_size;
		if (save_target == buf_end) save_target = buf_begin;
		auto read = file.Read_upto(offset, buffer_size * sizeof(T), (char*)save_target, delete_original);
		offset += read;
		buf_checkpoint = save_target;
		remaining += read / sizeof(T);
	}

	std::string file_name; /* file name that this dispender reads */
	Splittable_Turbo_bin_io_handler file;
	std::vector<T> buffer; /* the buffer that contains the data from file */
	T* buf_begin; 
	T* buf_end;
	T* position;
	T** output;
	tbb::atomic<T*> buf_checkpoint;
	tbb::atomic<size_t> remaining; /* the size of remaining data in the buffer */
	tbb::task_group read_tasks;
	bool delete_original;
	size_t offset;
	size_t buffer_size;
	static const size_t BUFFER_COUNT = 2; //DO_NOT_CHANGE
};

template <typename T>
class BinaryCollector {
  public:

	/* 
	* Construct this collector for the given file name 
	*/
	BinaryCollector(std::string filename,
	                size_t unit_size) : file_name(filename),
		buffer_size(unit_size) {
		
		/* Prepare the buffer */
		buffer.resize(buffer_size * BUFFER_COUNT);
		buf_begin = buffer.data();
		buf_end = buffer.data() + buffer.size();
		buf_checkpoint = buffer.data() + buffer_size;
		position = buf_begin;
		input = &position;

		/* Make the writer thread to write data to file */
		write_tasks.run([&] { file.open(file_name, std::ios::out | std::ios::binary); });
	}
	~BinaryCollector() {
		close();
	}

	/*
	* Move the next position at buffer
	*/
	void forward() {
		position++;
		if (position == buf_end) position = buf_begin;

		/*
		* If the position at buffer checkpoint, 
		* then Make the writer thread to write data to file
		*/
		if (position == buf_checkpoint) {
			write_tasks.wait();
			buf_checkpoint += buffer_size;
			if (buf_checkpoint == buf_end) buf_checkpoint = buf_begin;
			write_tasks.run([&] { write(); });
		}
	}

	/*
	* flush the whole data in the buffer without using the writer thread
	*/
	void flush() {
		write_tasks.wait();
		T* write_until = position;
		position = buf_checkpoint;
		buf_checkpoint += buffer_size;
		if (buf_checkpoint == buf_end) buf_checkpoint = buf_begin;
		T* write_from = buf_checkpoint;
		file.write((char*)write_from, (write_until - write_from) * sizeof(T));
	}
	/*
	* Close this collector
	* It flush the data to the file
	*/
	bool close() {
		flush();
		file.close();
	}

	/*
	* Get the pointer to the data
	*/
	T** get_input() const {
		return input;
	}
  private:

	/*
	* Write the data in the buffer to the file
	*/
	void write() {
		T* write_from = buf_checkpoint;
		file.write((char*)write_from, buffer_size * sizeof(T));
	}

	std::string file_name; /* the file name */
	std::ofstream file;
	std::vector<T> buffer; /* the buffer */
	T* buf_begin;
	T* buf_end;
	T** input;
	T* position;
	tbb::atomic<T*> buf_checkpoint;
	tbb::task_group write_tasks;
	size_t buffer_size;
	static const size_t BUFFER_COUNT = 2; //DO_NOT_CHANGE
};

