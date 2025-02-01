#pragma once

/*
 * Design of the Splittable_Turbo_bin_io_handler
 *
 * This class provides the same set of API as Turbo_bin_io_handler, and can be 
 * used the same as Turbo_bin_io_handler even if the user does not know the 
 * internal operation process. The difference is that Turbo_bin_io_handler 
 * manages data as a single binary file, whereas this class manages binary files 
 * by splitting them into chunks of the size specified in FILE_CHUNK_SIZE.
 *
 * The advantage of managing files in this way can be taken when the size of the 
 * temporary intermediate results that occur during the data processing are large. 
 * Suppose you manage files using Turbo_bin_io_handler. Even if the temporary 
 * intermediate result file can be read once and deleted, it continues to take up 
 * disk space because the file cannot be deleted until the entire file has been 
 * read. Splittable_Turbo_bin_io_handler reads a file in chunks and deletes it 
 * immediately, so it is useful when the available disk space is small.
 */

#include <streambuf>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <glob.h>

#include "analytics/datastructure/page.hpp"
#include "analytics/util/util.hpp"
#include "analytics/io/Turbo_bin_io_handler.hpp"

#define END_OF_file	-1
#define FILE_CHUNK_SIZE (128 * 1024 * 1024)

class Splittable_Turbo_bin_io_handler {
  public:

	Splittable_Turbo_bin_io_handler() {
		total_file_num = 0;
		last_file_size = 0;
		current_file_idx = 0;
	}

	Splittable_Turbo_bin_io_handler(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false) {
		total_file_num = 0;
		last_file_size = 0;
		current_file_idx = 0;
		OpenFile(file_name, create_if_not_exist, write_enabled, delete_if_exist);
	}

	~Splittable_Turbo_bin_io_handler() {
		for(auto i = 0; i < total_file_num; i++)
			delete io_handler[i];
	}


	void Close(bool rm = false) {
		for(auto i = current_file_idx; i < total_file_num; i++)
			io_handler[i]->Close(rm);
		total_file_num = 0;
	}

	ReturnStatus OpenFile(const char* file_name, bool create_if_not_exist = false, bool write_enabled = false, bool delete_if_exist = false) {
		file_prefix = std::string(file_name);
		std::string splitted_file_name = file_prefix + "." + std::to_string(0);

		glob_t gl;
		int file_num = 0;
		if(glob((std::string(file_name) + ".*").c_str(), GLOB_NOSORT, NULL, &gl) == 0)
			file_num = gl.gl_pathc;
		globfree(&gl);

		if(file_num == 0) {
			io_handler.push_back(new Turbo_bin_io_handler);
			io_handler[0]->OpenFile(splitted_file_name.c_str(), true, write_enabled, delete_if_exist);
			total_file_num++;
		} else {
			for(auto handler_idx = 0; handler_idx < file_num; handler_idx++) {
				splitted_file_name = file_prefix + "." + std::to_string(handler_idx);
				io_handler.push_back(new Turbo_bin_io_handler);
				io_handler[handler_idx]->OpenFile(splitted_file_name.c_str(), false, write_enabled);
				last_file_size = io_handler[handler_idx]->file_size();
				total_file_num++;
			}
		}

		return OK;
	}


	ReturnStatus Append(std::size_t size_to_append, char* data) {
		int64_t tmp_file_size = io_handler[total_file_num - 1]->file_size();
		std::size_t tmp_size_to_append = size_to_append;

		if(tmp_file_size + size_to_append <= file_chunk_size) {
			io_handler[total_file_num - 1]->Append(size_to_append, data);
		} else {
			std::size_t write_size = file_chunk_size - tmp_file_size;
			io_handler[total_file_num - 1]->Append(write_size, data);
			data = &data[write_size];

			tmp_size_to_append -= write_size;

			while(tmp_size_to_append > 0) {
				io_handler.push_back(new Turbo_bin_io_handler((file_prefix + "." + std::to_string(total_file_num)).c_str(), true, true, true));

				if(tmp_size_to_append > file_chunk_size) {
					io_handler[total_file_num]->Append(file_chunk_size, data);
					data = &data[file_chunk_size];
					last_file_size = io_handler[total_file_num++]->file_size();
					tmp_size_to_append -= file_chunk_size;
				} else {
					io_handler[total_file_num]->Append(tmp_size_to_append, data);
					last_file_size = io_handler[total_file_num++]->file_size();
					tmp_size_to_append = 0;
				}
			}
		}

		return OK;
	}

	ReturnStatus Read(std::size_t offset_to_read, std::size_t size_to_read, char* data, bool rm = false) {
		int chunk_num = offset_to_read / file_chunk_size;
		ALWAYS_ASSERT(chunk_num <= total_file_num - 1 && chunk_num >= 0);

		std::size_t tmp_offset = offset_to_read - (chunk_num * file_chunk_size);
		std::size_t tmp_size_to_read = size_to_read;
		std::size_t size_to_read_chunk;

		while(tmp_size_to_read > 0) {
			ALWAYS_ASSERT(chunk_num <= total_file_num - 1 && chunk_num >= 0);
			if(tmp_offset + tmp_size_to_read > file_chunk_size)
				size_to_read_chunk = file_chunk_size - tmp_offset;
			else
				size_to_read_chunk = tmp_size_to_read;

			ALWAYS_ASSERT(tmp_offset >= 0);
			ALWAYS_ASSERT(size_to_read_chunk >= 0);
			io_handler[chunk_num]->Read(tmp_offset, size_to_read_chunk, data);
			data = &data[size_to_read_chunk];

			if(rm && (tmp_offset + size_to_read_chunk == file_chunk_size)) {
				io_handler[chunk_num]->Close(true);
				current_file_idx++;
			}

			chunk_num++;
			tmp_offset = (tmp_offset + size_to_read_chunk) % file_chunk_size;
			tmp_size_to_read -= size_to_read_chunk;
		}

		return OK;
	}

	std::size_t Read_upto(std::size_t offset_to_read, std::size_t size_to_read, char* data, bool rm = false) {
		int chunk_num = offset_to_read / file_chunk_size;
		ALWAYS_ASSERT(chunk_num <= total_file_num - 1 && chunk_num >= 0);

		std::size_t tmp_offset = offset_to_read - (chunk_num * file_chunk_size);
		std::size_t tmp_size_to_read = size_to_read;
		std::size_t size_to_read_chunk;
		std::size_t total_read_size = 0;

		while(tmp_size_to_read > 0) {
			ALWAYS_ASSERT(chunk_num <= total_file_num - 1 && chunk_num >= 0);
			if(tmp_offset + tmp_size_to_read > file_chunk_size)
				size_to_read_chunk = file_chunk_size - tmp_offset;
			else
				size_to_read_chunk = tmp_size_to_read;

			if(io_handler[chunk_num]->file_size() < tmp_offset + size_to_read_chunk) {
				size_to_read_chunk = io_handler[chunk_num]->file_size() - tmp_offset;
				tmp_size_to_read = size_to_read_chunk;
			}

			ALWAYS_ASSERT(size_to_read_chunk >= 0);

			io_handler[chunk_num]->Read(tmp_offset, size_to_read_chunk, data);
			data = &data[size_to_read_chunk];

			if(rm && (tmp_offset + size_to_read_chunk == file_chunk_size)) {
				io_handler[chunk_num]->Close(true);
				current_file_idx++;
			}

			chunk_num++;
			total_read_size += size_to_read_chunk;
			tmp_offset = (tmp_offset + size_to_read_chunk) % file_chunk_size;

			if(chunk_num == total_file_num)
				tmp_size_to_read = 0;
			else
				tmp_size_to_read -= size_to_read_chunk;
		}

		return total_read_size;
	}

	ReturnStatus Write(size_t offset_to_write, size_t size_to_write, char* data) {
		int chunk_num = offset_to_write / file_chunk_size;
		ALWAYS_ASSERT(chunk_num <= total_file_num - 1);

		std::size_t tmp_offset = offset_to_write - (chunk_num * file_chunk_size);
		std::size_t tmp_size_to_write = size_to_write;
		std::size_t size_to_write_chunk;

		while(tmp_size_to_write > 0) {
			ALWAYS_ASSERT(chunk_num <= total_file_num - 1 && chunk_num >= 0);
			if(tmp_size_to_write + tmp_offset > file_chunk_size)
				size_to_write_chunk = file_chunk_size - tmp_offset;
			else
				size_to_write_chunk = tmp_size_to_write;

			if(io_handler[chunk_num]->file_size() < tmp_offset + size_to_write_chunk) {
				size_to_write_chunk = io_handler[chunk_num]->file_size() - tmp_offset;
				tmp_size_to_write = size_to_write_chunk;
			}

			io_handler[chunk_num]->Write(tmp_offset, size_to_write_chunk, data);
			data = &data[size_to_write_chunk];

			chunk_num++;
			tmp_offset = (tmp_offset + size_to_write_chunk) % file_chunk_size;
			tmp_size_to_write -= size_to_write_chunk;
		}

		return OK;
	}
	//	static ReturnStatus callWrite(Turbo_bin_io_handler* object,size_t offset_to_write, size_t size_to_write, char* data) {
	//	object->Write(offset_to_write, size_to_write, data);
	//	}

	int64_t file_size() {
		return (file_chunk_size * (total_file_num - 1) + last_file_size);
	}
	/*		void AssertFileDescriptor(int fd){
				ALWAYS_ASSERT(fd == file_descriptor);
			}
			int fdval(){
				return file_descriptor;
			}*/
  private:
	std::vector<Turbo_bin_io_handler*> io_handler;
	const std::size_t file_chunk_size = FILE_CHUNK_SIZE;
	int64_t last_file_size;
	//char* file_mmap;
	std::string file_prefix;
	int total_file_num;
	int current_file_idx;
};

