//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/serializer.hpp"
#include "common/file_system.hpp"

namespace duckdb {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public Serializer {
public:
	static constexpr uint8_t DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;

	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedFileWriter(FileSystem &fs, const string &path, uint8_t open_flags = DEFAULT_OPEN_FLAGS,
	                   FileOpener *opener = nullptr);

	FileSystem &fs;
	string path;
	unique_ptr<data_t[]> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;

public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;
	//! Flush the buffer to disk and sync the file to ensure writing is completed
	void Sync();
	//! Flush the buffer to the file (without sync)
	void Flush();
	//! Returns the current size of the file
	int64_t GetFileSize();
	//! Truncate the size to a previous size (given that size <= GetFileSize())
	void Truncate(int64_t size);

	idx_t GetTotalWritten();
};

} // namespace duckdb
