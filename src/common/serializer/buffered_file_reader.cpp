#include "common/serializer/buffered_file_reader.hpp"
#include "common/serializer/buffered_file_writer.hpp"
#include "common/exception.hpp"

#include <cstring>
#include <algorithm>

namespace duckdb {

BufferedFileReader::BufferedFileReader(FileSystem &fs, const char *path, FileOpener *opener)
    : fs(fs), data(unique_ptr<data_t[]>(new data_t[FILE_BUFFER_SIZE])), offset(0), read_data(0), total_read(0) {
	handle =
	    fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK, FileSystem::DEFAULT_COMPRESSION, opener);
	file_size = fs.GetFileSize(*handle);
}

void BufferedFileReader::ReadData(data_ptr_t target_buffer, uint64_t read_size) {
	// first copy anything we can from the buffer
	data_ptr_t end_ptr = target_buffer + read_size;
	while (true) {
		idx_t to_read = MinValue<idx_t>(end_ptr - target_buffer, read_data - offset);
		if (to_read > 0) {
			memcpy(target_buffer, data.get() + offset, to_read);
			offset += to_read;
			target_buffer += to_read;
		}
		if (target_buffer < end_ptr) {
			D_ASSERT(offset == read_data);
			total_read += read_data;
			// did not finish reading yet but exhausted buffer
			// read data into buffer
			offset = 0;
			read_data = fs.Read(*handle, data.get(), FILE_BUFFER_SIZE);
			if (read_data == 0) {
				throw SerializationException("not enough data in file to deserialize result");
			}
		} else {
			return;
		}
	}
}

bool BufferedFileReader::Finished() {
	return total_read + offset == file_size;
}

} // namespace duckdb
