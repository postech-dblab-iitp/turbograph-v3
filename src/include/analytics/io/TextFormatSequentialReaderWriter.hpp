#pragma once

/*
 * Design of the TextFormatSequentialReaderWriter
 *
 * There are two classes implemented in this file for reading and writing text 
 * files. The Reader class provides a function to open a given input file and a
 * function to read each line of the file. The Writer class provides a function
 * to open a given input file and a function to write a given string to the file.
 */

#include <iostream>
#include <fstream>
#include <string>

#include "analytics/io/Turbo_bin_mmapper.hpp"

#define END_OF_FILE	-1

class TextFormatSequentialReader {
  public:
	std::ifstream file_;
	std::string file_name_;
  public:

	TextFormatSequentialReader() {}

	TextFormatSequentialReader(const char* file_name) {
		Open(file_name);
	}

	virtual ~TextFormatSequentialReader() {
		Close(false);
	}

	TextFormatSequentialReader(const TextFormatSequentialReader& o) {

	}

	void Close(bool rm) {
		file_.close();
		if (rm) {
			int is_error = remove(file_name_.c_str());
			if (is_error != 0) {
				fprintf(stderr, "The file (%s) cannot be deleted; ErrorCode=%d\n", file_name_.c_str(), errno);
			}
		}
	}

	ReturnStatus rewind() {
		file_.clear();
		file_.seekg(0);
		return ReturnStatus::OK;
	}

	ReturnStatus Open(const char* file_name) {
		file_.open(file_name);
		if (!file_.good()) {
			fprintf(stdout, "[TextReader] Failed to open %s\n", file_name);
			return ReturnStatus::FAIL;
		}
		file_name_ = std::string(file_name);
		return ReturnStatus::OK;
	}

	ReturnStatus getNext(std::string& line) {
		if (!std::getline(file_, line, '\n')) {
			return ReturnStatus::FAIL;
		}
		return ReturnStatus::OK;
	}

};

class TextFormatSequentialWriter {
  public:
	std::ofstream file_;
  public:

	TextFormatSequentialWriter() {}

	TextFormatSequentialWriter(const char* file_name) {
		ReturnStatus st = Open(file_name);
		if (st != ReturnStatus::OK) {
			std::cout  << "[TextFormatSequentialWriter] failed to open the file "<<file_name << std::endl;
		}
	}

	~TextFormatSequentialWriter() {
		file_.close();
	}

	ReturnStatus Open(const char* file_name) {
		file_.open(file_name);
		if (!file_.good()) {
			return ReturnStatus::FAIL;
		}
		return ReturnStatus::OK;
	}

	ReturnStatus pushNext(std::string& str) {
		file_ << str;
		return ReturnStatus::OK;
	}
};


/* Maybe this class needs to be moved to BinaryFormatSequentialReaderWriter file.. */
template <typename entry_t>
class BinaryFormatSequentialReader {

  public:
	BinaryFormatSequentialReader() {}

	BinaryFormatSequentialReader(const char* file_name) {
		Open(file_name);
	}

	virtual ~BinaryFormatSequentialReader() {
	}

	void Open(const char* file_name) {
		reader_.OpenFileAndMemoryMap(file_name, false);
		file_size_ = reader_.file_size();
		if (file_size_)
			ptr_ = (entry_t*) reader_.data();
		current_idx_ = -1;
		ALWAYS_ASSERT(ptr_ != NULL);
	}

	void Close(bool rm = false) {
		reader_.Close(rm);
	}

	ReturnStatus rewind() {
		current_idx_ = -1;
		return ReturnStatus::OK;
	}

	ReturnStatus getNext(entry_t& item) {
		if (sizeof(entry_t) * (current_idx_ + 1) >= file_size_) {
			return ReturnStatus::FAIL;
		}
		item = ptr_[++current_idx_];
		return ReturnStatus::OK;
	}
	int64_t file_size() {
		return reader_.file_size();
	}

  private:
	entry_t* ptr_;
	Turbo_bin_mmapper reader_;
	int64_t current_idx_;
	std::size_t file_size_;
};

/* Maybe this class needs to be moved to BinaryFormatSequentialReaderWriter file.. */
template <typename entry_t>
class ParallelBinaryFormatSequentialReader {

  public:
	ParallelBinaryFormatSequentialReader() {}

	ParallelBinaryFormatSequentialReader(const char* file_name, int thread_num) {
		Open(file_name, thread_num);
	}

	virtual ~ParallelBinaryFormatSequentialReader() {
	}

	void Open(const char* file_name, int total_thread) {
		total_thread_ = total_thread;
		dummy.src_vid_ = -1;
		dummy.dst_vid_ = -1;
//		current_idx_list_ = new int64_t[total_thread_];
//		first_idx_list_ = new int64_t[total_thread_ + 1];

		reader_.OpenFileAndMemoryMap(file_name, false);
		file_size_ = reader_.file_size();
		if (file_size_) {
			ptr_ = (entry_t*) reader_.data();
			current_idx_ = 0;
			/*for(auto i = 0; i < total_thread_; i++) {
				current_idx_list_[i] = (file_size_ * i) / (total_thread_ * sizeof(entry_t));
				current_idx_list_[i] = ((current_idx_list_[i] + sizeof(entry_t) - 1) / sizeof(entry_t)) * sizeof(entry_t);
				first_idx_list_[i] = current_idx_list_[i];
			}
			ALWAYS_ASSERT(file_size_ % sizeof(entry_t) == 0);
			first_idx_list_[total_thread_] = file_size_ / sizeof(entry_t);*/
		}
		ALWAYS_ASSERT(ptr_ != NULL);
	}

	void Close(bool rm = false) {
		reader_.Close(rm);
	}

	ReturnStatus rewind() {
		current_idx_ = 0;
		/*for(auto i = 0; i < total_thread_; i++) {
			current_idx_list_[i] = first_idx_list_[i];
			ALWAYS_ASSERT(current_idx_list_[i] <= file_size_);
		}*/
		return ReturnStatus::OK;
	}

	ReturnStatus getNext(entry_t* item) {
		//	ALWAYS_ASSERT(thread_num <= total_thread_ - 1);
//		int64_t partition_end = first_idx_list_[thread_num + 1];

//		if(current_idx_list_[thread_num] == partition_end) return ReturnStatus::FAIL;
		if (sizeof(entry_t) * (current_idx_) >= file_size_) return ReturnStatus::FAIL;
		for(auto i = 0; i < total_thread_; i++) {
			if (sizeof(entry_t) * (current_idx_ + i) < file_size_)
				item[i] = ptr_[current_idx_ + i];
			else
				item[i] = dummy;
		}
		current_idx_ += total_thread_;
//		item = ptr_[current_idx_list_[thread_num]++];
		return ReturnStatus::OK;
	}

	int64_t file_size() {
		return reader_.file_size();
	}

  private:
	entry_t* ptr_;
	entry_t dummy;
	Turbo_bin_mmapper reader_;
	int64_t* current_idx_list_;
	int64_t current_idx_;
	int64_t* first_idx_list_;
	std::size_t file_size_;
	int total_thread_;
};

/*
// SrcVid Degree DstVid1 DstVid2 ...
class TextAdjListReader {
    public:
        TextAdjListReader() {

        }


        ReturnStatus getNext(EdgePairEntry<entry_t>& item) {
            return ReturnStatus::OK;
        }
    private:
        std::vector<TextFormatSequentialReader> readers_;
};
*/

