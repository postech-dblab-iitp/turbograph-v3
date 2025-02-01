#ifndef EdgePairListReader_H
#define EdgePairListReader_H

/*
 * Design of the EdgePairListReader
 *
 * Several classes are implemented in this header file, depending on the format 
 * of the various input data. The supported data formats are combinations of
 * the following 1) Binary / Text and 2) Edge Pair / Adj List. For each getNext()
 * call, the edges existing in the input data are returned one by one in order.
 */

#include <vector>
#include <string>

#include "analytics/core/TypeDef.hpp"
#include "analytics/io/GraphDataReaderWriterInterface.hpp"
#include "analytics/io/TextFormatSequentialReaderWriter.hpp"
#include "analytics/core/turbo_dist_internal.hpp"


// {EdgePair1}, ... {EdgePairK}, {K, K,}, {null}
template <typename Entry_t, bool endian_trfm >
class BinaryFormatEdgePairListReader : public ReaderInterface<Entry_t> {
  public:

	/* Construct this reader */
	BinaryFormatEdgePairListReader() {}
	BinaryFormatEdgePairListReader(const char* file_name) : ReaderInterface<Entry_t>() {
		Open(file_name);
	}

	/* Open this reader for the given file */
	void Open(const char* file_name) {
		reader_.Open(file_name);
		last_entry_.src_vid_ = -1;
		last_entry_.dst_vid_ = -1;
	}

	/* Close this reader */
	void Close(bool rm = false) {
		reader_.Close(rm);
	}

	/* Rewind the reader */
	virtual ReturnStatus rewind() {
		return reader_.rewind();
	}

	/* Get the next edge pair */
	virtual ReturnStatus getNext(Entry_t& item) {
		if (reader_.getNext(item) == FAIL) {
			last_entry_.src_vid_ = -1;
			last_entry_.dst_vid_ = -1;
			item = last_entry_;
			return FAIL;
		}
		/* If the the edges was stored in the big endian style */
		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	/* Get the current edge pair */
	ReturnStatus getCurrent(Entry_t& item) {
		if (last_entry_.src_vid_ == -1 && last_entry_.dst_vid_ == -1) {
			item = last_entry_;
			return FAIL;
		}
		item = last_entry_;
		return OK;
	}

	/* Get the size of the file */
	int64_t file_size() {
		return reader_.file_size();
	}

  private:
	BinaryFormatSequentialReader<Entry_t> reader_; /* reader */
	Entry_t last_entry_; /* the last entry that the reader read */
};

class TextEdgePairListReader: public ReaderInterface<EdgePairEntry<node_t>> {
  public:

	/* Construct this reader */
	TextEdgePairListReader() {}
	TextEdgePairListReader(const char* file_name) : ReaderInterface<EdgePairEntry<node_t>>() {
		Open(file_name);
	}

	/* Construct this reader */
	TextEdgePairListReader(const TextEdgePairListReader& o) {

	}

	/* Open this reader for the given file */
	void Open(const char* file_name) {
		reader_.Open(file_name);
		line_buffer_.reserve(1024);
		token_buffer_.reserve(1024);
		last_entry_.src_vid_ = -1;
		last_entry_.dst_vid_ = -1;
	}

	/* Close this reader for the given file */
	void Close(bool rm = false) {

	}

	/* Rewind the reader */
	virtual ReturnStatus rewind() {
		return reader_.rewind();
	}

	/* Get the next edge pair */
	virtual ReturnStatus getNext(EdgePairEntry<node_t>& item) {
		if (reader_.getNext(line_buffer_) == FAIL
		        || line_buffer_.empty()) {
			last_entry_.src_vid_ = -1;
			last_entry_.dst_vid_ = -1;
			item = last_entry_;
			return FAIL;
		}
		std::istringstream iss(line_buffer_);
		iss >> item.src_vid_ >> item.dst_vid_;
		last_entry_ = item;
		return OK;
	}

	/* Get the current edge pair */
	ReturnStatus getCurrent(EdgePairEntry<node_t>& item) {
		if (last_entry_.src_vid_ == -1 && last_entry_.dst_vid_ == -1) {
			item = last_entry_;
			return FAIL;
		}
		item = last_entry_;
		return OK;
	}

	/* Get the file size */
	int64_t file_size() {
		ALWAYS_ASSERT(0); // can't called
		return -1;
	}

  private:
	TextFormatSequentialReader reader_; /* reader */
	std::string line_buffer_; /* the buffer for a line of a text */
	std::string token_buffer_; /* the buffer for the tokens */
	EdgePairEntry<node_t> last_entry_; /* the last entry that this reader read */
};

template <typename Entry_t>
class TextFormatAdjListReader: public ReaderInterface<EdgePairEntry<Entry_t> > {
  public:
	std::ifstream file_;
  public:

	/* Construct the reader */
	TextFormatAdjListReader() {
		degree_left = 0;
		src_vid = dst_vid = -1;
	}

	/* Construct the reader for the given file */
	TextFormatAdjListReader(const char* file_name) : ReaderInterface<EdgePairEntry<node_t>>() {
		Open(file_name);
	}

	/* Deconstruct the reader */
	virtual ~TextFormatAdjListReader() {
		file_.close();
	}

	/* Construct the reader from another reader */
	TextFormatAdjListReader(const TextFormatAdjListReader& o) {
	}

	/* Rewind the reader */
	ReturnStatus rewind() {
		degree_left = 0;
		src_vid = dst_vid = -1;
		file_.clear();
		file_.seekg(0);
		return OK;
	}

	/* Initialize the reader for the given file */
	ReturnStatus Open(const char* file_name) {
		fprintf(stdout, "[TextFormatAdjListReader] Open File %s\n", file_name);
		file_.open(file_name);
		if (!file_.good()) {
			return FAIL;
		}
		return OK;
	}

	/* Close the reader */
	ReturnStatus Close(bool rm = false) {
		return OK;
	}

	/* Get the next edge */
	virtual ReturnStatus getNext(EdgePairEntry<node_t>& edge) {
		while(degree_left == 0) {
			if(file_.eof())
				return FAIL;

			file_>> src_vid >> degree_left;
		}
		edge.src_vid_ = src_vid;
		file_ >> edge.dst_vid_;
		dst_vid = edge.dst_vid_;
		degree_left--;
	}

	/* Get the current edge */
	ReturnStatus getCurrent(EdgePairEntry<node_t>& item) {
		if (src_vid == -1 && dst_vid == -1) {
			item.src_vid_ = item.dst_vid_ = -1;
			return FAIL;
		}
		item.src_vid_ = src_vid;
		item.dst_vid_ = dst_vid;
		return OK;
	}
  private:
	node_t src_vid; /* the source id of the last edge that this reader read */
	node_t dst_vid; /* the destination id of the last edge that this reader read */
	node_t degree_left; /* the number of not yet accessed destination vids of the lastly accessed source id */
};



template <typename EdgePairListReader_t, typename Entry_t>
class EdgePairListFilesReader : public ReaderInterface<Entry_t> {
  public:

	/* 
	* Construct the reader that reads edge pairs from multiple files
	*/
	EdgePairListFilesReader(std::vector<std::string>& list_of_files): ReaderInterface<Entry_t>() {
		readers_.resize(list_of_files.size());
		cached_entries_.resize(list_of_files.size());

		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].Open(list_of_files[idx].c_str());
		}

		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].getNext(cached_entries_[idx]);
		}
	}

	/*
	* Get the next edge
	* It assumes that the files store sorted edges and return the smallest edge in the not accessed edges 
	*/
	virtual ReturnStatus getNext(Entry_t& item) {
		std::size_t smallest_idx = 0;
		Entry_t smallest;
		smallest.src_vid_ = -1;
		smallest.dst_vid_ = -1;
		ReturnStatus st;
		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			if (cached_entries_[idx].src_vid_ == -1 && cached_entries_[idx].dst_vid_ == -1)
				continue;

			if (smallest.src_vid_ == -1 && smallest.dst_vid_ == -1) {
				smallest = cached_entries_[idx];
				smallest_idx = idx;
			} else {
				if (cached_entries_[idx] < smallest) {
					smallest = cached_entries_[idx];
					smallest_idx = idx;
				}
			}
		}

		if (smallest.src_vid_ == -1 && smallest.dst_vid_ == -1) {
			item = smallest;
			return FAIL;
		}
		ALWAYS_ASSERT(smallest == cached_entries_[smallest_idx]);
		Entry_t dummy;
//   std::cout << smallest.src_vid_ << ", " << smallest.dst_vid_ << " | "  << smallest_idx << std::endl;
		readers_[smallest_idx].getCurrent(dummy);
		ALWAYS_ASSERT(smallest == dummy);
		item = smallest;
		st = readers_[smallest_idx].getNext(cached_entries_[smallest_idx]);
		ALWAYS_ASSERT(smallest.src_vid_ >= 0);
		ALWAYS_ASSERT(smallest.dst_vid_ >= 0);
		return OK;
	}

	/* Rewind the reader */
	virtual ReturnStatus rewind() {
		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].rewind();
		}

		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].getNext(cached_entries_[idx]);
		}
		return OK;
	}

	/* Get the total size of the files */
	int64_t total_size() {
		int64_t return_val = 0;
		for(int64_t i = 0 ; i < readers_.size(); i++)
			return_val += readers_[i].file_size();
		return return_val;

	}

  private:
	std::vector<EdgePairListReader_t> readers_; /* readers */
	std::vector<Entry_t> cached_entries_; /* cached edges */
};

template <typename EdgePairListReader_t, typename Entry_t>
class EdgePairListSortedFilesReader : public ReaderInterface<Entry_t> {
  public:

	/* Construct the reader for the given files */
	EdgePairListSortedFilesReader(std::vector<std::string>& list_of_files, bool rm = false): ReaderInterface<Entry_t>() {
		remove_mode = rm;
		readers_.resize(list_of_files.size());

		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].Open(list_of_files[idx].c_str());
		}
		if(list_of_files.size() == 0) current_reader = -1;
		else current_reader = 0;
	}

	/*
	* Get the next edge
	* It assumes that each file has sorted edges
	* It also assumes that edges in the file a are smaller than the edges in the file b 
	* if the index of a is smaller than that of b
	*/
	virtual ReturnStatus getNext(Entry_t& item) {
		if(current_reader == -1) return FAIL;
		while(readers_[current_reader].getNext(item) == FAIL) {
			if(remove_mode)
				readers_[current_reader++].Close(true);
			if(current_reader == readers_.size())
				return FAIL;
		}
		return OK;
	}

	/* Rewind this reader */
	virtual ReturnStatus rewind() {
		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].rewind();
		}
		current_reader = 0;
		return OK;
	}

	/* Get the total size of files */
	int64_t total_size() {
		int64_t return_val = 0;
		for(int64_t i = 0 ; i < readers_.size(); i++)
			return_val += readers_[i].file_size();
		return return_val;

	}

  private:
	std::vector<EdgePairListReader_t> readers_; /* readers */
	int64_t current_reader; /* current reader */
	bool remove_mode;
};

template <typename EdgePairListReader_t, typename Entry_t>
class ParallelEdgePairListSortedFilesReader : public ReaderInterface<Entry_t> {
  public:
	/* Construct this reader for the given list of files */
	ParallelEdgePairListSortedFilesReader(std::vector<std::string>& list_of_files, int total_thread, bool rm = false): ReaderInterface<Entry_t>() {
		remove_mode = rm;

		readers_.resize(list_of_files.size());

//#pragma omp parallel for num_threads(total_thread)
		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].Open(list_of_files[idx].c_str(), total_thread);
		}

//		current_reader = new int64_t[total_thread];
//#pragma omp parallel for num_threads(total_thread)
		//	for(auto i = 0; i < total_thread; i++)
		//	current_reader[i] = 0;
		current_reader = 0;
		total_thread_ = total_thread;
	}

	~ParallelEdgePairListSortedFilesReader() {
	}

	/* get the next edge */
	virtual ReturnStatus getNext(Entry_t& item) {
		return FAIL;
	}

	/* get the next edge */
	virtual ReturnStatus getNext(Entry_t* item) {
		while(readers_[current_reader].getNext(item) == FAIL) {
			//std::atomic_fetch_add((std::atomic<int>*)&done_partition[current_reader[thread_num]], (int)1);
			//while(done_partition[current_reader[thread_num]] < total_thread_) {
			//	usleep(1000);
			//}
			if(remove_mode)
				readers_[current_reader].Close(false);

			current_reader++;
			if(current_reader == readers_.size())
				return FAIL;
		}
		return OK;
	}

	/* rewind this reader */
	virtual ReturnStatus rewind() {
		for (std::size_t idx = 0; idx < readers_.size(); idx++) {
			readers_[idx].rewind();
		}

		current_reader = 0;
		return OK;
	}

	/* Get the total size of the files */
	int64_t total_size() {
		int64_t return_val = 0;
		for(int64_t i = 0 ; i < readers_.size(); i++)
			return_val += readers_[i].file_size();
		return return_val;

	}

  private:
	std::vector<EdgePairListReader_t> readers_; /* readers */
//  int64_t* current_reader;
	int64_t current_reader; /* the index of the current reader */
	int total_thread_; /* the number of threads that this reader use */
	bool remove_mode;
};




template <typename entry_t, bool endian_trfm >
class BinaryAdjListReader : public ReaderInterface<EdgePairEntry<node_t>> {
  public:

	/* Construct the redader */
	BinaryAdjListReader() {}

	/* Construct the reader for the given file */
	BinaryAdjListReader(const char* file_name) {
		Open(file_name);
	}

	/* Deconstruct the reader */
	virtual ~BinaryAdjListReader() {
	}

	/* Open the reader for the given file */
	void Open(const char* file_name) {
		reader_.OpenFileAndMemoryMap(file_name, false);
		ptr_ = (entry_t*) reader_.data();
		current_idx_ = -1;
		src_ = -1;
		src_degree_ = -1;
		file_size_ = reader_.file_size();
		ALWAYS_ASSERT(ptr_ != NULL);
	}

	/* Close the reader for the given file */
	void Close(bool rm = false) {
		reader_.Close(rm);
	}

	/* Rewind the reader */
	virtual ReturnStatus rewind() {
		src_ = -1;
		current_idx_ = -1;
		return OK;
	}

	/* Get next edge */
	virtual ReturnStatus getNext(EdgePairEntry<node_t>& item) {
		if(src_ == -1) {
			if (sizeof(entry_t) * (current_idx_ + 2) >= file_size_) {
				return FAIL;
			}
			src_ = (node_t)(int64_t)ptr_[current_idx_ + 1];
			if(endian_trfm)
				src_ = __bswap_64((int64_t) src_);
			src_degree_ = (node_t)(int64_t)ptr_[current_idx_+2];
			if(endian_trfm)
				src_degree_ = __bswap_64((int64_t)src_degree_);
			ALWAYS_ASSERT(src_degree_ != 0);
			current_idx_ += 3;
		} else
			current_idx_ ++;
		if (sizeof(entry_t) * (current_idx_) >= file_size_) {
			return FAIL;
		}
		item.dst_vid_ = (node_t)(int64_t)ptr_[current_idx_];
		if(endian_trfm)
			item.dst_vid_ = __bswap_64((int64_t)item.dst_vid_);
		item.src_vid_ = src_;
		src_degree_--;
		if(src_degree_ == 0)
			src_ = -1;
		return OK;
	}
	int64_t file_size() {
		return reader_.file_size();
	}

  private:
	entry_t* ptr_; /* pointer to the lastly accessed edge */
	Turbo_bin_mmapper reader_;
	node_t current_idx_;
	node_t src_; /* the source id of the lastly accessed edge */
	node_t src_degree_; /* the degree of source vertex of the lastly accessed edge */
	std::size_t file_size_; /* the size of file */

};

template <typename Entry_t>
class TextEdgePairListFastReader: public ReaderInterface<EdgePairEntry<Entry_t>> {
  public:

	/* Construct this reader */
	TextEdgePairListFastReader() : ReaderInterface<EdgePairEntry<Entry_t>>() {}

	/* Deconstruct this reader */
	~TextEdgePairListFastReader() {

	}

	/* Rewind this reader */
	ReturnStatus rewind() {
	}

	/* Get the size of file */
	int64_t file_size() {
	}

	/* Open this reader for the given file */
	ReturnStatus Open(const char* file_name, bool write_enabled = false) {
		reader_.OpenFileAndMemoryMap(file_name, false);
		ptr_ = reader_.data();
		current_idx_ = 0;
		file_size_ = reader_.file_size();
		ALWAYS_ASSERT(ptr_ != NULL);
	}

	/* Close this reader */
	void Close(bool rm = false) {
		reader_.Close(rm);
	}

	/* Get next edge */
	ReturnStatus getNext(EdgePairEntry<node_t>& item) {
		int lineLen = 0;
		char line[100];
		char* pEnd;

		if(current_idx_ == file_size_) return FAIL;

		while(current_idx_ < file_size_) {
			char temp = ptr_[current_idx_];
			current_idx_++;
			if(temp == '\n') {
				line[lineLen] = ' ';
				item.src_vid_ = std::strtoll(line, &pEnd, 10);
				item.dst_vid_ = std::strtoll(pEnd, NULL, 10);
				return OK;
			} else {
				line[lineLen] = temp;
				lineLen++;
				if(lineLen >= 100) return FAIL;
			}
		}

		return OK;
	}

  private:
	Turbo_bin_mmapper reader_;
	char* ptr_; /* pointer to the lastly accessed edge */
	int64_t current_idx_; /* index to the lastly accessed edge */
	size_t file_size_; /* the size of ile */
};

class ParallelTextEdgePairListFastReader: public ReaderInterface<EdgePairEntry<node_t>> {
  public:

	/* Construct this reader */
	ParallelTextEdgePairListFastReader() : ReaderInterface<EdgePairEntry<node_t>>() {}

	/* Deconstruct this reader */
	~ParallelTextEdgePairListFastReader() {
		delete[] current_idx_list_;
		delete[] first_idx_list_;
	}

	/* Rewind the reader */
	ReturnStatus rewind() {
		for(int i = 0; i < thread_num_; i++) {
			current_idx_list_[i] = first_idx_list_[i];
			if(current_idx_list_[i] > file_size_) return FAIL;
		}
		return OK;
	}

	/* Get the file size */
	int64_t file_size() {
		return file_size_;
	}

	/* 
	* Open the reader for the given file and thread num
	* It assumes that a thread read edges in an assigned partition 
	*/
	ReturnStatus Open(const char* file_name, int thread_num, bool write_enabled = false) {
		thread_num_ = thread_num;
		current_idx_list_ = new int64_t[thread_num_];
		first_idx_list_ = new int64_t[thread_num_ + 1];
		reader_.OpenFileAndMemoryMap(file_name, false);

		ptr_ = reader_.data();
		file_size_ = reader_.file_size();

		for(int i = 0; i < thread_num_; i++) {
			current_idx_list_[i] = (file_size_ * i) / thread_num_;

			while(ptr_[current_idx_list_[i]] != '\n') {
				current_idx_list_[i]++;
			}
			current_idx_list_[i]++;
			first_idx_list_[i] = current_idx_list_[i];
			ALWAYS_ASSERT(current_idx_list_[i] <= file_size_);
		}
		first_idx_list_[thread_num_] = file_size_;
		ALWAYS_ASSERT(ptr_ != NULL);
	}

	/* 
	* Open the reader for the given file and thread num
	* It assumes that a thread read edges in an assigned partition 
	*/
	ReturnStatus getNext(EdgePairEntry<node_t>& item, int64_t thread_num) {
		int lineLen = 0;
		char line[100];
		char* pEnd;
		ALWAYS_ASSERT(thread_num <= thread_num_ - 1);
		int64_t partition_end = first_idx_list_[thread_num + 1];

		if(current_idx_list_[thread_num] == partition_end) return FAIL;

		while(current_idx_list_[thread_num] < partition_end) {
			char temp = ptr_[current_idx_list_[thread_num]];
			current_idx_list_[thread_num]++;
			if(temp == '\n') {
				line[lineLen] = ' ';
				item.src_vid_ = std::strtoll(line, &pEnd, 10);
				item.dst_vid_ = std::strtoll(pEnd, NULL, 10);
				return OK;
			} else {
				line[lineLen] = temp;
				lineLen++;
				if(lineLen >= 100) {
					std::cout<<"Wrong format file"<<std::endl;
					return FAIL;
				}
			}
		}

		return OK;
	}

  private:
	Turbo_bin_mmapper reader_;
	char* ptr_; /* pointer to a memory space */
	int64_t* current_idx_list_; /* current idx per thread */
	int64_t* first_idx_list_; /* first idx per thread */
	size_t file_size_; /* size of file */
	int64_t thread_num_; /* the number of threads that access to this file */
};

template <typename Entry_t, bool endian_trfm >
class ParallelBinaryFormatEdgePairListReader : public ReaderInterface<Entry_t> {
  public:

	/* 
	* Construct the parallel reader
	*/
	ParallelBinaryFormatEdgePairListReader() {}
	ParallelBinaryFormatEdgePairListReader(const char* file_name, int total_thread) : ReaderInterface<Entry_t>() {
		total_thread_ = total_thread;
		Open(file_name, total_thread_);
	}

	/*
	* Open the reader for a given file and the number of threads that access to the file
	*/
	void Open(const char* file_name, int total_thread) {
		total_thread_ = total_thread;
		reader_.Open(file_name, total_thread);
		last_entry_.src_vid_ = -1;
		last_entry_.dst_vid_ = -1;
	}

	/*
	* Close the reader
	*/
	void Close(bool rm = false) {
		reader_.Close(rm);
	}

	/*
	* Rewind the reader
	*/
	virtual ReturnStatus rewind() {
		return reader_.rewind();
	}

	/*
	* Get the next edge
	*/
	virtual ReturnStatus getNext(Entry_t& item) {
		return FAIL;
	}

	/*
	* Get the next edge
	*/
	virtual ReturnStatus getNext(Entry_t* item) {
		if (reader_.getNext(item) == FAIL) {
			for(auto i = 0; i < total_thread_; i++) {
				item[i].src_vid_ = -1;
				item[i].dst_vid_ = -1;
			}
			return FAIL;
		}
		if(endian_trfm) {
			for(auto i = 0; i < total_thread_; i++) {
				item[i].src_vid_ = __bswap_64(item[i].src_vid_);
				item[i].dst_vid_ = __bswap_64(item[i].dst_vid_);
			}
		}
		//XXX
		//last_entry_ = item;
		return OK;
	}

	/*
	* Get the current edge
	*/
	ReturnStatus getCurrent(Entry_t& item) {
		if (last_entry_.src_vid_ == -1 && last_entry_.dst_vid_ == -1) {
			item = last_entry_;
			return FAIL;
		}
		item = last_entry_;
		return OK;
	}
	
	/*
	* Get the size of this file
	*/
	int64_t file_size() {
		return reader_.file_size();
	}

  private:
	ParallelBinaryFormatSequentialReader<Entry_t> reader_;
	Entry_t last_entry_; /* the lastly accessed entry */
	size_t file_size_; /* the size of the file */
	int64_t thread_num_; /* the thread id that access to the file using this reader */
	int total_thread_; /* the number of threads that access to this file */
};

#endif
