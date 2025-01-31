#pragma once

/*
 * Design of the DummyDataReader
 *
 * In this class, functions that generate virtual (dummy) input data are implemented.
 * Currently, a total of 6 modes are implemented. The main purpose of this class 
 * is to test and debug BBP, which allows you to put data with various topologies 
 * as inputs.
 * 
 */

#include <ctime>
#include <random>
#include "TypeDef.hpp"
#include "GraphDataReaderWriterInterface.hpp"
#include "pcg_random.hpp"

template <typename Entry_t, bool endian_trfm >
class BinaryFormatEdgePairDummyReader : public ReaderInterface<Entry_t> {
  public:

	BinaryFormatEdgePairDummyReader() {}
	BinaryFormatEdgePairDummyReader(size_t size, int64_t max_vid, int64_t data_mode, int64_t degree) : ReaderInterface<Entry_t>(),
		size_(size), left_(size), max_vid_(max_vid), data_mode_(data_mode), degree_(degree) {
		pcg_extras::seed_seq_from<std::random_device> seed_source;
		rand_gen_ = new pcg64_fast(seed_source);
		rand_chk_ = new pcg64_fast();
		*rand_chk_ = *rand_gen_;
		current_ = 1;
	}
	~BinaryFormatEdgePairDummyReader() {
		delete rand_gen_;
		delete rand_chk_;
	}

	virtual ReturnStatus rewind() {
		*rand_gen_ = *rand_chk_;
		left_ = size_;
		return OK;
	}

	virtual ReturnStatus getNext(Entry_t& item) {
		if(data_mode_ == 0) return random_Next(item);
		else if(data_mode_ == 1) return ring_Next(item);
		else if(data_mode_ == 2) return decrease_Next(item);
		else if(data_mode_ == 3) return duplicate_ring_Next(item);
		else if(data_mode_ == 4) return self_Next(item);
		else if(data_mode_ == 5) return duplicate_self_Next(item);
		else return FAIL;
	}

	ReturnStatus random_Next(Entry_t& item) {
		if (left_ <= 0)
			return FAIL;
		left_--;
		item.src_vid_ = (*rand_gen_)() % max_vid_;
		item.dst_vid_ = (*rand_gen_)() % max_vid_;
		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	ReturnStatus ring_Next(Entry_t& item) {
		if(current_ > degree_) {
			left_--;
			current_ = 1;
			if(left_ <= 0) return FAIL;
		}
		item.src_vid_ = size_ - left_;
		item.dst_vid_ = (size_ - left_ + current_) % max_vid_;
		current_++;
		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	ReturnStatus decrease_Next(Entry_t& item) {
		if(current_ > size_ - left_) {
			left_--;
			current_ = 1;
			if(left_ <= 0) return FAIL;
		}
		item.src_vid_ = size_ - left_;
		item.dst_vid_ = size_ - left_ - current_;
		current_++;

		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	ReturnStatus duplicate_ring_Next(Entry_t& item) {
		if(current_ > degree_) {
			left_--;
			current_ = 1;
			if(left_ <= 0) return FAIL;
		}
		item.src_vid_ = size_ - left_;
		item.dst_vid_ = (size_ - left_ + 1) % max_vid_;
		current_++;
		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	ReturnStatus self_Next(Entry_t& item) {
		if(current_ > degree_) {
			left_--;
			current_ = 1;
			if(left_ <= 0) return FAIL;
		}
		item.src_vid_ = size_ - left_;
		item.dst_vid_ = size_ - left_;
		current_++;
		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	ReturnStatus duplicate_self_Next(Entry_t& item) {
		if(current_ > degree_) {
			left_--;
			current_ = 1;
			if(left_ <= 0) return FAIL;
		}
		if(current_ == 1) {
			item.src_vid_ = size_ - left_;
			item.dst_vid_ = size_ - left_;
			current_++;
		} else {
			item.src_vid_ = size_ - left_;
			item.dst_vid_ = (size_ - left_ + 1) % max_vid_;
			current_++;
		}
		if(endian_trfm) {
			item.src_vid_ = __bswap_64(item.src_vid_);
			item.dst_vid_ = __bswap_64(item.dst_vid_);
		}
		last_entry_ = item;
		return OK;
	}

	ReturnStatus getCurrent(Entry_t& item) {
		if (left_ < 0) {
			return FAIL;
		}
		item = last_entry_;
		return OK;
	}
	int64_t file_size() {
		return size_;
	}

  private:
	Entry_t last_entry_;
	size_t size_;
	size_t left_;
	int64_t max_vid_;
	pcg64_fast* rand_gen_;
	pcg64_fast* rand_chk_;
	int64_t data_mode_;
	int64_t current_;
	int64_t degree_;
};
