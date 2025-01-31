#pragma once

/*
 * Design of the KWayMergeTree
 *
 * This class implements a tournament tree where the number of leaf nodes is k_.
 * To build the tree, it takes as input k_ sorted runs (named players). According 
 * to the given Compare function, k_ data extracted from runs are sorted. To 
 * retrieve the next highest priority value after using the top_data, the 
 * adjust_tree() function is called. The cost of this function is O(log_2 k_).
 *
 * The class has following member variables:
 * 
 * data_: The second half of the array consisting of data extracted from sorted run.
 *        Among the two adjacent values in the second half of the array, the higher 
 *        priority values are stored in the second half of the remaining array.
 *        (e.g. For the std::greater compare function, array[_ 8 68 3658])
 * tree_: An array of sorted runs.
 * map_: An array representing the position of each value stored in data_ on the 
 *       second half array of data_.
 */

#include <vector>

template <typename T, typename Compare>
class KWayMergeTree {
  public:

	/* Construct this k-way merge tree */
	KWayMergeTree(std::vector<T**>& players) : k_(players.size()),
		data_(new T[2 * k_]),
		map_(new size_t[2 * k_]),
		id_map_(new size_t[2 * k_]),
		tree_(new T**[2 * k_]),
		cmp_() {
		if (players.size() == 0) throw -1;
		construct_tree(players.data());
	}

	/* Deconstruct this k-way merge tree */
	~KWayMergeTree() {
		delete[] data_;
		delete[] tree_;
		delete[] id_map_;
		delete[] map_;
	}

	/* Remove the value with highest priority from the tree */
	void remove_top() {
		auto target = map_[1];
		k_--;
		tree_[target] = tree_[(k_ << 1) + 1];
		tree_[k_]= tree_[k_ << 1];
		id_map_[target] = id_map_[(k_ << 1) + 1];
		id_map_[k_] = id_map_[k_ << 1];
		data_[target] = data_[(k_ << 1) + 1];
		data_[k_] = data_[k_ << 1];
		map_[target] = target;
		map_[k_] = k_;

		size_t limit = k_;
		if (nxtpo2(limit - 1) != nxtpo2(target - 1)) limit <<= 1;
		limit ^= target;
		if (target < (k_ << 1))
			adjust_tree(target, limit);
		adjust_tree(k_);
	}

	/* Remove the value with highest priority and adjust the tournerment tree */
	void update_top() {
		size_t pos = map_[1];
		data_[pos] = **(tree_[pos]);
		adjust_tree(pos);
	}

	/* get the player that have the value with the highest priority in the tree */
	T** top() const {
		return tree_[map_[1]];
	}

	/* get the index of the player that have the value with the highest priority in the tree */
	size_t top_id() const {
		return id_map_[map_[1]];
	}

	/* get the vlaue with highest priority in the tree */
	T& top_data() const {
		return data_[1];
	}

	/* check whether there is no player in the tree */
	bool empty() const {
		return !k_;
	}

  private:

	/* Construct the tournerment tree */
	void construct_tree(T** players[]) {
		tree_[0] = nullptr;
		size_t i = 0;
		size_t j = 0;
		do {
			tree_[k_ + i] = players[i];
			data_[k_ + i] = **(players[i]);
			map_[k_ + i] = k_ + i;
			id_map_[k_ + i] = i;
		} while (++i < k_);
    // i == k_
		while (--i) {
			j = (i << 1);
			j |= cmp_(data_[j], data_[j + 1]);
			data_[i] = data_[j];
			map_[i] = map_[j];
		 }
	}
	/* Adjust the tree */
	void adjust_tree(size_t pos) {
		size_t i = pos;
		size_t j;
		while (i >>= 1) {
			j = (i << 1);
			j |= cmp_(data_[j], data_[j + 1]);
			data_[i] = data_[j];
			map_[i] = map_[j];
		}
	}
	/* Adjust the tree */
	void adjust_tree(size_t pos, size_t limit) {
		size_t i = pos;
		size_t j;
		while ((limit >>= 1) && (i >>= 1)) {
			j = (i << 1);
			j |= cmp_(data_[j], data_[j + 1]);
			data_[i] = data_[j];
			map_[i] = map_[j];
		}
	}
	/* Get the next position */
	size_t nxtpo2(register size_t x) {
		register int i = 1;
		while (i < sizeof(size_t) * 8) {
			x |= (x >> i);
			i <<= 1;
		}
		return x + 1;
	}
	size_t k_; /* the number of players */
	T* data_;
	T*** tree_;
	size_t* map_;
	size_t* id_map_;
	Compare cmp_; /* the compare function that compare the priorities of two values */
};
