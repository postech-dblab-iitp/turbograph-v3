#pragma once
#ifndef TYPEDEF_H
#define TYPEDEF_H

#include <cstdint>
#include <omp.h>
#include <functional>
#include <sys/mman.h>
#include <sys/stat.h>
#include <mpi.h>

#include <iostream>
#include <stdint.h>
#include <climits>
#include <vector>
#include <string>
#include <cmath>

//#include "Global.hpp"
#include "pcg_random.hpp"

#define __XCONCAT2(a, b) a ## b
#define __XCONCAT(a, b) __XCONCAT2(a, b)
#define CACHELINE_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define CACHE_PADOUT  \
  char __XCONCAT(__padout, __COUNTER__)[0] __attribute__((aligned(CACHELINE_SIZE)))

#ifndef LATENT_FACTOR_K
#define LATENT_FACTOR_K 2
#endif

#define LATENT_VECTOR_WO_BIAS

enum ReturnStatus {
	OK,
	DONE,
	FAIL,
	ON_GOING,
};

enum EdgeType {
    OUTEDGE = 0,
    INEDGE = 1,
    OUTEDGEFULLLIST=2,
    INEDGEFULLLIST=3,
    INOUTEDGE = 4,
};

enum DynamicDBType {
    INSERT = 0,
    DELETE = 1,
    ALL = 2,
    OLD = 3,
    DELTA = 4,
    NEW = 5,
};

enum IOMode {
    READ_IO = 0,
    WRITE_IO = 1,
};

enum Op {
	PLUS,
	MINUS,
	MULTIPLY,
    DIVIDE,
	LOR,
	LAND,
	MAX,
	MIN,
    BOR,
	UNDEFINED,
	// can be added
};

enum CompareType {
    GT = 0,
    GE = 1,
    LT = 2,
    LE = 3,
    NONE = 4,
};

#define TYPE_64

#ifdef TYPE_64
typedef int64_t node64_t;

typedef int64_t edge_t;
typedef int64_t node_t;
typedef int64_t level_t;

//typedef uint64_t bitmap_t;
typedef unsigned char bitmap_t;

typedef int16_t PartitionID;
typedef int32_t PageID;
typedef int16_t SlotNo;

typedef int32_t ChunkID;
typedef int32_t FrameID;

typedef int32_t degree_t;
typedef uint8_t version_t;
#else

typedef int64_t node64_t;

typedef int64_t edge_t;
typedef int32_t node_t;
typedef int64_t level_t;

typedef unsigned char bitmap_t;

typedef int16_t PartitionID;
typedef int32_t PageID;
typedef int16_t SlotNo;

typedef int32_t ChunkID;
typedef int32_t FrameID;

typedef int32_t degree_t;
typedef uint8_t version_t;
#endif

typedef std::pair<node_t, node_t> EdgePair;

// Range : [begin, end]
template <typename T>
class Range {

  public:

	Range() : begin(-1), end(-1) {}
	Range(T b, T e) : begin(b), end(e) {}

	inline T length() const {
		return (end - begin + 1);
	}

	inline bool contains(T o) const {
		return (o <= end && begin <= o);
	}
	inline bool contains(Range<T> o) const {
		return (begin <= o.begin && o.end <= end);
	}

	inline void Set(Range<T> o) {
		begin = o.begin;
		end = o.end;
	}
	inline void Set(T b, T e) {
		begin = b;
		end = e;
	}
	inline void SetBegin(T b) {
		begin = b;
	}
	inline void SetEnd(T e) {
		end = e;
	}

	inline T GetBegin() const {
		return begin;
	}
	inline T GetEnd() const {
		return end;
	}

	inline Range<T> Union(Range left, Range right) {
		ALWAYS_ASSERT(left.end == right.begin);
		return Range(left.begin, right.end);
	}

	inline Range<T> Intersection(Range right) {
		Range<T> result(std::max(begin, right.begin), std::min(end, right.end));
		if (result.end < result.begin) {
			return Range<T>(0, -1);
		} else {
			return result;
		}
	}

	inline bool Overlapped(Range right) {
		if (begin <= right.begin) {
			return (end >= right.begin);
		} else {
			return (begin <= right.end);
		}
	}

	template <typename elem_t>
	friend bool operator> (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator>= (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator<= (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator< (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator== (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator!= (Range<elem_t>& l, Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator> (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator< (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator< (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator== (Range<elem_t>& l, const Range<elem_t>& r);
	template <typename elem_t>
	friend bool operator!= (Range<elem_t>& l, const Range<elem_t>& r);

  public:
	T begin;
	T end;
};

template <typename elem_t>
bool operator> (Range<elem_t>& l, Range<elem_t>& r) {
	return (l.begin > r.begin || ((l.begin == r.begin) && l.end > r.end));
}
template <typename elem_t>
bool operator< (Range<elem_t>& l, Range<elem_t>& r) {
	return (l.begin < r.begin || ((l.begin == r.begin) && l.end < r.end));
}
template <typename elem_t>
bool operator>= (Range<elem_t>& l, Range<elem_t>& r) {
	return (l.begin > r.begin || ((l.begin == r.begin) && l.end >= r.end));
}
template <typename elem_t>
bool operator<= (Range<elem_t>& l, Range<elem_t>& r) {
	return (l.begin < r.begin || ((l.begin == r.begin) && l.end <= r.end));
}
template <typename elem_t>
bool operator== (Range<elem_t>& l, Range<elem_t>& r) {
	return (l.begin == r.begin && l.end == r.end);
}
template <typename elem_t>
bool operator!= (Range<elem_t>& l, Range<elem_t>& r) {
	return !(l == r);
}
template <typename elem_t>
bool operator> (Range<elem_t>& l, const Range<elem_t>& r) {
	return (l.begin > r.begin || ((l.begin == r.begin) && l.end > r.end));
}
template <typename elem_t>
bool operator< (Range<elem_t>& l, const Range<elem_t>& r) {
	return (l.begin < r.begin || ((l.begin == r.begin) && l.end < r.end));
}
template <typename elem_t>
bool operator<= (Range<elem_t>& l, const Range<elem_t>& r) {
	return (l.begin < r.begin || ((l.begin == r.begin) && l.end <= r.end));
}
template <typename elem_t>
bool operator== (Range<elem_t>& l, const Range<elem_t>& r) {
	return (l.begin == r.begin && l.end == r.end);
}
template <typename elem_t>
bool operator!= (Range<elem_t>& l, const Range<elem_t>& r) {
	return !(l == r);
}



template <typename node_t_ = node_t>
struct EdgePairEntry {
	node_t_ src_vid_;
	node_t_ dst_vid_;

	EdgePairEntry() {}

	EdgePairEntry(EdgePairEntry<int64_t>& i) {
		this->src_vid_ = i.src_vid_;
		this->dst_vid_ = i.dst_vid_;
	}

	EdgePairEntry(EdgePairEntry<int32_t>& i) {
		this->src_vid_ = i.src_vid_;
		this->dst_vid_ = i.dst_vid_;
	}
	EdgePairEntry(const EdgePairEntry<int64_t>& i) {
		this->src_vid_ = i.src_vid_;
		this->dst_vid_ = i.dst_vid_;
	}

	EdgePairEntry(const EdgePairEntry<int32_t>& i) {
		this->src_vid_ = i.src_vid_;
		this->dst_vid_ = i.dst_vid_;
	}

	template <typename t_>
	friend bool operator> (EdgePairEntry<t_> &l, EdgePairEntry<t_> &r);
	template <typename t_>
	friend bool operator> (const EdgePairEntry<t_> &l, const EdgePairEntry<t_> &r);
	template <typename t_>
	friend bool operator< (EdgePairEntry<t_> &l, EdgePairEntry<t_> &r);
	template <typename t_>
	friend bool operator< (const EdgePairEntry<t_> &l, const EdgePairEntry<t_> &r);
	template <typename t_>
	friend bool operator== (EdgePairEntry<t_> &l, EdgePairEntry<t_> &r);
	template <typename t_>
	friend bool operator!= (EdgePairEntry<t_> &l, EdgePairEntry<t_> &r);
	template <typename t_>
	friend std::ostream& operator<<(std::ostream& out, EdgePairEntry<t_>& o);
};


template <typename node_t_>
bool operator> (EdgePairEntry<node_t_> &l, EdgePairEntry<node_t_> &r) {
	return (l.src_vid_ > r.src_vid_ ||
	        ((l.src_vid_ == r.src_vid_) && l.dst_vid_ > r.dst_vid_));
}
template <typename node_t_>
bool operator> (const EdgePairEntry<node_t_> &l, const EdgePairEntry<node_t_> &r) {
	return (l.src_vid_ > r.src_vid_ ||
	        ((l.src_vid_ == r.src_vid_) && l.dst_vid_ > r.dst_vid_));
}
template <typename node_t_>
bool operator< (EdgePairEntry<node_t_> &l, EdgePairEntry<node_t_> &r) {
	return (l.src_vid_ < r.src_vid_ ||
	        ((l.src_vid_ == r.src_vid_) && l.dst_vid_ < r.dst_vid_));
}
template <typename node_t_>
bool operator< (const EdgePairEntry<node_t_> &l, const EdgePairEntry<node_t_> &r) {
	return (l.src_vid_ < r.src_vid_ ||
	        ((l.src_vid_ == r.src_vid_) && l.dst_vid_ < r.dst_vid_));
}

template <typename node_t_>
bool operator!= (EdgePairEntry<node_t_> &l, EdgePairEntry<node_t_> &r) {
	return l.src_vid_ != r.src_vid_ || l.dst_vid_ != r.dst_vid_;
}
template <typename node_t_>
bool operator== (EdgePairEntry<node_t_> &l, EdgePairEntry<node_t_> &r) {
	return l.src_vid_ == r.src_vid_ && l.dst_vid_ == r.dst_vid_;

}
template <typename node_t_>
std::ostream& operator<<(std::ostream& out, EdgePairEntry<node_t_>& o) {
	out << o.src_vid_ << "\t" << o.dst_vid_;

}

struct DegreeEntry {
	node_t vid_;
	node_t degree_;
};


class int48_t final {
  public:
	// default constructors
	int48_t() : _s(0) {}
	int48_t(const int48_t& obj) = default;
	int48_t(int48_t&& obj) noexcept = default;

	// converter constructors
	int48_t(const std::int64_t i) : _s(i) {}
	int48_t(const std::int32_t i) : _s(i) {}

	int48_t(const std::uint64_t i) : _s(static_cast<std::int64_t>(i)) {}
	int48_t(const std::uint32_t i) : _s(static_cast<std::int32_t>(i)) {}

	// default assigments
	int48_t& operator=(const int48_t& obj) = default;
	int48_t& operator=(int48_t&& obj) noexcept = default;

	// converter assigments
	int48_t& operator=(std::int64_t i) {
		_s = i;
		return *this;
	}

	int48_t& operator=(std::int32_t i) {
		_s = (int64_t)i;
		return *this;
	}

	int48_t& operator=(std::uint64_t i) {
		_s = (int64_t)i;
		return *this;
	}
	/*
	        int48_t& operator=(std::uint64_t i) {
	            _s = static_cast<std::int64_t>(i);
	            return *this;
	        }
	*/
	// cast operators
	operator std::int64_t() const {
		return _s;
	}
	/*
	        operator std::uint64_t() const {
	            return static_cast<std::uint64_t>(_s);
	        }
	*/
	// comparison
	bool operator==(const int48_t& obj) const {
		return _s == obj._s;
	}

	bool operator!=(const int48_t& obj) const {
		return _s != obj._s;
	}
	/*
	        friend bool operator==(const std::int64_t i, const int48_t& obj);
	        friend bool operator==(const int48_t& obj, const std::int64_t i);

	        friend bool operator!=(const std::int64_t i, const int48_t& obj);
	        friend bool operator!=(const int48_t& obj, const std::int64_t i);
	*/
	friend bool operator==(const std::int64_t i, const int48_t& obj) {
		return i == obj._s;
	}

	friend bool operator==(const int48_t& obj, const std::int64_t i) {
		return i == obj._s;
	}

	friend bool operator!=(const std::int64_t i, const int48_t& obj) {
		return i != obj._s;
	}

	friend bool operator!=(const int48_t& obj, const std::int64_t i) {
		return i != obj._s;
	}

	friend bool operator<=(const int48_t& obj, const std::int64_t i) {
		return obj._s <= i;
	}

	friend bool operator>=(const int48_t& obj, const std::int64_t i) {
		return obj._s >= i;
	}

	friend bool operator<(const int48_t& obj, const std::int64_t i) {
		return obj._s < i;
	}

	friend bool operator>(const int48_t& obj, const std::int64_t i) {
		return obj._s > i;
	}

	friend bool operator<(const int48_t& obj1, const int48_t& obj2) {
		return obj1._s < obj2._s;
	}

	friend bool operator>(const int48_t& obj1, const int48_t& obj2) {
		return obj1._s > obj2._s;
	}

	friend bool operator<=(const std::int64_t i, const int48_t& obj) {
		return i <= obj._s;
	}

	friend bool operator>=(const std::int64_t i, const int48_t& obj) {
		return i >= obj._s;
	}

	friend bool operator<(const std::int64_t i, const int48_t& obj) {
		return i < obj._s;
	}

	friend bool operator>(const std::int64_t i, const int48_t& obj) {
		return i > obj._s;
	}

	friend int48_t operator++(int48_t& obj, int dummy) {
		int48_t tmp = obj;
		obj._s++;
		return tmp;
	}

	friend int48_t operator++(int48_t& obj) {
		obj._s++;
		return obj;
	}

	friend int48_t operator/(int48_t& obj, const std::int64_t i) {
		return obj._s / i;
	}

	friend int48_t operator*(int48_t& obj, const std::int64_t i) {
		return obj._s * i;
	}

	friend int48_t operator-(int48_t& obj, const std::int64_t i) {
		return obj._s - i;
	}

	friend int48_t operator+(int48_t& obj, const std::int64_t i) {
		return obj._s + i;
	}

	// printing
	//   friend std::ostream& operator<<(std::ostream& stream, const int48_t& obj);
	friend std::ostream& operator<<(std::ostream& stream, const int48_t& obj) {
		stream << obj._s;
		return stream;
	}
  private:
	std::int64_t _s : 48;
} __attribute__((packed));

typedef int48_t file_t;

static_assert(sizeof(int48_t)    == 6,  "size of int48_t is not 48bit, check your compiler!");
static_assert(sizeof(int48_t[2]) == 12, "size of int48_t[2] is not 2*48bit, check your compiler!");
static_assert(sizeof(int48_t[3]) == 18, "size of int48_t[3] is not 3*48bit, check your compiler!");
static_assert(sizeof(int48_t[4]) == 24, "size of int48_t[4] is not 4*48bit, check your compiler!");

namespace std {
template<>
struct hash<int48_t> {
	size_t operator()(const int48_t& obj) const {
		// XXX: implement fast path
		return _helper(static_cast<std::int64_t>(obj));
	}

	std::hash<std::int64_t> _helper;
};
}

template <typename T, int K>
struct LatentVector {
	using type = LatentVector<T, K>;
	using value_type = T;

	T k[K];
#ifndef LATENT_VECTOR_WO_BIAS
	T bias;
#endif

	template<typename... Args>
		LatentVector(T first, Args... args) {
			std::vector<T> vec = {first, args...};
			for (auto i = 0; i < K; i++) k[i] = vec[i];
		}

	template <typename U>
	operator LatentVector<U, K>() const {
		LatentVector<U, K> tmp;
		for (auto i = 0; i < K; i++) tmp[i] = k[i];
#ifndef LATENT_VECTOR_WO_BIAS
		tmp.bias = bias;
#endif
		return tmp;
	}

	LatentVector() {
	}

	LatentVector(T init_value_) {
		for (auto i = 0; i < K; i++) k[i] = init_value_;
	}


	~LatentVector() {
	}

	void SetValue(T value_) {
		for (auto i = 0; i < K; i++) k[i] = value_;
	}

#ifndef LATENT_VECTOR_WO_BIAS
	void SetBias(T value_) {
		bias = value_;
	}
#endif

	void Scale(T scale_factor_) {
		for (auto i = 0; i < K; i++) k[i] = scale_factor_ * k[i];
	}

	void Pow(int64_t exponent) {
		for (auto i = 0; i < K; i++) {
			k[i] = std::pow(k[i], exponent);
		}
	}

//Compile error because of UserArguments kjhong
	void SetRandomValue(pcg64_fast* rand_gen_);


	void InitializeHashValue(node_t seed) {
		T total = (T)0;
		T val, mod=0.5;
		for (auto i = 0; i < K; i++) {
			//k[i] = (T)((T) std::hash<node_t>{}(seed+i)) / std::numeric_limits<size_t>::max() + 0.5 + seed + i; //XXX 0.5 == bias
			//k[i] = std::abs(std::cos(seed + i) + 0.5); // XXX for LBP temp
			val = (T)((seed + i + 3) * 1.23456);
			k[i] = std::fmod(val, mod); //XXX GB LP
			total += k[i];
		}
		if (total == (T)0) return;
		for (auto i = 0; i < K; i++) {
			k[i] = k[i] / total;
		}
	}

	void DotProduct(const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
		for (auto i = 0; i < K; i++)
			k[i] = l[i] * r[i];
#ifndef LATENT_VECTOR_WO_BIAS
		bias = l.bias * r.bias;
#endif
	}

	void LogNormalize() {
		// XXX - tslee implement it
        T max = k[0];
        for (auto i = 1; i < K; i++)
            if (k[i] > max) max = k[i];
        for (auto i = 0; i < K; i++) {
            k[i] -= max;
        }
	}

	void Normalize() {
		T total = (T)0;
		for (auto i = 0; i < K; i++)
			total += k[i];
		if (total == (T)0) return;
		for (auto i = 0; i < K; i++) {
			k[i] /= total;
			//k[i] = (k[i] + 0.1) / (1 + K * 0.1); // Clamping?
		}
#ifndef LATENT_VECTOR_WO_BIAS
		bias = total;
#endif
	}
	
	void ScaleNormalize(T scale) {
		T total = (T)0;
		for (auto i = 0; i < K; i++)
			total += k[i];
		if (total == (T)0) return;
		for (auto i = 0; i < K; i++) {
			k[i] *= scale;
			k[i] /= total;
		}
	}

	void Dump(node_t vid, std::string vector_name) {
		std::string to_dump = "Vector " + vector_name + " (" + std::to_string(vid) + ") ";
		for (auto i = 0; i < K; i++) {
			to_dump += std::to_string(k[i]) + " ";
		}
		fprintf(stdout, "%s\n", to_dump.c_str());
	}

	T cwiseAbsSum() {
		T sum = 0;
		for (auto i = 0; i < K; i++) {
			sum += k[i] > 0 ? k[i] : -k[i];
		}
		return sum;
	}

	T Sum() {
		T sum = 0;
		for (auto i = 0; i < K; i++) {
			sum += k[i];
		}
		return sum;
	}
	T Max() {
		T max = -1;
		for (auto i = 0; i < K; i++) {
			if (max < k[i]) max = k[i];
		}
		return max;
	}

	LatentVector<T, K> Abs() {
		LatentVector<T, K> tmp;
		for (auto i = 0; i < K; i++) {
			tmp[i] = (k[i] > 0) ? k[i] : -k[i];
		}
		return tmp;
	}

	bool isdiffOverThreshold (LatentVector<T, K> other, T threshold) {
		T diff;
		for (auto i = 0; i < K; i++) {
			diff = std::fabs(k[i] - other[i]);
			if (diff > threshold) return true;
		}
		return false;
	}

	int64_t ArgMax() {
		T max = std::numeric_limits<T>::min();
		int64_t idx = 0;
		for (auto i = 0; i < K; i++) {
			if (k[i] > max) {
				max = k[i];
				idx = i;
			}
		}
		return idx;
	}

	inline T& operator[] (node_t idx) {
		//ALWAYS_ASSERT(idx >= 0 && idx < K);
		return k[idx];
	}

	inline const T& operator[] (node_t idx) const {
		//ALWAYS_ASSERT(idx >= 0 && idx < K);
		return k[idx];
	}

	LatentVector<T, K>& operator=(const LatentVector<T, K> &l) {
		for (auto i = 0; i < K; i++) 
            k[i] = l[i];
#ifndef LATENT_VECTOR_WO_BIAS
        bias = l.bias;
#endif
        return *this;
	}

	//template <typename t_>
	//	friend bool operator== (LatentVector<t_> &l, LatentVector<t_> &r);
	template <typename t_, int k_>
		friend bool operator== (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	//template <typename t_>
	//	friend bool operator!= (LatentVector<t_> &l, LatentVector<t_> &r);
	template <typename t_, int k_>
		friend bool operator!= (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	template <typename t_, int k_>
		friend LatentVector<t_, k_> operator* (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	template <typename t_, int k_>
		friend LatentVector<t_, k_> operator/ (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	template <typename t, typename t_, int k_>
		friend LatentVector<t_, k_> operator/ (const LatentVector<t_, k_> &l, const t &r);
	template <typename t_, int k_>
		friend t_ operator* (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	template <typename t, typename t_, int k_>
		friend LatentVector<t_, k_> operator* (const t &l, const LatentVector<t_, k_> &r);
	template <typename t, typename t_, int k_>
		friend LatentVector<t_, k_> operator* (const LatentVector<t_, k_> &l, const t &r);
	template <typename t_, int k_>
		friend LatentVector<t_, k_> operator+ (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	template <typename t_, int k_>
		friend LatentVector<t_, k_> operator+ (const LatentVector<t_, k_> &l, const t_ r);
	template <typename t_, int k_>
		friend LatentVector<t_, k_> operator- (const LatentVector<t_, k_> &l, const LatentVector<t_, k_> &r);
	template <typename t_, int k_>
		friend std::ostream& operator<< (std::ostream& out, const LatentVector<t_, k_> &l);
};


/*template <typename T>
  bool operator== (LatentVector<T> &l, LatentVector<T> &r) {
  ALWAYS_ASSERT(l.latent_factor == r.latent_factor);
  for (auto i = 0; i < l.latent_factor; i++)
  if (l[i] != r[i]) return false;
  return true;
  }*/
template <typename T, int K>
bool operator== (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
    return checkEquality(l, r);
	//ALWAYS_ASSERT(l.latent_factor == r.latent_factor);
	//for (auto i = 0; i < K; i++)
	//	if (l[i] != r[i]) return false;
	//if (l.bias != r.bias) return false;
	//return true;
}
/*template <typename T>
  bool operator!= (LatentVector<T> &l, LatentVector<T> &r) {
  ALWAYS_ASSERT(l.latent_factor == r.latent_factor);
  for (auto i = 0; i < l.latent_factor; i++)
  if (l[i] != r[i]) return true;
  return false;
  }*/
template <typename T, int K>
bool operator!= (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
    return !checkEquality(l, r);
	//for (auto i = 0; i < K; i++)
	//	if (l[i] != r[i]) return true;
	//if (l.bias != r.bias) return true;
	//return false;
}
template <typename T, int K>
LatentVector<T, K> operator* (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] * r[i];
#ifndef LATENT_VECTOR_WO_BIAS
	result.bias = l.bias * r.bias;
#endif
	return result;
}
template <typename T, int K>
LatentVector<T, K> operator/ (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] / r[i];
#ifndef LATENT_VECTOR_WO_BIAS
	result.bias = l.bias / r.bias;
#endif
	return result;
}
template <typename S, typename T, int K>
LatentVector<T, K> operator/ (const LatentVector<T, K> &l, const S &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] / r;
	return result;
}
template <typename T, int K>
T operator* (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
	T result = (T)0;
	for (auto i = 0; i < K; i++)
		result += (l[i] * r[i]);
	return result;
}
template <typename S, typename T, int K>
LatentVector<T, K> operator* (const S &l, const LatentVector<T, K> &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l * r[i];
#ifndef LATENT_VECTOR_WO_BIAS
	result.bias = l * r.bias;
#endif
	return result;
}
template <typename S, typename T, int K>
LatentVector<T, K> operator* (const LatentVector<T, K> &l, const S &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = r * l[i];
#ifndef LATENT_VECTOR_WO_BIAS
	result.bias = r * l.bias;
#endif
	return result;
}
template <typename T, int K>
LatentVector<T, K> operator+ (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] + r[i];
#ifndef LATENT_VECTOR_WO_BIAS
	result.bias = l.bias + r.bias;
#endif
	return result;
}
template <typename T, int K>
LatentVector<T, K> operator+ (const LatentVector<T, K> &l, const T r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] + r;
	return result;
}
template <typename T, int K>
LatentVector<T, K> operator- (const LatentVector<T, K> &l, const LatentVector<T, K> &r) {
	LatentVector<T, K> result;
	for (auto i = 0; i < K; i++)
		result[i] = l[i] - r[i];
#ifndef LATENT_VECTOR_WO_BIAS
	result.bias = l.bias - r.bias;
#endif
	return result;
}
template <typename T, int K>
std::ostream& operator<< (std::ostream& out, const LatentVector<T, K> &l) {
    for (auto i = 0; i < K; i++) {
    	out << l[i] << " ";
    }
    return out;
}

template<typename T>
typename LatentVector<T, LATENT_FACTOR_K>::type LOGNORMALIZE(T first, T second) {
	LatentVector<T, LATENT_FACTOR_K> tmp(first, second);
	tmp.LogNormalize();
	return tmp;
}
template<typename T>
typename LatentVector<T, LATENT_FACTOR_K>::type NORMALIZE(T first, T second) {
	LatentVector<T, LATENT_FACTOR_K> tmp(first, second);
	tmp.Normalize();
	return tmp;
}
template<typename T>
typename LatentVector<T, LATENT_FACTOR_K>::type SCALENORMALIZE(T first, T second, T scale) {
	LatentVector<T, LATENT_FACTOR_K> tmp(first, second);
	tmp.ScaleNormalize(scale);
	return tmp;
}
template<typename T>
T LOGNORMALIZE(T tmp) {
	tmp.LogNormalize();
	return tmp;
}
template<typename T>
T NORMALIZE(T tmp) {
	tmp.Normalize();
	return tmp;
}
template<typename T, typename S>
T SCALENORMALIZE(T tmp, S scale) {
	tmp.ScaleNormalize(scale);
	return tmp;
}
/*
template<typename... Ts>
typename LatentVector<float, 2>::type LOGNORMALIZE(Ts... ts) {
	LatentVector<float, 2> tmp(ts...);
	tmp.LogNormalize();
	return tmp;
}
template<typename... Ts>
typename LatentVector<float, 2>::type NORMALIZE(Ts... ts) {
	LatentVector<float, 2> tmp(ts...);
	tmp.Normalize();
	return tmp;
}
*/

template<typename T>
typename T::value_type SUM(T input) {
	return input.Sum();
}
template<typename T>
typename T::value_type MMAX(T input) {
	return input.Max();
}


template<typename T>
T _abs(T input) {
	if (input > 0) { return input; }
	else { return -1 * input; }
}
template<>
LatentVector<double, LATENT_FACTOR_K> _abs(LatentVector<double, LATENT_FACTOR_K> input);
template<>
LatentVector<float, LATENT_FACTOR_K> _abs(LatentVector<float, LATENT_FACTOR_K> input);
template<>
LatentVector<int32_t, LATENT_FACTOR_K> _abs(LatentVector<int32_t, LATENT_FACTOR_K> input);
template<>
LatentVector<int64_t, LATENT_FACTOR_K> _abs(LatentVector<int64_t, LATENT_FACTOR_K> input);


//template <typename T>
//T idenelem(Op operation);

template <typename T>
T idenelem(Op operation) {
	switch(operation) {
	case PLUS:
		return 0;
	case MULTIPLY:
		return 1;
	case LOR:
		return false;
	case LAND:
		return true;
	case MAX:
		return std::numeric_limits<T>::min();
	case MIN:
		return std::numeric_limits<T>::max();
	default:
        T t;
        return t;
	}
}

//XXX - tslee: 3/5 2018

template <>
LatentVector<double, LATENT_FACTOR_K> idenelem(Op operation);

template <>
LatentVector<float, LATENT_FACTOR_K> idenelem(Op operation);

template <>
LatentVector<int32_t, LATENT_FACTOR_K> idenelem(Op operation);

template <>
LatentVector<int64_t, LATENT_FACTOR_K> idenelem(Op operation);

struct SimpleContainer {
	SimpleContainer(char* p, std::size_t c, std::size_t s): data(p), capacity(c), size_used(s) {}
	SimpleContainer(): data(nullptr), capacity(0), size_used(0) {}

	std::size_t size_available() {
		return (capacity - size_used);
	}

	char* data;
	std::size_t capacity;
	std::size_t size_used;
};

template <typename T>
MPI_Datatype PrimitiveType2MPIType(T dummy) {
	double doubledummy;
	float floatdummy;
	char chardummy;
	signed char signedchardummy;
	unsigned char unsignedchardummy;
	signed char bytedummy;
	short shortdummy;
	unsigned short unsignedshortdummy;
	int intdummy;
	unsigned unsigneddummy;
	long longdummy;
	unsigned long unsignedlongdummy;
	long double longdoubledummy;
	long long int longlongintdummy;
	unsigned long long unsignedlonglongdummy;
	bool booldummy;
	{
		if(typeid(dummy) == typeid (doubledummy))
			return MPI_DOUBLE;
		else if (typeid(dummy) == typeid(floatdummy))
			return MPI_FLOAT;
		else if (typeid(dummy) == typeid(chardummy))
			return MPI_CHAR;
		else if (typeid(dummy) == typeid(signedchardummy) )
			return MPI_SIGNED_CHAR;
		else if (typeid(dummy) == typeid(unsignedchardummy) )
			return MPI_UNSIGNED_CHAR;
		else if (typeid(dummy) == typeid(bytedummy) )
			return MPI_BYTE;
		else if (typeid(dummy) == typeid(shortdummy) )
			return MPI_SHORT;
		else if (typeid(dummy) == typeid(unsignedshortdummy) )
			return MPI_UNSIGNED_SHORT;
		else if (typeid(dummy) == typeid(intdummy) )
			return MPI_INT;
		else if (typeid(dummy) == typeid(unsigneddummy) )
			return MPI_UNSIGNED;
		else if (typeid(dummy) == typeid(longdummy) )
			return MPI_LONG;
		else if (typeid(dummy) == typeid(unsignedlongdummy))
			return MPI_UNSIGNED_LONG;
		else if (typeid(dummy) == typeid(longdoubledummy) )
			return MPI_LONG_DOUBLE;
		else if (typeid(dummy) == typeid(longlongintdummy) )
			return MPI_LONG_LONG_INT;
		else if (typeid(dummy) == typeid(unsignedlonglongdummy))
			return MPI_UNSIGNED_LONG_LONG;
		else if (typeid(dummy) == typeid(booldummy))
			// return MPI::BOOL;
			return MPI_CHAR;
		else
			abort();
	}
	return (MPI_Datatype)-1;
}

struct VerIDPageIDPair {
    int version_id;
    PageID pid;

    VerIDPageIDPair() { }
    ~VerIDPageIDPair() { }
    VerIDPageIDPair(int version_id_, PageID pid_) :
        version_id(version_id_),
        pid(pid_) { }
};

#ifdef OldMessageTransfer

template <typename T>
struct WritebackMessage {
    node_t dst_vid;
    T message;
    T old_message;

    WritebackMessage() {};
    WritebackMessage(node_t vid, T elem) {
        dst_vid = vid;
        message = elem;
    }
    WritebackMessage(node_t vid, T elem, T old_elem) {
        dst_vid = vid;
        message = elem;
        old_message = old_elem;
    }
};

template <typename T>
struct WritebackMessageWOOldMessage {
	node_t dst_vid;
	T message;
	WritebackMessageWOOldMessage() {};
	WritebackMessageWOOldMessage(node_t vid, T elem) {
		dst_vid = vid;
		message = elem;
	}
	
    template <typename type>
	friend bool operator < (const WritebackMessageWOOldMessage<type>& l, const WritebackMessageWOOldMessage<type>& r);
	template <typename type>
	friend bool operator >(const WritebackMessageWOOldMessage<type>& l, const WritebackMessageWOOldMessage<type>& r);

	template <typename type>
	friend bool operator < (WritebackMessageWOOldMessage<type>& l, WritebackMessageWOOldMessage<type>& r);
	template <typename type>
	friend bool operator >(WritebackMessageWOOldMessage<type>& l, WritebackMessageWOOldMessage<type>& r);
};

template <typename type>
bool operator < (WritebackMessageWOOldMessage<type>& l, WritebackMessageWOOldMessage<type>& r) {
	return l.dst_vid < r.dst_vid;
}
template <typename type>
bool operator > (WritebackMessageWOOldMessage<type>& l, WritebackMessageWOOldMessage<type>& r) {
	return l.dst_vid > r.dst_vid;
}
template <typename type>
bool operator < (const WritebackMessageWOOldMessage<type>& l, const WritebackMessageWOOldMessage<type>& r) {
	return l.dst_vid < r.dst_vid;
}
template <typename type>
bool operator > (const WritebackMessageWOOldMessage<type>& l, const WritebackMessageWOOldMessage<type>& r) {
	return l.dst_vid > r.dst_vid;
}

#else

template <typename T>
struct WritebackMessage {
	node_t dst_vid;
	T message;
	WritebackMessage() {};
	WritebackMessage(node_t vid, T elem) {
		dst_vid = vid;
		message = elem;
	}
	
    template <typename type>
	friend bool operator < (const WritebackMessage<type>& l, const WritebackMessage<type>& r);
	template <typename type>
	friend bool operator >(const WritebackMessage<type>& l, const WritebackMessage<type>& r);

	template <typename type>
	friend bool operator < (WritebackMessage<type>& l, WritebackMessage<type>& r);
	template <typename type>
	friend bool operator >(WritebackMessage<type>& l, WritebackMessage<type>& r);
};

template <typename type>
bool operator < (WritebackMessage<type>& l, WritebackMessage<type>& r) {
	return l.dst_vid < r.dst_vid;
}
template <typename type>
bool operator > (WritebackMessage<type>& l, WritebackMessage<type>& r) {
	return l.dst_vid > r.dst_vid;
}
template <typename type>
bool operator < (const WritebackMessage<type>& l, const WritebackMessage<type>& r) {
	return l.dst_vid < r.dst_vid;
}
template <typename type>
bool operator > (const WritebackMessage<type>& l, const WritebackMessage<type>& r) {
	return l.dst_vid > r.dst_vid;
}

#endif

struct DummyUDFDataStructure {
	char data[1024];
};

enum RDWRMODE {
	NOUSE,
	RDONLY,
	WRONLY,
	RDWR
};

template <typename T>
struct DeltaMessage {
	node_t dst_vid;
	T message;
	
    DeltaMessage() {};
	DeltaMessage(node_t vid, T elem) {
		dst_vid = vid;
		message = elem;
	}
};

class Version {
    public:

    Version() : update_ver(-1), superstep_ver(-1) {}
    Version(int u, int s) : update_ver(u), superstep_ver(s) {}

    int GetUpdateVersion() { return update_ver; }
    int GetSuperstepVersion() { return superstep_ver; }
    void SetUpdateVersion(int u) { update_ver = u; }
    void SetSuperstepVersion(int s) { superstep_ver = s; }
    void Set(int u, int s) { 
        update_ver = u;
        superstep_ver = s; 
    }

    private:

    int update_ver;
    int superstep_ver;
};

struct PaddedIdx {
	int64_t idx;
	CACHE_PADOUT;
};

#define FLOAT_PRECISION_TOLERANCE 0.005 // original
//#define FLOAT_PRECISION_TOLERANCE 0
#define AbsMacro(x)    ((x) < 0 ? -(x) : (x))
#define Max(a, b) ((a) > (b) ? (a) : (b))

template <typename T>
inline T AbsDif(T a, T b)
{
    return AbsMacro(a - b);
}

inline double RelDif(double a, double b)
{
    double c = AbsMacro(a);
    double d = AbsMacro(b);
    d = Max(c, d);
    return d == 0.0 ? 0.0 : AbsMacro(a - b) / d;
}

inline float RelDif(float a, float b)
{
    float c = AbsMacro(a);
    float d = AbsMacro(b);
    d = Max(c, d);
    return d == 0.0 ? 0.0 : AbsMacro(a - b) / d;
}

template <typename K>
inline bool checkEquality(K a, K b) {
    return (a == b);
}

//template <>        
//inline bool checkEquality<int32_t>(int32_t a, int32_t b) {
//    return (AbsDif(a,b) <= 1);
//}

template <>        
inline bool checkEquality<double>(double a, double b) {
    return (RelDif(a,b) <= FLOAT_PRECISION_TOLERANCE);
}

template <>        
inline bool checkEquality<float>(float a, float b) {
    return (RelDif(a,b) <= FLOAT_PRECISION_TOLERANCE);
}

template <>        
inline bool checkEquality<LatentVector<bool, LATENT_FACTOR_K>>(LatentVector<bool, LATENT_FACTOR_K> a, LatentVector<bool, LATENT_FACTOR_K> b) {
    for (int i = 0; i < LATENT_FACTOR_K; i++) {
        if (!checkEquality(a[i], b[i])) return false;
    }
    return true;
}

template <>        
inline bool checkEquality<LatentVector<int32_t, LATENT_FACTOR_K>>(LatentVector<int32_t, LATENT_FACTOR_K> a, LatentVector<int32_t, LATENT_FACTOR_K> b) {
    for (int i = 0; i < LATENT_FACTOR_K; i++) {
        if (!checkEquality(a[i], b[i])) return false;
    }
    return true;
}

template <>        
inline bool checkEquality<LatentVector<int64_t, LATENT_FACTOR_K>>(LatentVector<int64_t, LATENT_FACTOR_K> a, LatentVector<int64_t, LATENT_FACTOR_K> b) {
    for (int i = 0; i < LATENT_FACTOR_K; i++) {
        if (!checkEquality(a[i], b[i])) return false;
    }
    return true;
}

template <>        
inline bool checkEquality<LatentVector<double, LATENT_FACTOR_K>>(LatentVector<double, LATENT_FACTOR_K> a, LatentVector<double, LATENT_FACTOR_K> b) {
    for (int i = 0; i < LATENT_FACTOR_K; i++) {
        if (RelDif(a[i], b[i]) > FLOAT_PRECISION_TOLERANCE) return false;
    }
    return true;
}

template <>        
inline bool checkEquality<LatentVector<float, LATENT_FACTOR_K>>(LatentVector<float, LATENT_FACTOR_K> a, LatentVector<float, LATENT_FACTOR_K> b) {
    for (int i = 0; i < LATENT_FACTOR_K; i++) {
        if (RelDif(a[i], b[i]) > FLOAT_PRECISION_TOLERANCE) return false;
    }
    return true;
}

template <typename K>
inline bool checkZero(K a) {
    return (a == 0);
}

template <>        
inline bool checkZero<double>(double a) {
    return (RelDif(a,0.) <= FLOAT_PRECISION_TOLERANCE);
}

template <>        
inline bool checkZero<float>(float a) {
    return (RelDif(a,0.f) <= FLOAT_PRECISION_TOLERANCE);
}




#endif
