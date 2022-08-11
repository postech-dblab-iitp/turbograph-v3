#pragma once

namespace duckdb {

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

} // namespace duckdb
