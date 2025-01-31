#ifndef DISTRIBUTED_VECTOR_H
#define DISTRIBUTED_VECTOR_H

/*
 * Design of the atomic_util
 *
 * This class implements a set of atomic operations for various data types.
 * For frequently used major operations (e.g. CAS, atomic_fetch_add for int64_t 
 * type), we implement template specialization to improve speed. Note that CAS 
 * operation is essential to implement various atomic operations.
 */

#include <iostream>
#include <algorithm>
#include <atomic>
#include <queue>
#include <future>

template <typename T> inline bool CAS (T* ptr, T old_val, T new_val) {
    fprintf(stderr, "CAS on this Operand is not defined\n");
    abort();
    return false;
    /*if (sizeof(T) == 8) {
        return __sync_bool_compare_and_swap((int64_t *)ptr, *((bool *)&old_val), *((bool *)&new_val));
    } else {
        fprintf(stderr, "CAS on this Operand is not defined\n");
        abort();
        return false;
    }*/
}
template<> inline bool CAS (bool* ptr, bool old_val, bool new_val) {
	return std::atomic_compare_exchange_weak((std::atomic<bool>*) ptr, &old_val, new_val);
}
template<> inline bool CAS (int64_t* ptr, int64_t old_val, int64_t new_val) {
	return std::atomic_compare_exchange_weak( (std::atomic<int64_t>*) ptr, &old_val, new_val);
}
template<> inline bool CAS (int32_t* ptr, int32_t old_val, int32_t new_val) {
	return std::atomic_compare_exchange_weak((std::atomic<int32_t>*) ptr, &old_val, new_val);
}
/*template<> inline bool CAS (double* ptr, double old_val, double new_val) {
    return __sync_bool_compare_and_swap((int64_t *)ptr, *((int64_t *)&old_val), *((int64_t *)&new_val));
	//return std::atomic_compare_exchange_weak((std::atomic<int64_t>*) ptr, (int64_t*) &old_val, *((int64_t*) &new_val));
}*/
template<> inline bool CAS (double* ptr, volatile double old_val, volatile double new_val) {
    return __sync_bool_compare_and_swap((int64_t *)ptr, *((int64_t *)&old_val), *((int64_t *)&new_val));
	//return std::atomic_compare_exchange_weak((std::atomic<int64_t>*) ptr, (int64_t*) &old_val, *((int64_t*) &new_val));
}
template<> inline bool CAS (float* ptr, float old_val, float new_val) {
	return std::atomic_compare_exchange_weak((std::atomic<int32_t>*) ptr, (int32_t*) &old_val, *((int32_t*) &new_val));
}

template<typename T>
inline T atomic_fetch_add_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected + arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<>
inline int32_t atomic_fetch_add_<int32_t>(std::atomic<int32_t> *obj, int32_t arg) {
	return std::atomic_fetch_add(obj, arg);
}

inline int32_t atomic_fetch_add_(std::atomic<int32_t> *obj, int64_t arg) {
	return atomic_fetch_add_(obj, (int32_t) arg);
}

template<>
inline int64_t atomic_fetch_add_<int64_t>(std::atomic<int64_t> *obj, int64_t arg) {
	return std::atomic_fetch_add(obj, arg);
}

inline int64_t atomic_fetch_add_(std::atomic<int64_t> *obj, int32_t arg) {
	return atomic_fetch_add_(obj, (int64_t) arg);
}

template<>
inline double atomic_fetch_add_<double>(std::atomic<double> *obj, double arg) {
	double expected = *((double*) obj);
	//double expected = obj->load();
	double result;
	do {
		result = expected + arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<>
inline float atomic_fetch_add_<float>(std::atomic<float> *obj, float arg) {
	float expected = *((float*) obj);
	//float expected = obj->load();
	float result;
	do {
		result = expected + arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}


template<typename T>
inline T atomic_fetch_max_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected > arg ? expected : arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_min_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected < arg ? expected : arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_prod_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected * arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_land_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected && arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_lor_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected || arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_band_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected & arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_bor_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected | arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_lxor_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = !expected != !arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}

template<typename T>
inline T atomic_fetch_bxor_(std::atomic<T> *obj, T arg) {
	T expected = *((T*) obj);
	//T expected = obj->load();
	T result;
	do {
		result = expected ^ arg;
	} while (!atomic_compare_exchange_weak(obj, &expected, result));
	return expected;
}


#endif
