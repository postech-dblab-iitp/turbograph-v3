#ifndef __REQUESTRESPOND_H
#define __REQUESTRESPOND_H

/*
 * Design of the RequestRespond
 *
 * This class implements a request-response mechanism that enables machines on
 * a network to communicate with each other. Various request types are provided
 * such as reading DistributedVector, writing messages, reading materialized
 * adjacency list, etc.
 *
 * In the system initialization step, a thread is spawned to probe incoming 
 * requests from other machines and respond to them. The probing step can be
 * handled using MPI_Probe() API. According to the type of request, the
 * corresponding task is enqueued into the asynchronous thread pool (Please
 * refer ReceiveRequest() function).
 *
 * In particular, there are two important functions handled by this class. One
 * is RespondAdjListBatchIO(), and the other is SendDataInBuffer().
 *     - RespondAdjListBatchIO: It handles I/O requests for materialized 
 *     adjacency lists. To achieve the goal, we first find the pages that needs
 *     to be read from disk (by FindPageIdsToReadFrom function). After that,
 *     we spawn a thread that materializes full adjacency lists from pages
 *     (by MaterializeAndEnqueue function). The materialized adjacency lists 
 *     are put into a queue (req_data_queue), and a pre-spawned thread sends 
 *     the adjacency list in the queue to the requesting machine over the 
 *     network.
 *     - SendDataInBuffer: It sends data in network I/O buffer. Usually, this
 *     function is used by TG_DistributedVectorWindow class. In that class,
 *     we store the locally aggregated messages in a buffer and put them into
 *     a queue. In the SendDataInBuffer() function, the buffer is continuously 
 *     popped from the queue and the data in the buffer is sent over the network.
 *     This process continues until it meets the EOM.
 */

#include <thread>
#include <unistd.h>
#include <mutex>
#include <cstring>
#include <algorithm>

#include "analytics/core/Global.hpp"
#include "analytics/core/TG_DistributedVectorBase.hpp"
#include "analytics/core/turbo_callback.hpp"
#include "analytics/core/turbo_tcp.hpp"
#include "analytics/datastructure/disk_aio_factory.hpp"
#include "analytics/datastructure/TwoLevelBitMap.hpp"
#include "analytics/datastructure/FixedSizeVector.hpp"
#include "analytics/util/Aio_Helper.hpp"
#include "analytics/util/TG_NWSM_Utility.hpp"
#include "analytics/util/VariableSizedMemoryAllocatorWithCircularBuffer.hpp"
#include "analytics/util/atomic_util.hpp"

#define USE_DEGREE_THRESHOLD 1

void* TurboMalloc(int64_t sz);
void TurboFree(void* ptr, int64_t sz);

// TODO - tslee: move below Operation/AtomicOperation.. to another file
template <typename T, Op op> inline void Operation(T* a, T& b) { 
	fprintf(stdout, "%s op = %d\n", typeid(T).name(), op);
	LOG_ASSERT(false); 
}
template <typename T, Op op> inline void InverseOperation(T* a, T& b) { LOG_ASSERT(false); }
template <typename T, Op op> inline T AtomicOperation(T* a, T& b) { LOG_ASSERT(false); }
template <typename T, Op op> inline void UpdateAggregation(T* agg, T& o, T& n) { 
    fprintf(stdout, "sizeof(T) = %d, op = %d\n", sizeof(T), op);
    LOG_ASSERT(false); 
}
template <typename T, Op op> inline void UpdateAggregationWithCnt(T* a, T& b) { LOG_ASSERT(false); }
template <typename T, Op op> inline void UpdateAggregationWithCntStatic(T* a, T& b) { LOG_ASSERT(false); }
template <typename T, Op op> inline void AtomicUpdateAggregation(T* agg, T& o, T& n) { 
	fprintf(stdout, "%s op = %d\n", typeid(T).name(), op);
	LOG_ASSERT(false); 
}
template <typename T, Op op> inline void AtomicUpdateAggregationWithCnt(T* a, T& b) { LOG_ASSERT(false); }
template <typename T, Op op> inline void AtomicUpdateAggregationWithCntStatic(T* a, T& b) { LOG_ASSERT(false); }

template <typename T, Op op> inline void AccumulateNewAndOldMessagesIntoTGB(T* target, T& new_msg, T& old_msg) { 
	fprintf(stdout, "%s op = %d\n", typeid(T).name(), op);
  LOG_ASSERT(false); 
}
template <typename T, Op op> inline void AtomicAccumulateNewAndOldMessagesIntoTGB(T* target, T& new_msg, T& old_msg) { 
	fprintf(stdout, "%s op = %d\n", typeid(T).name(), op);
  LOG_ASSERT(false); 
}
template <typename T, Op op> inline void AtomicAccumulateNewAndOldMessagesIntoTGB(T* target, T& new_msg) { 
	fprintf(stdout, "%s op = %d\n", typeid(T).name(), op);
  LOG_ASSERT(false); 
}

template<> inline float AtomicOperation<float, PLUS>(float* a, float& b) {
	float expected, result;
	do {
		expected = *a;
		result = expected + b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline double AtomicOperation<double, PLUS>(double* a, double& b) {
	volatile double expected, result;
	do {
		expected = *a;
		result = expected + b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int64_t AtomicOperation<int64_t, PLUS>(int64_t* a, int64_t& b) {
	/*int64_t expected, result;
	do {
		expected = *a;
		result = expected + b;
	} while (!CAS(a, expected, result));*/
  int64_t expected;
  if (b == 0) return 0;
  else expected = std::atomic_fetch_add((std::atomic<int64_t>*) a, b);
  return expected;
}
template<> inline int32_t AtomicOperation<int32_t, PLUS>(int32_t* a, int32_t& b) {
	/*int32_t expected, result;
	do {
		expected = *a;
		result = expected + b;
	} while (!CAS(a, expected, result));*/
  int32_t expected;
  if (b == 0) return 0;
  else expected = std::atomic_fetch_add((std::atomic<int32_t>*) a, b);
  return expected;
}
template<> inline float AtomicOperation<float, MINUS>(float* a, float& b) {
	float expected, result;
	do {
		expected = *a;
		result = expected - b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline double AtomicOperation<double, MINUS>(double* a, double& b) {
	double expected, result;
	do {
		expected = *a;
		result = expected - b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int64_t AtomicOperation<int64_t, MINUS>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	do {
		expected = *a;
		result = expected - b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int32_t AtomicOperation<int32_t, MINUS>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	do {
		expected = *a;
		result = expected - b;
	} while (!CAS(a, expected, result));
    return expected;
}

template<> inline float AtomicOperation<float, MULTIPLY>(float* a, float& b) {
	float expected, result;
	do {
		expected = *a;
		result = expected * b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline double AtomicOperation<double, MULTIPLY>(double* a, double& b) {
	double expected, result;
	do {
		expected = *a;
		result = expected * b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int64_t AtomicOperation<int64_t, MULTIPLY>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	do {
		expected = *a;
		result = expected * b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int32_t AtomicOperation<int32_t, MULTIPLY>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	do {
		expected = *a;
		result = expected * b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline float AtomicOperation<float, DIVIDE>(float* a, float& b) {
	float expected, result;
	do {
		expected = *a;
		result = expected / b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline double AtomicOperation<double, DIVIDE>(double* a, double& b) {
	double expected, result;
	do {
		expected = *a;
		result = expected / b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int64_t AtomicOperation<int64_t, DIVIDE>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	do {
		expected = *a;
		result = expected / b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int32_t AtomicOperation<int32_t, DIVIDE>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	do {
		expected = *a;
		result = expected / b;
	} while (!CAS(a, expected, result));
    return expected;
}

template<> inline float AtomicOperation<float, MAX>(float* a, float& b) {
	float expected, result;
	do {
		expected = *a;
		result = (expected > b) ? expected : b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline double AtomicOperation<double, MAX>(double* a, double& b) {
	double expected, result;
	do {
		expected = *a;
		result = (expected > b) ? expected : b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int64_t AtomicOperation<int64_t, MAX>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	do {
		expected = *a;
		result = (expected > b) ? expected : b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int32_t AtomicOperation<int32_t, MAX>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	do {
		expected = *a;
		result = (expected > b) ? expected : b;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline float AtomicOperation<float, MIN>(float* a, float& b) {
	float expected, result;
	do {
		expected = *a;
		result = (expected > b) ? b : expected;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline double AtomicOperation<double, MIN>(double* a, double& b) {
	double expected, result;
	do {
		expected = *a;
		result = (expected > b) ? b : expected;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int64_t AtomicOperation<int64_t, MIN>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	do {
		expected = *a;
		result = (expected > b) ? b : expected;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline int32_t AtomicOperation<int32_t, MIN>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	do {
		expected = *a;
		result = (expected > b) ? b : expected;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline bool AtomicOperation<bool, LOR>(bool* a, bool& b) {
	bool expected, result;
	do {
		expected = *a;
		result = b || expected;
	} while (!CAS(a, expected, result));
    return expected;
}
template<> inline LatentVector<int32_t, LATENT_FACTOR_K> AtomicOperation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int32_t, LATENT_FACTOR_K>* a, LatentVector<int32_t, LATENT_FACTOR_K>& b) {
    LatentVector<int32_t, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		AtomicOperation<int32_t, PLUS>(a->k + i, b[i]);
		//AtomicOperation<int32_t, PLUS>(&(a->k[i]), b[i]);
		//AtomicOperation<int32_t, PLUS>(&(a->operator[](i)), b[i]);
		//expected[i] = AtomicOperation<int32_t, PLUS>(&(a->operator[](i)), b[i]);
		//expected[i] = AtomicOperation<int32_t, PLUS>(&(*a)[i], b[i]);
	}
    return expected;
}
template<> inline LatentVector<int64_t, LATENT_FACTOR_K> AtomicOperation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int64_t, LATENT_FACTOR_K>* a, LatentVector<int64_t, LATENT_FACTOR_K>& b) {
    LatentVector<int64_t, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		AtomicOperation<int64_t, PLUS>(a->k + i, b[i]);
		//AtomicOperation<int64_t, PLUS>(&(a->k[i]), b[i]);
		//AtomicOperation<int64_t, PLUS>(&(a->operator[](i)), b[i]);
		//expected[i] = AtomicOperation<int64_t, PLUS>(&(a->operator[](i)), b[i]);
		//expected[i] = AtomicOperation<int64_t, PLUS>(&(*a)[i], b[i]);
	}
    return expected;
}
template<> inline LatentVector<float, LATENT_FACTOR_K> AtomicOperation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
    LatentVector<float, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		expected[i] = AtomicOperation<float, PLUS>(&(*a)[i], b[i]);
	}
	//expected.bias = AtomicOperation<float, PLUS>(&(*a).bias, b.bias);
    return expected;
}

template<> inline LatentVector<double, LATENT_FACTOR_K> AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
    LatentVector<double, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		expected[i] = AtomicOperation<double, PLUS>(&(*a)[i], b[i]);
	}
	//expected.bias = AtomicOperation<double, PLUS>(&(*a).bias, b.bias);
    return expected;
}

template<> inline LatentVector<float, LATENT_FACTOR_K> AtomicOperation<LatentVector<float, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
    LatentVector<float, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		expected[i] = AtomicOperation<float, MULTIPLY>(&(*a)[i], b[i]);
	}
	//expected.bias = AtomicOperation<float, MULTIPLY>(&(*a).bias, b.bias);
    return expected;
}

template<> inline LatentVector<double, LATENT_FACTOR_K> AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
    LatentVector<double, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		expected[i] = AtomicOperation<double, MULTIPLY>(&(*a)[i], b[i]);
	}
	//expected.bias = AtomicOperation<double, MULTIPLY>(&(*a).bias, b.bias);
    return expected;
}
template<> inline LatentVector<float, LATENT_FACTOR_K> AtomicOperation<LatentVector<float, LATENT_FACTOR_K>, DIVIDE>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
    LatentVector<float, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		expected[i] = AtomicOperation<float, DIVIDE>(&(*a)[i], b[i]);
	}
	//expected.bias = AtomicOperation<float, DIVIDE>(&(*a).bias, b.bias);
    return expected;
}

template<> inline LatentVector<double, LATENT_FACTOR_K> AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, DIVIDE>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
    LatentVector<double, LATENT_FACTOR_K> expected;
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		expected[i] = AtomicOperation<double, DIVIDE>(&(*a)[i], b[i]);
	}
	//expected.bias = AtomicOperation<double, DIVIDE>(&(*a).bias, b.bias);
    return expected;
}
template<> inline LatentVector<double, LATENT_FACTOR_K> AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, UNDEFINED>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline int32_t AtomicOperation<int32_t, UNDEFINED>(int32_t* a, int32_t& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline int64_t AtomicOperation<int64_t, UNDEFINED>(int64_t* a, int64_t& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline double AtomicOperation<double, UNDEFINED>(double* a, double& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline float AtomicOperation<float, UNDEFINED>(float* a, float& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline bool AtomicOperation<bool, UNDEFINED>(bool* a, bool& b) {
	fprintf(stdout, "Operation is not defined\n");
}


template<> inline void Operation<float, PLUS>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = expected + b;
	*a = result;
}
template<> inline void Operation<double, PLUS>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = expected + b;
	*a = result;
}
template<> inline void Operation<int64_t, PLUS>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = expected + b;
	*a = result;
}
template<> inline void Operation<int32_t, PLUS>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = expected + b;
	*a = result;
}
template<> inline void Operation<float, MINUS>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void Operation<double, MINUS>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void Operation<int64_t, MINUS>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void Operation<int32_t, MINUS>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}

template<> inline void Operation<float, MULTIPLY>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = expected * b;
	*a = result;
}
template<> inline void Operation<double, MULTIPLY>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = expected * b;
	*a = result;
}
template<> inline void Operation<int64_t, MULTIPLY>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = expected * b;
	*a = result;
}
template<> inline void Operation<int32_t, MULTIPLY>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = expected * b;
	*a = result;
}
template<> inline void Operation<float, DIVIDE>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = expected / b;
	*a = result;
}
template<> inline void Operation<double, DIVIDE>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = expected / b;
	*a = result;
}
template<> inline void Operation<int64_t, DIVIDE>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = expected / b;
	*a = result;
}
template<> inline void Operation<int32_t, DIVIDE>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = expected / b;
	*a = result;
}

template<> inline void Operation<float, MAX>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = (expected > b) ? expected : b;
	*a = result;
}
template<> inline void Operation<double, MAX>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = (expected > b) ? expected : b;
	*a = result;
}
template<> inline void Operation<int64_t, MAX>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = (expected > b) ? expected : b;
	*a = result;
}
template<> inline void Operation<int32_t, MAX>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = (expected > b) ? expected : b;
	*a = result;
}
template<> inline void Operation<float, MIN>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = (expected > b) ? b : expected;
	*a = result;
}
template<> inline void Operation<double, MIN>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = (expected > b) ? b : expected;
	*a = result;
}
template<> inline void Operation<int64_t, MIN>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = (expected > b) ? b : expected;
	*a = result;
}
template<> inline void Operation<int32_t, MIN>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = (expected > b) ? b : expected;
	*a = result;
}
template<> inline void Operation<int32_t, LOR>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = expected || b;
	*a = result;
}
template<> inline void Operation<bool, LOR>(bool* a, bool& b) {
	bool expected, result;
	expected = *a;
	result = expected || b;
	*a = result;
}
template<> inline void Operation<char, BOR>(char* a, char& b) {
	char expected, result;
	expected = *a;
	result = expected | b;
	*a = result;
}
template<> inline void Operation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int32_t, LATENT_FACTOR_K>* a, LatentVector<int32_t, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<int32_t, PLUS>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int64_t, LATENT_FACTOR_K>* a, LatentVector<int64_t, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<int64_t, PLUS>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<float, PLUS>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<double, PLUS>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<float, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<float, MULTIPLY>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<double, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<double, MULTIPLY>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<float, LATENT_FACTOR_K>, DIVIDE>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<float, DIVIDE>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<double, LATENT_FACTOR_K>, DIVIDE>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<double, DIVIDE>(&(*a)[i], b[i]);
	}
}
template<> inline void Operation<LatentVector<double, LATENT_FACTOR_K>, UNDEFINED>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline void Operation<int32_t, UNDEFINED>(int32_t* a, int32_t& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline void Operation<int64_t, UNDEFINED>(int64_t* a, int64_t& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline void Operation<double, UNDEFINED>(double* a, double& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline void Operation<float, UNDEFINED>(float* a, float& b) {
	fprintf(stdout, "Operation is not defined\n");
}
template<> inline void Operation<bool, UNDEFINED>(bool* a, bool& b) {
	fprintf(stdout, "Operation is not defined\n");
}

template<> inline void InverseOperation<int64_t, PLUS>(int64_t* a, int64_t& b) {
	int64_t expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void InverseOperation<int32_t, PLUS>(int32_t* a, int32_t& b) {
	int32_t expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void InverseOperation<double, PLUS>(double* a, double& b) {
	double expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void InverseOperation<float, PLUS>(float* a, float& b) {
	float expected, result;
	expected = *a;
	result = expected - b;
	*a = result;
}
template<> inline void InverseOperation<int64_t, UNDEFINED>(int64_t* a, int64_t& b) {
    INVARIANT(false);
}
template<> inline void InverseOperation<double, UNDEFINED>(double* a, double& b) {
    INVARIANT(false);
}
template<> inline void InverseOperation<bool, UNDEFINED>(bool* a, bool& b) {
    INVARIANT(false);
}
template<> inline void InverseOperation<LatentVector<double, LATENT_FACTOR_K>, UNDEFINED>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
    INVARIANT(false);
}
template<> inline void InverseOperation<int64_t, MIN>(int64_t* a, int64_t& b) {
    INVARIANT(false);
}
template<> inline void InverseOperation<bool, LOR>(bool* a, bool& b) {
    INVARIANT(false);
}
template<> inline void InverseOperation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<float, MINUS>(&(*a)[i], b[i]);
	}
}
template<> inline void InverseOperation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<double, MINUS>(&(*a)[i], b[i]);
	}
}
template<> inline void InverseOperation<LatentVector<float, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<float, LATENT_FACTOR_K>* a, LatentVector<float, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<float, DIVIDE>(&(*a)[i], b[i]);
	}
}
template<> inline void InverseOperation<LatentVector<double, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<double, LATENT_FACTOR_K>* a, LatentVector<double, LATENT_FACTOR_K>& b) {
	for (auto i = 0; i < LATENT_FACTOR_K; i++) {
		Operation<double, DIVIDE>(&(*a)[i], b[i]);
	}
}
template<> inline void UpdateAggregationWithCnt<int32_t, MIN>(int32_t* a, int32_t& b) {
    int32_t a_cnt;
    int32_t a_val;
    int32_t b_cnt = GET_CNT_32BIT(b);
    int32_t b_val = GET_VAL_32BIT(b);

    int32_t expected, result;
    a_val = GET_VAL_32BIT(*a);
    a_cnt = GET_CNT_32BIT(*a);
    if (a_val < b_val) {
        return;
    } else if (a_val == b_val) {
        if (a_cnt == MIN_CNT_32BIT) {
            return;
        } else if (b_cnt == MIN_CNT_32BIT) {
            *a = b;
            return;
        } else {
            *a = b_val;
            if ((a_cnt + b_cnt) > MAX_CNT_32BIT) {
                MARK_CNT_32BIT(*a, MAX_CNT_32BIT);
            } else if ((a_cnt + b_cnt) < MIN_CNT_32BIT) {
                MARK_CNT_32BIT(*a, MIN_CNT_32BIT);
            } else {
                MARK_CNT_32BIT(*a, (a_cnt + b_cnt));
            }
        }
    } else { // a_val > b
        *a = b;
    }
}
template<> inline void UpdateAggregationWithCnt<int64_t, MIN>(int64_t* a, int64_t& b) {
    int64_t a_cnt;
    int64_t a_val;
    int64_t b_cnt = GET_CNT_64BIT(b);
    int64_t b_val = GET_VAL_64BIT(b);

    int64_t expected, result;
    a_val = GET_VAL_64BIT(*a);
    a_cnt = GET_CNT_64BIT(*a);
    if (a_val < b_val) {
        return;
    } else if (a_val == b_val) {
        if (a_cnt != MIN_CNT_32BIT && b_cnt != MIN_CNT_64BIT) {
            *a = b_val;
            if ((a_cnt + b_cnt) > MAX_CNT_64BIT) {
                MARK_CNT_64BIT(*a, MAX_CNT_64BIT);
            } else if ((a_cnt + b_cnt) < MIN_CNT_64BIT) {
                MARK_CNT_64BIT(*a, MIN_CNT_64BIT);
            } else {
                MARK_CNT_64BIT(*a, (a_cnt + b_cnt));
            }
        } else if (a_cnt == MIN_CNT_64BIT) {
            return;
        } else if (b_cnt == MIN_CNT_64BIT) {
            *a = b;
            return;
        }
    } else { // a_val > b
        *a = b;
    }
}
template<> inline void UpdateAggregationWithCntStatic<int64_t, MIN>(int64_t* a, int64_t& b) {
    int64_t a_cnt;
    int64_t a_val;
    int64_t b_cnt = 1;
    int64_t b_val = b;
    MARK_CNT_64BIT(b, 1);

    int64_t expected, result;
    a_val = GET_VAL_64BIT(*a);
    a_cnt = GET_CNT_64BIT(*a);
    if (a_val < b_val) {
        return;
    } else if (a_val == b_val) {
        if (a_cnt != MIN_CNT_32BIT && b_cnt != MIN_CNT_64BIT) {
            *a = b_val;
            if ((a_cnt + b_cnt) > MAX_CNT_64BIT) {
                MARK_CNT_64BIT(*a, MAX_CNT_64BIT);
            } else if ((a_cnt + b_cnt) < MIN_CNT_64BIT) {
                MARK_CNT_64BIT(*a, MIN_CNT_64BIT);
            } else {
                MARK_CNT_64BIT(*a, (a_cnt + b_cnt));
            }
        } else if (a_cnt == MIN_CNT_64BIT) {
            return;
        } else if (b_cnt == MIN_CNT_64BIT) {
            *a = b;
            return;
        }
    } else { // a_val > b
        *a = b;
    }
}
template<> inline void UpdateAggregationWithCnt<LatentVector<int32_t, LATENT_FACTOR_K>, MIN>(LatentVector<int32_t, LATENT_FACTOR_K>* a, LatentVector<int32_t, LATENT_FACTOR_K>& b) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			UpdateAggregationWithCnt<int32_t, MIN>(&(*a)[i], b[i]);
		}
}
template<> inline void UpdateAggregationWithCnt<LatentVector<int64_t, LATENT_FACTOR_K>, MIN>(LatentVector<int64_t, LATENT_FACTOR_K>* a, LatentVector<int64_t, LATENT_FACTOR_K>& b) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			UpdateAggregationWithCnt<int64_t, MIN>(&(*a)[i], b[i]);
		}
}
template<> inline void UpdateAggregation<bool, LOR>(bool* agg, bool& o, bool& n) {
    Operation<bool, LOR>(agg, n); // XXX hmm..
}
template<> inline void UpdateAggregation<int32_t, PLUS>(int32_t* agg, int32_t& o, int32_t& n) {
    int32_t d = n - o;
    Operation<int32_t, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<int64_t, PLUS>(int64_t* agg, int64_t& o, int64_t& n) {
    int64_t d = n - o;
    Operation<int64_t, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<int32_t, MIN>(int32_t* agg, int32_t& o, int32_t& n) {
    int32_t b;
    if (n < o) {
        MARK_CNT_OUTPUT_32BIT(b, n, 1);
    } else if (n == o) {
        b = n;
    } else { // n > o
        MARK_CNT_OUTPUT_32BIT(b, o, -1);
    }
    UpdateAggregationWithCnt<int32_t, MIN>(agg, b);
    //*agg = b;
}
template<> inline void UpdateAggregation<int64_t, MIN>(int64_t* agg, int64_t& o, int64_t& n) {
    int64_t b;
    if (n < o) {
        MARK_CNT_OUTPUT_64BIT(b, n, 1);
    } else if (n == o) {
        b = n;
    } else { // n > o
        MARK_CNT_OUTPUT_64BIT(b, o, -1);
    }
    UpdateAggregationWithCnt<int64_t, MIN>(agg, b);
    //*agg = b;
}
template<> inline void UpdateAggregation<float, PLUS>(float* agg, float& o, float& n) {
    float d = n - o;
    Operation<float, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<double, PLUS>(double* agg, double& o, double& n) {
    double d = n - o;
    Operation<double, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int32_t, LATENT_FACTOR_K>* agg, LatentVector<int32_t, LATENT_FACTOR_K>& o, LatentVector<int32_t, LATENT_FACTOR_K>& n) {
    LatentVector<int32_t, LATENT_FACTOR_K> d = n - o;
    Operation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int64_t, LATENT_FACTOR_K>* agg, LatentVector<int64_t, LATENT_FACTOR_K>& o, LatentVector<int64_t, LATENT_FACTOR_K>& n) {
    LatentVector<int64_t, LATENT_FACTOR_K> d = n - o;
    Operation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(LatentVector<double, LATENT_FACTOR_K>* agg, LatentVector<double, LATENT_FACTOR_K>& o, LatentVector<double, LATENT_FACTOR_K>& n) {
    LatentVector<double, LATENT_FACTOR_K> d = n - o;
    Operation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(LatentVector<float, LATENT_FACTOR_K>* agg, LatentVector<float, LATENT_FACTOR_K>& o, LatentVector<float, LATENT_FACTOR_K>& n) {
    LatentVector<float, LATENT_FACTOR_K> d = n - o;
    Operation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void UpdateAggregation<LatentVector<int32_t, LATENT_FACTOR_K>, MIN>(LatentVector<int32_t, LATENT_FACTOR_K>* agg, LatentVector<int32_t, LATENT_FACTOR_K>& o, LatentVector<int32_t, LATENT_FACTOR_K>& n) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			UpdateAggregation<int32_t, MIN>(&(*agg)[i], o[i], n[i]);
		}
}
template<> inline void UpdateAggregation<LatentVector<int64_t, LATENT_FACTOR_K>, MIN>(LatentVector<int64_t, LATENT_FACTOR_K>* agg, LatentVector<int64_t, LATENT_FACTOR_K>& o, LatentVector<int64_t, LATENT_FACTOR_K>& n) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			UpdateAggregation<int64_t, MIN>(&(*agg)[i], o[i], n[i]);
		}
}
template<> inline void AtomicUpdateAggregationWithCnt<int32_t, MIN>(int32_t* a, int32_t& b) {
    int32_t a_cnt;
    int32_t a_val;
    int32_t b_cnt = GET_CNT_32BIT(b);
    int32_t b_val = GET_VAL_32BIT(b);

    int32_t expected, result;
    do {
        expected = *a;
        a_val = GET_VAL_32BIT(*a);
        a_cnt = GET_CNT_32BIT(*a);
        if (a_val < b_val) {
            result = expected;
        } else if (a_val == b_val) {
            if (a_cnt == MIN_CNT_32BIT) {
                result = *a;
            } else if (b_cnt == MIN_CNT_32BIT) {
                result = b;
            } else {
                result = b_val;
                if ((a_cnt + b_cnt) > MAX_CNT_32BIT) {
                    MARK_CNT_32BIT(result, MAX_CNT_32BIT);
                } else if ((a_cnt + b_cnt) < MIN_CNT_32BIT) {
                    MARK_CNT_32BIT(result, MIN_CNT_32BIT);
                } else {
                    MARK_CNT_32BIT(result, (a_cnt + b_cnt));
                }
            }
        } else { // a_val > b
            result = b;
        }
    } while (!CAS(a, expected, result));
}
template<> inline void AtomicUpdateAggregationWithCnt<int64_t, MIN>(int64_t* a, int64_t& b) {
    int64_t a_cnt;
    int64_t a_val;
    int64_t b_cnt = GET_CNT_64BIT(b);
    int64_t b_val = GET_VAL_64BIT(b);

    int64_t expected, result;
    a_val = GET_VAL_64BIT(*a);
    if (a_val < b_val) {
        return;
    } else {
        do {
            expected = *a;
            a_val = GET_VAL_64BIT(expected);
            a_cnt = GET_CNT_64BIT(expected);
            if (a_val < b_val) {
                return;
            } else if (a_val == b_val) {
                if (a_cnt == MIN_CNT_64BIT) {
                    result = expected;
                } else if (b_cnt == MIN_CNT_64BIT) {
                    result = b;
                } else {
                    result = b_val;
                    if ((a_cnt + b_cnt) > MAX_CNT_64BIT) {
                        MARK_CNT_64BIT(result, MAX_CNT_64BIT);
                    } else if ((a_cnt + b_cnt) < MIN_CNT_64BIT) {
                        MARK_CNT_64BIT(result, MIN_CNT_64BIT);
                    } else {
                        MARK_CNT_64BIT(result, (a_cnt + b_cnt));
                    }
                }
            } else { // a_val > b
                result = b;
            }
        } while (!CAS(a, expected, result));
    }
}
template<> inline void AtomicUpdateAggregationWithCntStatic<int64_t, MIN>(int64_t* a, int64_t& b) {
    int64_t a_cnt;
    int64_t a_val;
    int64_t b_cnt = 1;
    int64_t b_val = b;
    MARK_CNT_64BIT(b, 1);

    int64_t expected, result;
    a_val = GET_VAL_64BIT(*a);
    if (a_val < b_val) {
        return;
    } else {
        do {
            expected = *a;
            a_val = GET_VAL_64BIT(expected);
            a_cnt = GET_CNT_64BIT(expected);
            if (a_val < b_val) {
                return;
            } else if (a_val == b_val) {
                if (a_cnt == MIN_CNT_64BIT) {
                    result = expected;
                } else if (b_cnt == MIN_CNT_64BIT) {
                    result = b;
                } else {
                    result = b_val;
                    if ((a_cnt + b_cnt) > MAX_CNT_64BIT) {
                        MARK_CNT_64BIT(result, MAX_CNT_64BIT);
                    } else if ((a_cnt + b_cnt) < MIN_CNT_64BIT) {
                        MARK_CNT_64BIT(result, MIN_CNT_64BIT);
                    } else {
                        MARK_CNT_64BIT(result, (a_cnt + b_cnt));
                    }
                }
            } else { // a_val > b
                result = b;
            }
        } while (!CAS(a, expected, result));
    }
}
template<> inline void AtomicUpdateAggregationWithCnt<LatentVector<int32_t, LATENT_FACTOR_K>, MIN>(LatentVector<int32_t, LATENT_FACTOR_K>* a, LatentVector<int32_t, LATENT_FACTOR_K>& b) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			AtomicUpdateAggregationWithCnt<int32_t, MIN>(&(*a)[i], b[i]);
		}
}
template<> inline void AtomicUpdateAggregationWithCnt<LatentVector<int64_t, LATENT_FACTOR_K>, MIN>(LatentVector<int64_t, LATENT_FACTOR_K>* a, LatentVector<int64_t, LATENT_FACTOR_K>& b) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			AtomicUpdateAggregationWithCnt<int64_t, MIN>(&(*a)[i], b[i]);
		}
}

template<> inline void AtomicUpdateAggregation<bool, LOR>(bool* agg, bool& o, bool& n) {
    LOG_ASSERT(false);
}
template<> inline void AtomicUpdateAggregation<int64_t, UNDEFINED>(int64_t* agg, int64_t& o, int64_t& n) {
    LOG_ASSERT(false);
}
template<> inline void AtomicUpdateAggregation<bool, UNDEFINED>(bool* agg, bool& o, bool& n) {
    LOG_ASSERT(false);
}
template<> inline void AtomicUpdateAggregation<int64_t, MIN>(int64_t* agg, int64_t& o, int64_t& n) {
    int64_t b;
    if (n < o) {
        MARK_CNT_OUTPUT_64BIT(b, n, 1);
    } else if (n == o) {
        b = n;
    } else { // n > o
        MARK_CNT_OUTPUT_64BIT(b, o, -1);
    }
		//if (UserArguments::UPDATE_VERSION >= 1)
		//	fprintf(stdout, "[%ld] [AtomicUpdateAggregation] b: (%ld, cnt %d) old_msg %ld, new_msg %ld\n", PartitionStatistics::my_machine_id(), (int64_t) GET_VAL_64BIT(b), (int) GET_CNT_64BIT(b), o, n);
    AtomicUpdateAggregationWithCnt<int64_t, MIN>(agg, b);
}
template<> inline void AtomicUpdateAggregation<int32_t, MIN>(int32_t* agg, int32_t& o, int32_t& n) {
    int32_t b;
    if (n < o) {
        MARK_CNT_OUTPUT_32BIT(b, n, 1);
    } else if (n == o) {
        b = n;
    } else { // n > o
        MARK_CNT_OUTPUT_32BIT(b, o, -1);
    }
    AtomicUpdateAggregationWithCnt<int32_t, MIN>(agg, b);
}
template<> inline void AtomicUpdateAggregation<double, UNDEFINED>(double* agg, double& o, double& n) {
	fprintf(stdout, "Operation is not defined\n");
    LOG_ASSERT(false);
}
template<> inline void AtomicUpdateAggregation<int64_t, PLUS>(int64_t* agg, int64_t& o, int64_t& n) {
    int64_t d = n - o;
    AtomicOperation<int64_t, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<int32_t, PLUS>(int32_t* agg, int32_t& o, int32_t& n) {
    int32_t d = n - o;
    AtomicOperation<int32_t, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<float, PLUS>(float* agg, float& o, float& n) {
    float d = n - o;
    AtomicOperation<float, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<double, PLUS>(double* agg, double& o, double& n) {
    double d = n - o;
    AtomicOperation<double, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<float, LATENT_FACTOR_K>, MINUS>(LatentVector<float, LATENT_FACTOR_K>* agg, LatentVector<float, LATENT_FACTOR_K>& o, LatentVector<float, LATENT_FACTOR_K>& n) {
    LatentVector<float, LATENT_FACTOR_K> d = o - n;	// not (n - o)
    AtomicOperation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(LatentVector<float, LATENT_FACTOR_K>* agg, LatentVector<float, LATENT_FACTOR_K>& o, LatentVector<float, LATENT_FACTOR_K>& n) {
    LatentVector<float, LATENT_FACTOR_K> d = n - o;
    AtomicOperation<LatentVector<float, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<float, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<float, LATENT_FACTOR_K>* agg, LatentVector<float, LATENT_FACTOR_K>& o, LatentVector<float, LATENT_FACTOR_K>& n) {
    LatentVector<float, LATENT_FACTOR_K> d = n / o;
    AtomicOperation<LatentVector<float, LATENT_FACTOR_K>, MULTIPLY>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<double, LATENT_FACTOR_K>, UNDEFINED>(LatentVector<double, LATENT_FACTOR_K>* agg, LatentVector<double, LATENT_FACTOR_K>& o, LatentVector<double, LATENT_FACTOR_K>& n) {
	fprintf(stdout, "Operation is not defined\n");
    LOG_ASSERT(false);
}
template<> inline void AtomicUpdateAggregation<LatentVector<double, LATENT_FACTOR_K>, MINUS>(LatentVector<double, LATENT_FACTOR_K>* agg, LatentVector<double, LATENT_FACTOR_K>& o, LatentVector<double, LATENT_FACTOR_K>& n) {
    LatentVector<double, LATENT_FACTOR_K> d = o - n;	// not (n - o)
    AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(LatentVector<double, LATENT_FACTOR_K>* agg, LatentVector<double, LATENT_FACTOR_K>& o, LatentVector<double, LATENT_FACTOR_K>& n) {
    LatentVector<double, LATENT_FACTOR_K> d = n - o;
    AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<double, LATENT_FACTOR_K>, MULTIPLY>(LatentVector<double, LATENT_FACTOR_K>* agg, LatentVector<double, LATENT_FACTOR_K>& o, LatentVector<double, LATENT_FACTOR_K>& n) {
    LatentVector<double, LATENT_FACTOR_K> d = n / o;
    AtomicOperation<LatentVector<double, LATENT_FACTOR_K>, MULTIPLY>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int32_t, LATENT_FACTOR_K>* agg, LatentVector<int32_t, LATENT_FACTOR_K>& o, LatentVector<int32_t, LATENT_FACTOR_K>& n) {
    LatentVector<int32_t, LATENT_FACTOR_K> d = n - o;
    AtomicOperation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(LatentVector<int64_t, LATENT_FACTOR_K>* agg, LatentVector<int64_t, LATENT_FACTOR_K>& o, LatentVector<int64_t, LATENT_FACTOR_K>& n) {
    LatentVector<int64_t, LATENT_FACTOR_K> d = n - o;
    AtomicOperation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(agg, d);
}
template<> inline void AtomicUpdateAggregation<LatentVector<int32_t, LATENT_FACTOR_K>, MIN>(LatentVector<int32_t, LATENT_FACTOR_K>* agg, LatentVector<int32_t, LATENT_FACTOR_K>& o, LatentVector<int32_t, LATENT_FACTOR_K>& n) {
		for (auto i = 0; i < LATENT_FACTOR_K; i++) {
			AtomicUpdateAggregation<int32_t, MIN>(&(*agg)[i], o[i], n[i]);
		}
}

template<> inline void AccumulateNewAndOldMessagesIntoTGB<bool, LOR> (bool* target, bool& new_msg, bool& old_msg) {
  Operation<bool, LOR>(target, new_msg);
}
template<> inline void AccumulateNewAndOldMessagesIntoTGB<int32_t, PLUS> (int32_t* target, int32_t& new_msg, int32_t& old_msg) {
  if (new_msg != old_msg)
    UpdateAggregation<int32_t, PLUS>(target, old_msg, new_msg);
}
template<> inline void AccumulateNewAndOldMessagesIntoTGB<int64_t, PLUS> (int64_t* target, int64_t& new_msg, int64_t& old_msg) {
  if (new_msg != old_msg)
    UpdateAggregation<int64_t, PLUS>(target, old_msg, new_msg);
}
template<> inline void AccumulateNewAndOldMessagesIntoTGB<int64_t, MIN> (int64_t* target, int64_t& new_msg, int64_t& old_msg) {
  UpdateAggregation<int64_t, MIN>(target, old_msg, new_msg);
}
template<> inline void AccumulateNewAndOldMessagesIntoTGB<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS> (LatentVector<int32_t, LATENT_FACTOR_K>* target, LatentVector<int32_t, LATENT_FACTOR_K>& new_msg, LatentVector<int32_t, LATENT_FACTOR_K>& old_msg) {
  if (!checkEquality(new_msg, old_msg))
    UpdateAggregation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(target, old_msg, new_msg);
}
template<> inline void AccumulateNewAndOldMessagesIntoTGB<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS> (LatentVector<int64_t, LATENT_FACTOR_K>* target, LatentVector<int64_t, LATENT_FACTOR_K>& new_msg, LatentVector<int64_t, LATENT_FACTOR_K>& old_msg) {
  if (!checkEquality(new_msg, old_msg))
    UpdateAggregation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(target, old_msg, new_msg);
}

template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<bool, LOR> (bool* target, bool& new_msg, bool& old_msg) {
  AtomicOperation<bool, LOR>(target, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<int32_t, PLUS> (int32_t* target, int32_t& new_msg, int32_t& old_msg) {
  if (new_msg != old_msg)
    AtomicUpdateAggregation<int32_t, PLUS>(target, old_msg, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<int64_t, PLUS> (int64_t* target, int64_t& new_msg, int64_t& old_msg) {
  if (new_msg != old_msg)
    AtomicUpdateAggregation<int64_t, PLUS>(target, old_msg, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<int64_t, MIN> (int64_t* target, int64_t& new_msg, int64_t& old_msg) {
  if (old_msg == idenelem<int64_t>(MIN)) {
    AtomicUpdateAggregationWithCntStatic<int64_t, MIN>(target, new_msg);
  } else {
    AtomicUpdateAggregation<int64_t, MIN>(target, old_msg, new_msg);
  }
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS> (LatentVector<int32_t, LATENT_FACTOR_K>* target, LatentVector<int32_t, LATENT_FACTOR_K>& new_msg, LatentVector<int32_t, LATENT_FACTOR_K>& old_msg) {
  if (!checkEquality(new_msg, old_msg))
    AtomicUpdateAggregation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(target, old_msg, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS> (LatentVector<int64_t, LATENT_FACTOR_K>* target, LatentVector<int64_t, LATENT_FACTOR_K>& new_msg, LatentVector<int64_t, LATENT_FACTOR_K>& old_msg) {
  if (!checkEquality(new_msg, old_msg))
    AtomicUpdateAggregation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(target, old_msg, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<bool, LOR> (bool* target, bool& new_msg) {
  AtomicOperation<bool, LOR>(target, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<int32_t, PLUS> (int32_t* target, int32_t& new_msg) {
  AtomicOperation<int32_t, PLUS>(target, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<int64_t, PLUS> (int64_t* target, int64_t& new_msg) {
  AtomicOperation<int64_t, PLUS>(target, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<int64_t, MIN> (int64_t* target, int64_t& new_msg) {
  AtomicUpdateAggregationWithCnt<int64_t, MIN>(target, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS> (LatentVector<int32_t, LATENT_FACTOR_K>* target, LatentVector<int32_t, LATENT_FACTOR_K>& new_msg) {
  AtomicOperation<LatentVector<int32_t, LATENT_FACTOR_K>, PLUS>(target, new_msg);
}
template<> inline void AtomicAccumulateNewAndOldMessagesIntoTGB<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS> (LatentVector<int64_t, LATENT_FACTOR_K>* target, LatentVector<int64_t, LATENT_FACTOR_K>& new_msg) {
  AtomicOperation<LatentVector<int64_t, LATENT_FACTOR_K>, PLUS>(target, new_msg);
}


enum RequestType {
	AdjListBatchIo = 3,
	AdjListBatchIoData = 4,
	UserCallback = 5,
	UserCallbackData = 6,
	OutputVectorRead = 7,
	InputVectorRead = 8,
	OutputVectorWriteMessage = 10,
	NewAdjListBatchIo = 11,
	NewAdjListBatchIoData = 12,
	SequentialVectorRead = 15,
	Exit = 99999999,
};

class Request {
  public:
	Request() : lv (-1) {}

	RequestType rt;
	int from;
	int to;
	int lv;
    int dummmmm;
};

class ExitRequest: public Request {
  public:
	int64_t dummy1;
};

class SequentialVectorReadRequest: public Request {
  public:
	int64_t vectorID;
	int64_t chunkID;
};
class OutputVectorReadRequest: public Request {
  public:
	int64_t vectorID;
	int64_t chunkID;
};
class InputVectorReadRequest: public Request {
  public:
	int64_t vectorID;
	int64_t chunkID;
};
class OutputVectorWriteMessageRequest: public Request {
  public:
	int64_t vectorID;
	int64_t fromChunkID; // (from chunkID) --> (chunkID)
	int64_t chunkID;	//
	int64_t send_num;
	int64_t tid;
	int64_t idx;
	int64_t combined;
};

class AdjListRequestBatch: public Request {
  public:
	Range<int64_t> src_edge_partition_range;
	Range<int64_t> dst_edge_partition_range;
    Range<int> version_range;
    EdgeType e_type;
    DynamicDBType d_type;
	int64_t MaxBytesToPin;
};

class AdjListRequest: public Request {
  public:
	node_t src_vid; // src_vid of the target
	Range<node_t> dst_vid_range;	// dst_vid range of the target
};

class UserCallbackRequest: public Request {
  public:
	UserCallbackRequest() {
		vid_range.Set(-1, -1);
	}

	int64_t parm1;
	int64_t data_size;
	Range<node_t> vid_range;
};


class Respond {
  public:
	RequestType rt;
	int from;
	int to;
};

class AdjListRespond: public Respond {
  public:
	int32_t num_pages;
};

struct UdfSendRequest {
	SimpleContainer cont;
	UserCallbackRequest req;
};

class MaterializedAdjacencyLists;
//use tag number 1 when sending request
class RequestRespond : public TG_NWSMCallback {
  public:
	static RequestRespond rr_;

	ThreadPool<int> async_req_pool;
	ThreadPool<int> async_resp_pool;
	ThreadPool<bool> async_udf_pool;
	per_thread_lazy<diskaio::DiskAioInterface**> per_thread_aio_interface;

	static int64_t total_request_buffer_size;
	static int64_t max_request_size;
	static int64_t num_request_buffer;
	static char* request_buffer;

	static int64_t total_data_buffer_size;
	static int64_t small_data_size;
	static int64_t large_data_size;
	static int64_t num_small_data_buffer;
	static int64_t num_large_data_buffer;
	static char* data_buffer;

	static std::atomic<int64_t> matadj_hit_bytes;
	static std::atomic<int64_t> matadj_miss_bytes;

	static turbo_tcp server_sockets;
	static turbo_tcp client_sockets;
	static turbo_tcp general_server_sockets[DEFAULT_NUM_GENERAL_TCP_CONNECTIONS];
	static turbo_tcp general_client_sockets[DEFAULT_NUM_GENERAL_TCP_CONNECTIONS];
	static std::vector<bool> general_connection_pool;

	static VariableSizedMemoryAllocatorWithCircularBuffer circular_temp_data_buffer;
	static VariableSizedMemoryAllocatorWithCircularBuffer circular_stream_send_buffer;
	static VariableSizedMemoryAllocatorWithCircularBuffer circular_stream_recv_buffer;
	
	static int64_t core_counts_;
    static __thread int64_t my_core_id_;

	RequestRespond() {}
	~RequestRespond() {}

	static void Initialize(int64_t trbs, int64_t mrs, int64_t tdbs, int64_t mds, int64_t nio_threads, int64_t udf_threads) {
        rr_.async_req_pool.SpawnThreadsToCpuSet(nio_threads, udf_threads, NumaHelper::num_cores() - 1);
		rr_.async_resp_pool.SpawnThreadsToCpuSet(nio_threads, udf_threads, NumaHelper::num_cores() - 1);
        rr_.async_udf_pool.SpawnThreads(udf_threads, 0, udf_threads - 1);

		mutex_for_end.lock();
		end = false;
		#pragma omp parallel num_threads(core_id::core_counts())
		{
			per_thread_container_.get(omp_get_thread_num()).capacity = 1024;
			per_thread_container_.get(omp_get_thread_num()).data = new char[1024];
		}
		mutex_for_end.unlock();

		total_request_buffer_size = trbs;
		max_request_size = mrs;
		num_request_buffer = trbs / mrs;
		request_buffer = new char[total_request_buffer_size];
		std::memset ((void*) request_buffer, 0, total_request_buffer_size);

		#pragma omp parallel for
		for (int64_t i = 0; i < num_request_buffer; i++) {
			char* tmp = request_buffer + max_request_size * i;
			req_buffer_queue_.enqueue(tmp);
		}

		total_data_buffer_size = tdbs;
		data_buffer = (char*) NumaHelper::alloc_numa_interleaved_memory(total_data_buffer_size);

		small_data_size = mds;
		large_data_size = 2 * small_data_size;
		num_small_data_buffer = (50 * tdbs) / (100 * small_data_size);
		num_large_data_buffer = (50 * tdbs) / (100 * large_data_size);

#pragma omp parallel for
        for (int64_t i = 0; i < num_small_data_buffer; i++) {
            char* tmp = data_buffer + small_data_size * i;
            std::memset ((void*) tmp, 0, small_data_size);
            SimpleContainer cont;
            cont.data = tmp;
            cont.capacity = small_data_size;
            small_data_buffer_queue_.enqueue(cont);
        }

#pragma omp parallel for
		for (int64_t i = 0; i < num_large_data_buffer; i++) {
			char* tmp = data_buffer + small_data_size * num_small_data_buffer + large_data_size * i;
			std::memset ((void*) tmp, 0, large_data_size);
			SimpleContainer cont;
			cont.data = tmp;
			cont.capacity = large_data_size;
			large_data_buffer_queue_.enqueue(cont);
		}
		max_dynamic_buffer_size = 1 * 1024 * 1024 * 1024L; // by default 1GB
		max_dynamic_recv_buffer_size = max_dynamic_buffer_size / 2;
		max_dynamic_send_buffer_size = max_dynamic_buffer_size / 2;

        InitializePerThreadBufferQueue();

		LocalStatistics::register_mem_alloc_info("RequestResond", (trbs + tdbs + per_thread_buffer_size_) / (1024 * 1024L));

        turbo_tcp::establish_all_connections(&server_sockets, &client_sockets);
        general_connection_pool.resize(DEFAULT_NUM_GENERAL_TCP_CONNECTIONS + 1, true);
        general_connection_pool[0] = false;
        general_connection_pool[1] = false;
        for (int i = 0; i < DEFAULT_NUM_GENERAL_TCP_CONNECTIONS; i++) {
            turbo_tcp::establish_all_connections(&general_server_sockets[i], &general_client_sockets[i]);
        }
    }

	static void Close() {
		INVARIANT (bytes_dynamically_allocated.load() == 0);
		INVARIANT (recv_buffer_bytes_dynamically_allocated.load() == 0);
		INVARIANT (send_buffer_bytes_dynamically_allocated.load() == 0);
		//INVARIANT (small_data_buffer_queue_[0].size_approx() == num_small_data_buffer / 2);
		//INVARIANT (small_data_buffer_queue_[1].size_approx() == num_small_data_buffer / 2);
		INVARIANT (small_data_buffer_queue_.size_approx() == num_small_data_buffer);
		INVARIANT (large_data_buffer_queue_.size_approx() == num_large_data_buffer);

		rr_.async_req_pool.Close();
		rr_.async_resp_pool.Close();
		rr_.async_udf_pool.Close();

		TwoLevelBitMap<node_t>* tmp = nullptr;
        for (int queue_idx = 0; queue_idx < 2; queue_idx++) {
            while (temp_bitmap_queue_[queue_idx].try_dequeue(tmp)) {
                INVARIANT(tmp != nullptr);
                delete tmp;
                tmp = nullptr;
            }
        }
		#pragma omp parallel num_threads(core_id::core_counts())
		{
			delete[] per_thread_container_.get(omp_get_thread_num()).data;
			per_thread_container_.get(omp_get_thread_num()).data = nullptr;
		}

		delete[] request_buffer;
		NumaHelper::free_numa_memory(data_buffer, total_data_buffer_size);
		LocalStatistics::register_mem_alloc_info("RequestResond", 0);

        INVARIANT(circular_temp_data_buffer.GetSize() == circular_temp_data_buffer.GetRemainingBytes());
        INVARIANT(circular_stream_send_buffer.GetSize() == circular_stream_send_buffer.GetRemainingBytes());
        INVARIANT(circular_stream_recv_buffer.GetSize() == circular_stream_recv_buffer.GetRemainingBytes());

		circular_temp_data_buffer.Close(true);
		circular_stream_send_buffer.Close(true);
		circular_stream_recv_buffer.Close(true);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularTempDataBuffer)", 0);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularSendBuffer)", 0);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularRecvBuffer)", 0);


        char* tmp_buf;
        int64_t num_entries_in_per_thread_buffer_queue = per_thread_buffer_size_ / PER_THREAD_BUFF_SIZE;
		while (per_thread_buffer_queue_.try_dequeue(tmp_buf)) {
            --num_entries_in_per_thread_buffer_queue;
        }
        INVARIANT(num_entries_in_per_thread_buffer_queue == 0);
		NumaHelper::free_numa_memory(per_thread_buffer_, per_thread_buffer_size_);

		client_sockets.close_socket();
		server_sockets.close_socket();
        for (int i = 0; i < DEFAULT_NUM_GENERAL_TCP_CONNECTIONS; i++) {
            general_client_sockets[i].close_socket();
            general_server_sockets[i].close_socket();
        }
	}

    static void ReceiveRequest();

	static void InitializeCoreIds();
	static void InitializeIoInterface();
    static void InitializeTempBitMapQueue(int64_t num_entries, int64_t num_buffers_for_sender, int64_t num_buffers_for_receiver);

	template<typename RequestType>
	static void SendRequestAsynchronously(int partition_id, RequestType req_buffer);

	template<typename RequestType>
	static void SendRequest(int partition_id, RequestType req_buffer);

	template<typename RequestType>
	static void SendRequest(int partition_id, RequestType* req, int num_reqs = 1);

	template<typename RequestType, typename ObjectType, typename FcnType>
	static void SendRequest(RequestType* req, int num_reqs, bool wait, ObjectType* obj, FcnType* fcn);

	template<typename Request>
	static void SendRequest(Request* req, int num_reqs, char* data_buffer[], int64_t data_size[], int64_t num_data_buffers);
	
    template<typename Request>
	static void SendRequestViaTcp(Request* req, int num_reqs, char* data_buffer, int64_t data_size);

	template<typename Request>
	static void SendRequestReturnContainer(Request req, char* data_buffer, int64_t data_size, SimpleContainer cont_to_return);

    static void SendSynchronously(int partition_id, char* buf, int64_t sz, int tag);

	static void SendData(char* buf, int64_t count, int partition_id, int tag, SimpleContainer cont, bool return_container);
	static void SendDataInBuffer(tbb::concurrent_queue<SimpleContainer>* req_buffer_queue, turbo_tcp* vector_client_sockets, int partition_id, int tag, int chunk_idx, int lv, int64_t eom_cnt, bool delete_req_buffer_queue = true);
	static void SendDataInBufferWithUdfRequest(tbb::concurrent_queue<UdfSendRequest>* req_buffer_queue, tbb::concurrent_queue<UdfSendRequest>* stoped_req_buffer_queue,int to, Range<node_t> vid_range, BitMap<node_t>* vids, int tag, int chunk_idx, int lv, int64_t MaxBytesToSend, int64_t* stop_flag);
    template <typename T, Op op>
    static void TurboAllReduceSync(char* buf, int count, int size_in_bits);
    template <typename T, Op op>
    static void TurboAllReduceInPlaceAsync(char* buf, int count, int size_in_bytes, int queue_idx=0);
    template <typename T, Op op>
    static void TurboAllReduceInPlace(turbo_tcp* server_socket, turbo_tcp* client_socket, int socket_idx, char* buf, int count, int size_in_bits);

    static void TurboBarrier(turbo_tcp* server_socket, turbo_tcp* client_socket);
    static void Barrier();
    static void WaitAllreduce();
    static void PopAllreduce();

	template <typename RequestType>
	static void Respond(RequestType* req, int num_reqs = 1);

	static void RespondSequentialVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv);
	static void RespondInputVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv);
	static void RespondOutputVectorRead(int32_t vectorID, int64_t chunkID, int from, int lv);
	static void RespondOutputVectorWriteMessage(int32_t vectorID, int64_t fromChunkID, int64_t chunkID, int tid, int idx, int from, int64_t send_num, int64_t combined, int lv);
	static void RespondAdjListBatchIO(char* buf);
	static void RespondAdjListBatchIoPartialList(char* buf);
	static void RespondAdjListBatchIoFullList(AdjListRequestBatch* req, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, Range<int> version_range, EdgeType e_type, DynamicDBType d_type=INSERT);
	static void RespondAdjListBatchIoFullListFromCache(int win_lv, AdjListRequestBatch* req, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, int64_t& bytes_served_from_memory, node_t& total_num_vids_cnts, node_t& num_vertices_served_from_memory, Range<int> version_range);
    static void RespondAdjListBatchIoFullListParallel(AdjListRequestBatch* req, Range<node_t> vid_range, Range<node_t> bitmap_vid_range, TwoLevelBitMap<node_t>* requested_vertices, TwoLevelBitMap<node_t>* vertices_from_cache, tbb::concurrent_queue<UdfSendRequest>* req_data_queue, int64_t* stop_flag, Range<int> version_range, EdgeType e_type, DynamicDBType d_type);
	static int64_t ComputeMaterializedAdjListsSize(Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, BitMap<node_t>* requested_vertices, int64_t MaxBytesToSend);
    static void FindPageIdsToReadFrom(Range<int> version_range, Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, bool is_initialized, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, Range<node_t> ProcessedVertices);
    static void FindFullListPageIdsToReadFrom(Range<int> version_range, Range<int64_t> dst_edge_partition_range, Range<node_t> vid_range, bool is_initialized, EdgeType e_type, DynamicDBType d_type, FixedSizeVector<FixedSizeVector<FixedSizeVector<PageID>>>& CurPid, Range<node_t> ProcessedVertices);
    static void CopyMaterializedAdjListsFromCache(MaterializedAdjacencyLists& mat_adj, SimpleContainer& cont, int64_t adjlist_data_size_cache, int64_t slot_data_size_cache, FixedSizeVector<node_t>& vertices, Range<node_t> vid_range, int64_t size_to_request_cache);

	static void MatAdjlistSanitycheck(char* data_buffer, int64_t data_size, int64_t capacity, int tag);

	static void GetMatAdjBufferHitMissBytes(int64_t& hit, int64_t& miss) {
		hit = matadj_hit_bytes.load();
		miss = matadj_miss_bytes.load();
        return;
	}

	static void EndRunning() {
		ExitRequest req;
		req.from = PartitionStatistics::my_machine_id();
		req.to = PartitionStatistics::my_machine_id();
		req.rt = RequestType::Exit;

		SendRequest(req.to, req);

		mutex_for_end.lock();
		end = true;
		mutex_for_end.unlock();
	}

	static void WaitRequestRespond(bool need_barrier) {
	    turbo_timer tim;
    
        tim.start_timer(0);
        tim.stop_timer(0);
        tim.start_timer(1);
        rr_.async_req_pool.WaitAll();
        tim.stop_timer(1);

        tim.start_timer(2);
        if (need_barrier)
			PartitionStatistics::wait_for_all();	// BARRIER, 불필요한 경우 안하도록 처리해야함
        tim.stop_timer(2);
		
        tim.start_timer(3);
		rr_.async_resp_pool.WaitAll();
        tim.stop_timer(3);
	
#ifdef REPORT_PROFILING_TIMERS
        fprintf(stdout, "[%ld] (%ld, %ld) WaitRequestRespond\t%.4f\t%.4f\t%.4f\t%.4f\n", PartitionStatistics::my_machine_id(), UserArguments::UPDATE_VERSION, UserArguments::SUPERSTEP, tim.get_timer(0), tim.get_timer(1), tim.get_timer(2), tim.get_timer(3));
#endif
    }

    static int64_t WaitMyPendingDiskIO(diskaio::DiskAioInterface* my_io) {
        ALWAYS_ASSERT(my_io != NULL);
        int num_to_complete = my_io->GetNumOngoing();
		int backoff = 1;
		while (my_io->GetNumOngoing() > 0) {
			usleep (backoff * 1024);
			my_io->WaitForResponses(0);
			if (backoff <= 16 * 1024) backoff *= 2;
		}
        ALWAYS_ASSERT(my_io->GetNumOngoing() == 0);
		return num_to_complete;
	}

	static int64_t WaitMyPendingDiskIO(diskaio::DiskAioInterface** my_io = GetMyDiskIoInterface()) {
        ALWAYS_ASSERT(my_io != NULL);
        int num_total_to_complete = 0;
        std::list<IOMode> io_modes = { WRITE_IO, READ_IO };
        for (auto& io_mode: io_modes) {
            int num_to_complete = my_io[io_mode]->GetNumOngoing();
            int backoff = 1;
            while (my_io[io_mode]->GetNumOngoing() > 0) {
                usleep (backoff * 1024);
                my_io[io_mode]->WaitForResponses(0);
                if (backoff <= 16 * 1024) backoff *= 2;
            }
            num_total_to_complete += num_to_complete;
        }
		return num_total_to_complete;
	}

    static void InitializePerThreadBufferQueue() {
        //per_thread_buffer_size_ = 8 * 1024 * 1024 * 1024L;
        per_thread_buffer_size_ = 2 * 1024 * 1024 * 1024L;
        //per_thread_buffer_size_ = 1 * 1024 * 1024 * 1024L;
        int64_t total_nums = per_thread_buffer_size_ / PER_THREAD_BUFF_SIZE;
        
        per_thread_buffer_ = (char*) NumaHelper::alloc_numa_interleaved_memory(per_thread_buffer_size_);
        for (int64_t i = 0; i < total_nums; i++) {
			char* tmp = per_thread_buffer_ + PER_THREAD_BUFF_SIZE * i;
			std::memset ((void*) tmp, 0, PER_THREAD_BUFF_SIZE);
            ReturnPerThreadBuffer(tmp);
        }
    }
    static double GetRemainingBuffersInPerThreadBufferInPortion() {
        double portion = (double) per_thread_buffer_queue_.size_approx() / ((double) per_thread_buffer_size_ / PER_THREAD_BUFF_SIZE);

        if (portion < 0.1f) {
            //fprintf(stdout, "remaining buffer portion = %.4f\n", portion);
        }
        return portion;
    }
    static int64_t GetRemainingBuffersInPerThreadBuffer() {
        return per_thread_buffer_queue_.size_approx();
    }
    static void ReturnPerThreadBuffer(char* buf) {
        per_thread_buffer_queue_.enqueue(buf);
    }
    static char* GetPerThreadBuffer(const std::function<void()>& fcn_to_call_when_waiting = {}) {
        char* buf = NULL;
        int backoff = 1;
        while (!per_thread_buffer_queue_.try_dequeue(buf)) {
            if (fcn_to_call_when_waiting) {
                fcn_to_call_when_waiting();
            }
            usleep(backoff * 1);
            backoff= 2 * backoff;
            if (backoff >= 1024) backoff = 1024;
        }
        ALWAYS_ASSERT(buf != NULL);
        return buf;
    }

	static diskaio::DiskAioInterface**& GetMyDiskIoInterface();
	static diskaio::DiskAioInterface*& GetMyDiskIoInterface(IOMode mode);

	static char* GetTempReqReceiveBuffer();
	static SimpleContainer GetTempDataBuffer(int64_t req_size);
	static TwoLevelBitMap<node_t>* GetTempNodeBitMap(bool sender_or_receiver, int64_t backoff_limit = -1);
	static void ReturnTempReqReceiveBuffer(char* tmp_buffer);
	static void ReturnTempDataBuffer(SimpleContainer cont);
	static void ReturnTempNodeBitMap(bool sender_or_receiver, TwoLevelBitMap<node_t>* tmp_bitmap);

	static std::mutex mutex_for_end;
	static bool end; // use mutex_for_end when using this

	static per_thread_lazy<SimpleContainer> per_thread_container_;
	static moodycamel::ConcurrentQueue<char*> req_buffer_queue_;
	static moodycamel::ConcurrentQueue<SimpleContainer> small_data_buffer_queue_;
	static moodycamel::ConcurrentQueue<SimpleContainer> large_data_buffer_queue_;
	static moodycamel::ConcurrentQueue<TwoLevelBitMap<node_t>*> temp_bitmap_queue_[2];
    static std::queue<std::future<void>> reqs_to_wait_allreduce[2];
	
    static moodycamel::ConcurrentQueue<char*> per_thread_buffer_queue_;
    static char* per_thread_buffer_;
    static int64_t per_thread_buffer_size_;

	static int64_t max_permanent_buffer_size;
	static std::atomic<int64_t> bytes_permanently_allocated;

	static int64_t max_dynamic_buffer_size;
	static std::atomic<int64_t> bytes_dynamically_allocated;

	static int64_t max_dynamic_recv_buffer_size;
	static std::atomic<int64_t> recv_buffer_bytes_dynamically_allocated;
	static int64_t max_dynamic_send_buffer_size;
	static std::atomic<int64_t> send_buffer_bytes_dynamically_allocated;
    
	static int64_t GetMaxPermanentBufferSize () {
		return RequestRespond::max_permanent_buffer_size;
	}
	static void SetMaxPermanentBufferSize (int64_t max) {
		RequestRespond::max_permanent_buffer_size = max;
	}
	static int64_t GetMaxDynamicBufferSize () {
		return RequestRespond::max_dynamic_buffer_size;
	}
	static void SetMaxDynamicBufferSize (int64_t max) {
		RequestRespond::max_dynamic_buffer_size = max;
		circular_temp_data_buffer.Initialize(max);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularTempDataBuffer)", max / (1024 * 1024L));
	}
	static int64_t GetMaxDynamicRecvBufferSize () {
		return RequestRespond::max_dynamic_recv_buffer_size;
	}
	static void SetMaxDynamicRecvBufferSize (int64_t max) {
		RequestRespond::max_dynamic_recv_buffer_size = max;
		circular_stream_recv_buffer.Initialize(max);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularRecvBuffer)", max / (1024 * 1024L));
	}
	static int64_t GetMaxDynamicSendBufferSize () {
		return RequestRespond::max_dynamic_send_buffer_size;
	}
	static void SetMaxDynamicSendBufferSize (int64_t max) {
		RequestRespond::max_dynamic_send_buffer_size = max;
		circular_stream_send_buffer.Initialize(max);
		LocalStatistics::register_mem_alloc_info("RequestResond(CircularSendBuffer)", max / (1024 * 1024L));
	}

	static SimpleContainer GetDataSendBuffer (int64_t req_buf_size);
	static SimpleContainer GetDataRecvBuffer (int64_t req_buf_size);
	static void ReturnDataSendBuffer (SimpleContainer cont);
	static void ReturnDataRecvBuffer (SimpleContainer cont);

  public:

	inline virtual void CallbackTask(diskaio::DiskAioRequestUserInfo &user_info) {
		ALWAYS_ASSERT (user_info.do_user_cb);
	}

	static void UserCallbackSanityCheck(char* data_buffer, int64_t data_size, RequestType rt);
};


template<typename RequestType>
void RequestRespond::SendRequestAsynchronously(int partition_id, RequestType req_buffer) {
	RequestType* tmp = (RequestType*) new char[sizeof(RequestType)];
	*tmp = req_buffer;
	Aio_Helper::async_pool.enqueue(RequestRespond::SendSynchronously, partition_id, (char*) tmp, sizeof(RequestType), 1);
}

template<typename RequestType>
void RequestRespond::SendRequest(int partition_id, RequestType req_buffer) {
	MPI_Send((void *) &req_buffer, sizeof(RequestType), MPI_CHAR, partition_id, 1, MPI_COMM_WORLD);
}

template<typename RequestType>
void RequestRespond::SendRequest(int partition_id, RequestType* req, int num_reqs) {
	ALWAYS_ASSERT(partition_id == req->to);
	ALWAYS_ASSERT(num_reqs > 0);
	ALWAYS_ASSERT(req != NULL);

	char str_buf[64];
	sprintf(&str_buf[0], "BEGIN\tSendRequest to %d\n", partition_id);
	LOG(INFO) << std::string(str_buf);

	int32_t reqs_buf_size = num_reqs * sizeof(RequestType);
	MPI_Send((void *) req, reqs_buf_size, MPI_CHAR, partition_id, 1, MPI_COMM_WORLD);

	sprintf(&str_buf[0], "END\tSendRequest to %d\n", partition_id);
	LOG(INFO) << std::string(str_buf);
}


template<typename RequestType, typename ObjectType, typename FcnType>
void RequestRespond::SendRequest(RequestType* req, int num_reqs, bool wait, ObjectType* obj, FcnType* fcn) {
	ALWAYS_ASSERT(req != NULL);
	ALWAYS_ASSERT(num_reqs > 0);

	int32_t reqs_buf_size = num_reqs * sizeof(RequestType);
	MPI_Send((void *) req, reqs_buf_size, MPI_CHAR, req->to, 1, MPI_COMM_WORLD);
	if (wait) {
		(*fcn)(obj, req->to);
	} else {
		Aio_Helper::async_pool.enqueue(*fcn, obj, req->to);
	}
}

template<typename Request>
void RequestRespond::SendRequest(Request* req, int num_reqs, char* data_buffer[], int64_t data_size[], int64_t num_data_buffers) {
    turbo_timer sendrequest_timer;
	ALWAYS_ASSERT(req != NULL && data_buffer != NULL);
	ALWAYS_ASSERT(num_reqs > 0);
	int32_t reqs_buf_size = num_reqs * sizeof(Request);
	int rc = -1;
    sendrequest_timer.start_timer(0);
	rc = MPI_Send((void *) req, reqs_buf_size, MPI_CHAR, req->to, 1, MPI_COMM_WORLD);
    sendrequest_timer.stop_timer(0);
	ALWAYS_ASSERT (rc == MPI_SUCCESS);

	RequestType rt_data;
	if (req->rt == AdjListBatchIo) {
		rt_data = RequestType::AdjListBatchIoData;
	} else if (req->rt == UserCallback) {
		rt_data = RequestType::UserCallbackData;
	} else {
		fprintf(stderr, "[RequestRespond::SendRequest] Undefined Request Type\n");
		ALWAYS_ASSERT (false);
	}
    
    int64_t num_total_bytes = 0;
    sendrequest_timer.start_timer(1);
    for (int64_t idx = 0; idx < num_data_buffers; idx++) {
		ALWAYS_ASSERT(data_size[idx] > 0);
        int64_t bytes_sent = 0;
        turbo_tcp::Send(&RequestRespond::general_client_sockets[0], (char*) data_buffer[idx], 0, data_size[idx], req->to, &bytes_sent, true);
        num_total_bytes += bytes_sent;
        ALWAYS_ASSERT(bytes_sent == data_size[idx]);
    }
    sendrequest_timer.stop_timer(1);
    //fprintf(stdout, "[%ld] SendRequest (%d->%d) %.3f %.3f, %.3f(MB/sec)\n", PartitionStatistics::my_machine_id(), PartitionStatistics::my_machine_id(), req->to, sendrequest_timer.get_timer(0), sendrequest_timer.get_timer(1), (double) num_total_bytes / (sendrequest_timer.get_timer(1) * 1024 * 1024));
}


template<typename Request>
void RequestRespond::SendRequestReturnContainer(Request req, char* data_buffer, int64_t data_size, SimpleContainer cont_to_return) {
	ALWAYS_ASSERT(data_buffer != NULL);
	ALWAYS_ASSERT(data_size > 0);
	int32_t reqs_buf_size = sizeof(Request);

	RequestRespond::client_sockets.send_to_server_lock(req.to);

	int rc = -1;
	req.data_size = data_size;
	rc = MPI_Send((void *) &req, reqs_buf_size, MPI_CHAR, req.to, 1, MPI_COMM_WORLD);
	ALWAYS_ASSERT (rc == MPI_SUCCESS);

    if (req.rt == UserCallback) {
    } else {
		fprintf(stderr, "[RequestRespond::SendRequest] Undefined Request Type\n");
		ALWAYS_ASSERT (false);
	}
	RequestRespond::client_sockets.send_to_server_unlock(req.to);
	RequestRespond::ReturnDataSendBuffer(cont_to_return);
}

template <typename T, Op op>
void RequestRespond::TurboAllReduceInPlace(turbo_tcp* server_socket, turbo_tcp* client_socket, int socket_idx, char* buf, int count, int size_in_bytes) {
    SimpleContainer cont_temp[2];
    int64_t bytes_sent[2], bytes_received[2];
    int64_t total_bytes_sent = 0, total_bytes_received = 0;
    int64_t size_once = (DEFAULT_NIO_BUFFER_SIZE / sizeof(T)) * sizeof(T);
    for (int i = 0; i < 2; i++) {
        cont_temp[i] = RequestRespond::GetDataSendBuffer(size_once);
        cont_temp[i].size_used = 0;
        bytes_sent[i] = bytes_received[i] = 0;
    }

    int64_t total_size = count * size_in_bytes;
    int granule = 2, cur = 0, prev = 0;
    T* buffer = (T*)buf;
    char* buffer_indexable;

    int iteration = 0;
    int max_iteration = std::ceil (std::log2 ((double) PartitionStatistics::num_machines()));
    bool is_finished = false;

    std::queue<std::future<void>> reqs_to_wait;

    // Reduce
    while (iteration < max_iteration) {
        total_bytes_sent = 0;
        total_bytes_received = 0;
        if (((PartitionStatistics::my_machine_id() + 1) % granule == 1) && ((PartitionStatistics::my_machine_id() + 1) + (granule / 2) > PartitionStatistics::num_machines()) || is_finished) {
        } else {
            if ((PartitionStatistics::my_machine_id() + 1) % granule == 1) { // recv from my_id + (granule / 2)
                cur = prev = 0;
                reqs_to_wait.push(Aio_Helper::async_pool.enqueue(turbo_tcp::Recv, server_socket, (const char*) cont_temp[cur].data, (size_t)0, PartitionStatistics::my_machine_id() + (granule / 2), &bytes_received[cur], size_once, true));

                while (true) {
                    reqs_to_wait.front().get();
                    reqs_to_wait.pop();
                    prev = cur;
                    cur = (cur + 1) % 2;

                    total_bytes_received += bytes_received[prev];
                    if (total_bytes_received == total_size) break;
                    reqs_to_wait.push(Aio_Helper::async_pool.enqueue(turbo_tcp::Recv, server_socket, (const char*) cont_temp[cur].data, (size_t)0, PartitionStatistics::my_machine_id() + (granule / 2), &bytes_received[cur], size_once, true));

                    T* recv_buffer = (T*) cont_temp[prev].data;
                    int64_t offset = total_bytes_received / sizeof(T);
                    int64_t num_entries_received = bytes_received[prev] / sizeof(T);
                    for (int64_t i = 0; i < num_entries_received; i++) {
                        Operation<T, op> (&buffer[offset - num_entries_received + i], recv_buffer[i]);
                    }
                }
                //#pragma omp parallel for
                T* recv_buffer = (T*) cont_temp[prev].data;
                int64_t offset = total_bytes_received / sizeof(T);
                int64_t num_entries_received = bytes_received[prev] / sizeof(T);
                for (int64_t i = 0; i < num_entries_received; i++) {
                    Operation<T, op> (&buffer[offset - num_entries_received + i], recv_buffer[i]);
                }
            } else { // send to my_id - (granule / 2)
                while (total_bytes_sent != total_size) {
                    int64_t size_to_send = (total_size - total_bytes_sent) > size_once ? size_once : total_size - total_bytes_sent;
                    int64_t bytes_send = client_socket->send_to_server((char*) buffer, total_bytes_sent, size_to_send, PartitionStatistics::my_machine_id() - (granule / 2));
                    INVARIANT(bytes_send == size_to_send);
                    total_bytes_sent += bytes_send;
                }
                is_finished = true;
            }
        }
        iteration++;
        granule *= 2;
    }


    // Gather
    iteration = 0;
    while (iteration < max_iteration) {
        total_bytes_sent = 0;
        total_bytes_received = 0;
        granule /= 2;
        if (((PartitionStatistics::my_machine_id() + 1) % granule != 1) && ((PartitionStatistics::my_machine_id() + 1) % granule != ((granule / 2) + 1) % granule)) {
        } else if (((PartitionStatistics::my_machine_id() + 1) % granule == 1) && (PartitionStatistics::my_machine_id() + (granule / 2) >= PartitionStatistics::num_machines())){
        } else {
            if ((PartitionStatistics::my_machine_id() + 1) % granule == 1) { // send to my_id + (granule / 2)
                while (total_bytes_sent != total_size) {
                    int64_t size_to_send = (total_size - total_bytes_sent) > size_once ? size_once : total_size - total_bytes_sent;
                    int64_t bytes_send = client_socket->send_to_server((char*) buffer, total_bytes_sent, size_to_send, PartitionStatistics::my_machine_id() + (granule / 2));
                    INVARIANT(bytes_send == size_to_send);
                    total_bytes_sent += bytes_send;
                }
            } else { // recv from my_id - (granule / 2)
                cur = prev = 0;
                reqs_to_wait.push(Aio_Helper::async_pool.enqueue(turbo_tcp::Recv, server_socket, (const char*) cont_temp[cur].data, (size_t)0, PartitionStatistics::my_machine_id() - (granule / 2), &bytes_received[cur], size_once, true));
                while (true) {
                    reqs_to_wait.front().get();
                    reqs_to_wait.pop();
                    prev = cur;
                    cur = (cur + 1) % 2;

                    total_bytes_received += bytes_received[prev];
                    if (total_bytes_received == total_size) break;
                    reqs_to_wait.push(Aio_Helper::async_pool.enqueue(turbo_tcp::Recv, server_socket, (const char*) cont_temp[cur].data, (size_t)0, PartitionStatistics::my_machine_id() - (granule / 2), &bytes_received[cur], size_once, true));
                    //#pragma omp parallel for
                    T* recv_buffer = (T*) cont_temp[prev].data;
                    int64_t offset = total_bytes_received / sizeof(T);
                    int64_t num_entries_received = bytes_received[prev] / sizeof(T);
                    for (int64_t i = 0; i < bytes_received[prev]; i++) {
                        buffer[offset - num_entries_received + i] = recv_buffer[i];
                    }
                }
                T* recv_buffer = (T*) cont_temp[prev].data;
                int64_t offset = total_bytes_received / sizeof(T);
                int64_t num_entries_received = bytes_received[prev] / sizeof(T);
                for (int64_t i = 0; i < bytes_received[prev]; i++) {
                    buffer[offset - num_entries_received + i] = recv_buffer[i];
                }
            }
        }
        iteration++;
    }
    ALWAYS_ASSERT(granule == 2);
    INVARIANT(reqs_to_wait.empty());
    for (int i = 0; i < 2; i++) RequestRespond::ReturnDataSendBuffer(cont_temp[i]);
    if (socket_idx != -1 && socket_idx != 0 && socket_idx != 1) {
        RequestRespond::general_server_sockets[socket_idx].lock_socket();
        RequestRespond::general_connection_pool[socket_idx] = true;
        RequestRespond::general_server_sockets[socket_idx].unlock_socket();
    }
}

template <typename T, Op op> 
void RequestRespond::TurboAllReduceSync(char* buf, int count, int size_in_bytes) {
    RequestRespond::TurboAllReduceInPlace<T, op>(&RequestRespond::general_server_sockets[1], &RequestRespond::general_client_sockets[1], 1, buf, count, size_in_bytes);
}

template <typename T, Op op> 
void RequestRespond::TurboAllReduceInPlaceAsync(char* buf, int count, int size_in_bytes, int queue_idx) {
    // TODO if # machines == 1, correct?
    // Get available sockets
    int available_socket_idx = -1;
    if (PartitionStatistics::my_machine_id() == 0) {
        while (true) {
            auto it = std::find (RequestRespond::general_connection_pool.begin(), RequestRespond::general_connection_pool.end(), true);
            if (it == RequestRespond::general_connection_pool.end()) usleep(1000);
            else {
                available_socket_idx = it - RequestRespond::general_connection_pool.begin();
                break;
            }
        }
        INVARIANT(available_socket_idx >= 0 && available_socket_idx < DEFAULT_NUM_GENERAL_TCP_CONNECTIONS);
        RequestRespond::general_server_sockets[available_socket_idx].lock_socket();
        RequestRespond::general_connection_pool[available_socket_idx] = false;
        RequestRespond::general_server_sockets[available_socket_idx].unlock_socket();
        MPI_Bcast((void*) &available_socket_idx, sizeof(int), MPI_INT, 0, MPI_COMM_WORLD);
    } else {
        MPI_Bcast((void*) &available_socket_idx, sizeof(int), MPI_INT, 0, MPI_COMM_WORLD);
    }

    // Enqueue into Async Pool
    RequestRespond::reqs_to_wait_allreduce[queue_idx].push(Aio_Helper::async_pool.enqueue(RequestRespond::TurboAllReduceInPlace<T, op>, &RequestRespond::general_server_sockets[available_socket_idx], &RequestRespond::general_client_sockets[available_socket_idx], available_socket_idx, buf, count, size_in_bytes));
}

#endif
