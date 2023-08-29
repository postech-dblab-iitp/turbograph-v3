#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <atomic>
#include <vector>
#include <queue>
#include <list>
#include <memory>
#include <thread>
#include <pthread.h>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <sched.h>
#include <sys/syscall.h>
#include "common/common.hpp"
#include "parallel/util.hpp"
#include "turbo_dist_internal.hpp"
#include "common/assert.hpp"

#define PRINT_CPU_AFFINITY 0

template <typename PerThreadData = int>
class ThreadPool {
  public:
	ThreadPool(size_t);
	ThreadPool();
	~ThreadPool();

	void WaitAll();
	void Close();
	template <typename FcnType> void InitPerThread(FcnType* fcn);
	void SpawnThreads(int64_t num_threads);
	void SpawnThreads(int64_t num_threads, int64_t affinity_from, int64_t affinity_to);
	void SpawnThreadsToCpuSet(int64_t num_threads, int64_t affinity_from, int64_t affinity_to);

	void run(int64_t thread_id, int64_t core_id, int64_t socket_id);

    template<class F, class... Args>
	auto enqueue(F&& f, Args... args)
	-> std::future<typename std::result_of<F(Args...)>::type>;

	template<class F, class... Args>
	auto enqueue_hot(F&& f, Args... args)
	-> std::future<typename std::result_of<F(Args...)>::type>;

	int32_t NumWorkers() {
		return workers.size();
	}
	double GetTotalCpuTime();

	PerThreadData& my(int64_t core_id);
	PerThreadData& my(int64_t core_id) const;

  private:

	// need to keep track of threads so we can join them
	std::vector< std::thread > workers;
	// the task queue
	//std::queue< std::function<void()> > tasks;
	std::list< std::function<void()> > tasks;

	// synchronization
	std::mutex queue_mutex;
	//atom lock;
	std::condition_variable condition;
	bool stop;

	int64_t num_running_tasks;
	timespec* cpu_time_begin;
};

template <typename T>
inline void ThreadPool<T>::run(int64_t thread_id, int64_t core_id, int64_t socket_id) {
    TG_ThreadContexts::thread_id = thread_id;
    TG_ThreadContexts::core_id = core_id;
    TG_ThreadContexts::socket_id = socket_id;

	struct timespec currTime;
	clockid_t cid;
	int rc = pthread_getcpuclockid(pthread_self(), &cid);
	if (rc != 0)
		perror("pthread_getcpuclockid");
	clock_gettime(cid, &currTime);
	cpu_time_begin[thread_id] = currTime;

	for(;;) {
		std::function<void()> task;
		{
			std::unique_lock<std::mutex> lock(this->queue_mutex);
			this->condition.wait(lock,
			                     [this] { return this->stop || !this->tasks.empty(); });
			if(this->stop && this->tasks.empty())
				return;
			std::atomic_fetch_add((std::atomic<int64_t>*) &num_running_tasks, 1L);
			task = std::move(this->tasks.front());
			this->tasks.pop_front();
		}
		task();
		std::atomic_fetch_add((std::atomic<int64_t>*) &num_running_tasks, -1L);
	}
}

// the constructor just launches some amount of workers
template <typename T>
inline ThreadPool<T>::ThreadPool(size_t threads)
	:   stop(false), num_running_tasks (0) {
	SpawnThreads(threads);
	cpu_time_begin = NULL;
}
template <typename T>
inline ThreadPool<T>::ThreadPool()
	:   stop(false), num_running_tasks (0) {
	cpu_time_begin = NULL;
}

// add new work item to the pool
template <typename T>
template<class F, class... Args>
auto ThreadPool<T>::enqueue_hot(F&& f, Args... args)
-> std::future<typename std::result_of<F(Args...)>::type> {
	using return_type = typename std::result_of<F(Args...)>::type;
	auto task = std::make_shared< std::packaged_task<return_type()> >(
	                std::bind(std::forward<F>(f), std::forward<Args>(args)...)
	            );

	std::future<return_type> res = task->get_future();
	{
		std::unique_lock<std::mutex> lock(queue_mutex);
		// don't allow enqueueing after stopping the pool
		if(stop) {
			fprintf(stdout, "enqueue on stopped ThreadPool");
			throw std::runtime_error("enqueue on stopped ThreadPool");
		}
		tasks.emplace_front([task]() {
			(*task)();
		});
		//tasks.emplace([task](){ (*task)(); });
	}
	condition.notify_one();
	return res;
}



// add new work item to the pool
template <typename T>
template<class F, class... Args>
auto ThreadPool<T>::enqueue(F&& f, Args... args)
-> std::future<typename std::result_of<F(Args...)>::type> {
	using return_type = typename std::result_of<F(Args...)>::type;
	auto task = std::make_shared< std::packaged_task<return_type()> >(
	                std::bind(std::forward<F>(f), std::forward<Args>(args)...)
	            );

	std::future<return_type> res = task->get_future();
	{
		std::unique_lock<std::mutex> lock(queue_mutex);
		// don't allow enqueueing after stopping the pool
		if(stop) {
			fprintf(stdout, "enqueue on stopped ThreadPool");
			throw std::runtime_error("enqueue on stopped ThreadPool");
		}
		tasks.emplace_back([task]() {
			(*task)();
		});
		//tasks.emplace([task](){ (*task)(); });
	}
	condition.notify_one();
	return res;
}

template <typename T>
inline void ThreadPool<T>::Close() {
	{
		std::unique_lock<std::mutex> lock(queue_mutex);
		stop = true;
	}
	condition.notify_all();
	if (!tasks.empty()) {
		fprintf(stdout, "ThreadPool: Being closed while there are left %d tasks\n", tasks.size());
	}
	for(std::thread &worker: workers) {
		worker.join();
	}
	workers.clear();
}

// the destructor joins all threads
template <typename T>
inline ThreadPool<T>::~ThreadPool() {
	if (stop == false) {
		Close();
	}
}

template <typename T>
inline void ThreadPool<T>::SpawnThreads(int64_t threads) {
	if (cpu_time_begin != NULL) {
		delete[] cpu_time_begin;
		cpu_time_begin = NULL;
	}
	cpu_time_begin = new timespec[threads];

	D_ASSERT (stop == false);
	Close();
    
    if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "[ThreadPool:SpawnThreads] Spawn %lld threads\n", threads);
	
    stop = false;
	num_running_tasks = 0;
	for(size_t i = 0; i<threads; ++i) {
		int64_t thread_id = i;
		int64_t core_id = NumaHelper::assign_core_id_by_round_robin(i + 1);
		int64_t socket_id = NumaHelper::get_socket_id(core_id);
		workers.emplace_back(std::move(std::thread(&ThreadPool::run, this, thread_id, core_id, socket_id)));
	}

	// http://eli.thegreenplace.net/2016/c11-threads-affinity-and-hyperthreading/
	cpu_set_t cpuset;
	for(size_t i = 0; i<threads; ++i) {
		CPU_ZERO(&cpuset);
		CPU_SET( NumaHelper::assign_core_id_by_round_robin(i + 1), &cpuset);
		int rc = pthread_setaffinity_np(workers[i].native_handle(), sizeof(cpu_set_t), &cpuset);
		if (rc != 0) {
			std::cout << "ERROR calling pthread_setaffinity_np; " << rc << std::endl;
			abort();
		}
	}
}
template <typename T>
inline void ThreadPool<T>::SpawnThreads(int64_t threads, int64_t affinity_from, int64_t affinity_to) {
	if (cpu_time_begin != NULL) {
		delete[] cpu_time_begin;
		cpu_time_begin = NULL;
	}
	cpu_time_begin = new timespec[threads];
	D_ASSERT (stop == false);
	Close();
    
    if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "[ThreadPool:SpawnThreads] Spawn %lld threads into cores [%ld, %ld]\n", threads, affinity_from, affinity_to);
    
    stop = false;
	num_running_tasks = 0;
	for(size_t i = 0; i<threads; ++i) {
		int64_t thread_id = i;
		int64_t core_id =  NumaHelper::assign_core_id_scatter(i, affinity_from, affinity_to);
		int64_t socket_id = NumaHelper::get_socket_id_by_core_id(core_id);
#if PRINT_CPU_AFFINITY
		if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "\tthread_id = %lld, core_id = %lld, socket_id = %lld\n", i, core_id, socket_id);
#endif
		workers.emplace_back(std::move(std::thread(&ThreadPool::run, this, thread_id, core_id, socket_id)));

		// http://eli.thegreenplace.net/2016/c11-threads-affinity-and-hyperthreading/
		cpu_set_t cpuset;
		int affinity = core_id;
		if (affinity < 0 || affinity >= NumaHelper::num_cores()) {
			fprintf(stdout, "i = %lld, threads = %lld, affinity = %lld, from = %lld, to = %lld\n", i, threads, affinity, affinity_from, affinity_to);
		}
		D_ASSERT (affinity >= 0 && affinity < NumaHelper::num_cores());
		CPU_ZERO(&cpuset);
		CPU_SET(affinity, &cpuset);
		int rc = pthread_setaffinity_np(workers[i].native_handle(), sizeof(cpu_set_t), &cpuset);
		if (rc != 0) {
			std::cout << "ERROR calling pthread_setaffinity_np; " << rc << std::endl;
			abort();
		}
	}
}
template <typename T>
inline void ThreadPool<T>::SpawnThreadsToCpuSet(int64_t threads, int64_t affinity_from, int64_t affinity_to) {
	if (cpu_time_begin != NULL) {
		delete[] cpu_time_begin;
		cpu_time_begin = NULL;
	}
	cpu_time_begin = new timespec[threads];
	D_ASSERT (stop == false);
	Close();
#if PRINT_CPU_AFFINITY
	if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "[ThreadPool:SpawnThreadsToCpuSet(%lld, %lld, %lld)] Spawn %lld threads\n", threads, affinity_from, affinity_to, threads);
#endif
    stop = false;
	num_running_tasks = 0;

	// http://eli.thegreenplace.net/2016/c11-threads-affinity-and-hyperthreading/
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	for(size_t i = 0; i<threads; ++i) {
		int64_t core_id =  NumaHelper::assign_core_id_scatter(i, affinity_from, affinity_to);
		int affinity = core_id;
		if (affinity < 0 || affinity >= NumaHelper::num_cores()) {
			fprintf(stdout, "i = %lld, threads = %lld, affinity = %lld, from = %lld, to = %lld\n", i, threads, affinity, affinity_from, affinity_to);
		}
		D_ASSERT (affinity >= 0 && affinity < NumaHelper::num_cores());
		CPU_SET(affinity, &cpuset);
	}

	for(size_t i = 0; i<threads; ++i) {
		int64_t thread_id = i;
		int64_t core_id =  NumaHelper::assign_core_id_scatter(i, affinity_from, affinity_to);
		int64_t socket_id = NumaHelper::get_socket_id_by_core_id(core_id);
#if PRINT_CPU_AFFINITY
		if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "\tthread_id = %lld, core_id = %lld, socket_id = %lld\n", i, core_id, socket_id);
#endif
        workers.emplace_back(std::move(std::thread(&ThreadPool::run, this, thread_id, core_id, socket_id)));

		int rc = pthread_setaffinity_np(workers[i].native_handle(), sizeof(cpu_set_t), &cpuset);
		if (rc != 0) {
			std::cout << "ERROR calling pthread_setaffinity_np; " << rc << std::endl;
			abort();
		}
	}
}

template <typename T>
double ThreadPool<T>::GetTotalCpuTime() {
	double total_secs = 0;
	for (int i = 0; i < workers.size(); i++) {
		struct timespec currTime;
		clockid_t cid;
		int rc = pthread_getcpuclockid(workers[i].native_handle(), &cid);
		if (rc != 0)
			perror("pthread_getcpuclockid");
		clock_gettime(cid, &currTime);
		total_secs += ((currTime.tv_sec - cpu_time_begin[i].tv_sec) + (currTime.tv_nsec - cpu_time_begin[i].tv_nsec) / 1000000000.0);
	}
	return total_secs;
}

template <typename T>
inline void ThreadPool<T>::WaitAll() {
	int backoff = 1;
	while (true) {
		{
			std::unique_lock<std::mutex> lock(queue_mutex);
			if (tasks.empty()) {
				break;
			}
		}
		usleep(backoff * 100);
		backoff = 2 * backoff;
		if (backoff >= 1024) {
			backoff = 1024;
		}
	}
	backoff = 1;
	int num_tasks = -1;
	while ( (num_tasks = std::atomic_load((std::atomic<int64_t>*) &num_running_tasks)) != 0) {
		usleep(backoff * 100);
		backoff = 2 * backoff;
		if (backoff >= 1024) {
			backoff = 1024;
		}
	}
}

template <typename T>
template <typename FcnType>
void ThreadPool<T>::InitPerThread(FcnType* fcn) {
	abort();
}


#endif