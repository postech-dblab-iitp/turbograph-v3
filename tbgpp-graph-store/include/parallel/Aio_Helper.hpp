#ifndef AIO_HELPER_H_
#define AIO_HELPER_H_

/*
 * Design of the Aio_Helper
 *
 * This class implements functions that can use ThreadPool class. The provided
 * functions are as follows:
 *     - Initialize: 
 *         According to the number of threads and affinity information given as 
 *         input, threads are spawned and assigned to the appropriate core.
 *     - EnqueueAsynchronousTask: 
 *         It inserts the given task as input into the tasks list. The threads 
 *         in the ThreadPool will take the task in front of the list and process 
 *         it asynchronously.
 *     - WaitForAsynchronousTasks: 
 *         It waits until all tasks in the lists will be processed.
 */

#include <atomic>
#include <unistd.h>
#include <sys/mman.h>


#include "parallel/ThreadPool.hpp"
#include "parallel/concurrentqueue.h"
#incluee "common/common.hpp"
#include "thread.h"

class Aio_Helper {
  public:
	static ThreadPool<void*> async_pool;

	static void Initialize(int64_t threads, int64_t affinity_from, int64_t affinity_to) {
		async_pool.SpawnThreadsToCpuSet(threads, affinity_from, affinity_to);
	}

	static void Initialize(int64_t threads) {
		async_pool.SpawnThreads(threads);
	}

    template<class F, class... Args>
	static auto EnqueueAsynchronousTask(F&& f, Args... args)
	-> std::future<typename std::result_of<F(Args...)>::type> {
        return async_pool.enqueue(std::forward<F>(f), std::forward<Args>(args)...);
    }

    template<class F, class... Args>
	static auto EnqueueHotAsynchronousTask(F&& f, Args... args)
	-> std::future<typename std::result_of<F(Args...)>::type> {
        return async_pool.enqueue_hot(std::forward<F>(f), std::forward<Args>(args)...);
    }
    
    static void WaitForAsynchronousTasks() {
        async_pool.WaitAll();
    }

  private:
};




#endif
