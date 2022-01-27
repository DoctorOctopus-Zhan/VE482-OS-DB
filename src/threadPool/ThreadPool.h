#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include "../query/Query.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

/**
 * how many rows will be processed
 * in one subquery. (aka, one thread)
 */
#define ROW_PER_SUBQUERY 1000

/**
 * singleton threadpool
 */
class ThreadPool {
private:
  size_t totalThreads;
  using Task = std::function<void()>;

  /**
   * The task queue
   */
  std::queue<Task> tasks;

  /**
   * The vector of the threads
   */
  std::vector<std::unique_ptr<std::thread>> threads;

  /**
   * lock of task queue
   */
  std::mutex taskMutex;

  /**
   * condition var of threads
   */
  std::condition_variable taskCondVar;

  /**
   * flag of the pool is running or not
   */
  std::atomic<bool> running;

  /**
   * idle threads number
   */
  std::atomic<int> idleThreads;

  /**
   * Default constructor, privatize in singleton mode
   */
  ThreadPool() = default;

  /**
   * let the threadPool get a task and exectute it
   */
  void threadMain(long id);

  /**
   * the global, unique, instance of ThreadPool.
   *
   */
  static std::unique_ptr<ThreadPool> instance;

public:
  /**
   * Constructor of threadPool
   */
  ThreadPool(long threadNum);

  size_t getTotalThreads() { return totalThreads; }

  static void initInstance(ThreadPool *ptr);

  /**
   * visit the singleton instance
   * @return instance
   */
  static ThreadPool &getInstance();

  /**
   * deleted functions for singleton
   */
  ThreadPool(const ThreadPool &) = delete;

  ThreadPool &operator=(const ThreadPool &) = delete;

  ThreadPool &operator=(ThreadPool &&) = delete;

  ThreadPool(ThreadPool &&) = delete;

  /**
   * Destructor
   */
  ~ThreadPool();

  /**
   * add task to the task queue
   */
  template <class F, class... Args>
  auto enqueTask(F &&f, Args &&...args) -> std::future<decltype(f(args...))> {
    auto task = std::make_shared<std::packaged_task<decltype(f(args...))()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    std::future<decltype(f(args...))> future = task->get_future();
    {
      std::lock_guard<std::mutex> lock(this->taskMutex);
      this->tasks.emplace([task]() { (*task)(); });
    }
    this->taskCondVar.notify_one();
    return future;
  }
};

#endif // THREAD_POOL_H
