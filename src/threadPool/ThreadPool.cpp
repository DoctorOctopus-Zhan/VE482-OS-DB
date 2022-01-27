/**
 * created by boqun 11/11/21
 */

#include "ThreadPool.h"
//#define Boqun_test
std::unique_ptr<ThreadPool> ThreadPool::instance = nullptr;

ThreadPool::ThreadPool(long threadNum) {
  // initially threadNum threads
  this->idleThreads.store(0);
  this->running.store(true);
  totalThreads = (size_t)threadNum;
  for (long i = 0; i < threadNum; i++) {
    auto func = [this, i] { threadMain(i); };
    this->threads.emplace_back(std::make_unique<std::thread>(func));
    this->idleThreads++;
  }
}

void ThreadPool::initInstance(ThreadPool *ptr) {
  ThreadPool::instance = std::unique_ptr<ThreadPool>(ptr);
}

ThreadPool &ThreadPool::getInstance() {
  if (ThreadPool::instance == nullptr) {
    std::cout
        << "Error: the singleton instance for threadPool is not initialized!"
        << std::endl;
  }
  return *instance;
}

ThreadPool::~ThreadPool() {
  std::unique_lock<std::mutex> uniqueLock(this->taskMutex);
  this->running.store(false);
  uniqueLock.unlock();
  this->taskCondVar.notify_all();
  for (auto &th : this->threads) {
    if (th->joinable()) {
      th->join();
    }
  }
}

void ThreadPool::threadMain(long id) {
#ifdef Boqun_test
  std::cout << "running thread " << id << std::endl;
#endif
  while (this->running.load()) {
    Task task;
    {
      std::unique_lock<std::mutex> lock(this->taskMutex);
      // wait until the task queue is empty or active thread is 0
      this->taskCondVar.wait(
          lock, [this] { return (!tasks.empty() || !this->running.load()); });
      // if taskqueue empty then break without waitting
      if (this->tasks.empty()) {
        break;
      }
      // if taskqueue not empty, exec task with the inactive thread
      task = std::move(tasks.front());
      tasks.pop();
    }
    this->idleThreads--;
    task();
    this->idleThreads++;
  }
  // this->taskCondVar.notify_all();
}
