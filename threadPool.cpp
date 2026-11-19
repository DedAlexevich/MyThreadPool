#include "threadPool.hpp"
#include <algorithm>
#include <iterator>

kuznetsov::ThreadPool::ThreadPool(size_t numThreads, size_t maxThrds):
  maxThreads_(std::max(size_t{1}, maxThrds)),
  threadsQueue_(maxThreads_),
  threadsQueueMutexes_(maxThreads_),
  numThreads_(std::min(std::max(size_t{1}, numThreads), maxThreads_)),
  stop_(false)
{
  threads_.reserve(numThreads_);
  size_t index = 0;

  size_t i = 0;
  try {
    for (; i < numThreads_; ++i) {
      auto run = [this, index]()
      {
        execute(index);
      };
      threads_.emplace_back(run);
      index++;
    }
  } catch (...) {
    mx_.lock();
    stop_ = true;
    mx_.unlock();
    cv_.notify_all();

    for (size_t j = 0; j < i; ++j) {
      threads_[j].join();
    }

    throw;
  }
}

kuznetsov::ThreadPool::~ThreadPool()
{
  mx_.lock();
  stop_ = true;
  mx_.unlock();
  cv_.notify_all();
  for (auto&& thread : threads_) {
    thread.join();
  }
}

void kuznetsov::ThreadPool::submit(task task)
{
  mx_.lock();
  try {
    tasks_.push_back(std::move(task));
  } catch (...) {
    mx_.unlock();
    throw;
  }
  if (tasks_.size() > numThreads_ * 3 / 2 && numThreads_ < maxThreads_) {
    const size_t newCount = std::min(numThreads_ / 2 * 3 + 1, maxThreads_);
    size_t i = numThreads_;

    try {
      for (; i < newCount; ++i) {
        auto runner = [this, i]()
        {
          execute(i);
        };
        threads_.emplace_back(runner);
      }
    } catch (...) {
    }
    numThreads_ = i;
  }
  mx_.unlock();
  cv_.notify_one();
}

bool kuznetsov::ThreadPool::hasTasksLocked()
{
  if (!tasks_.empty()) {
    return true;
  }

  for (size_t i = 0; i < threadsQueue_.size(); ++i) {
    std::shared_lock localRead(threadsQueueMutexes_[i]);
    if (!threadsQueue_[i].empty()) {
      return true;
    }
  }

  return false;
}

void kuznetsov::ThreadPool::tryTakeBatch(size_t index, std::shared_lock< std::shared_mutex >& globalRead)
{
  std::list< task >& localQueue = threadsQueue_[index];
  std::shared_mutex& localMutex = threadsQueueMutexes_[index];

  {
    std::shared_lock localRead(localMutex);
    if (!localQueue.empty() || tasks_.empty()) {
      return;
    }
  }

  globalRead.unlock();
  {
    std::unique_lock globalWrite(mx_);
    std::unique_lock localWrite(localMutex);

    if (localQueue.empty() && !tasks_.empty()) {
      const size_t count = std::max(size_t{1}, tasks_.size() / numThreads_);
      auto end = std::next(tasks_.begin(), count);
      localQueue.splice(localQueue.end(), tasks_, tasks_.begin(), end);
    }
  }
  globalRead.lock();
}

bool kuznetsov::ThreadPool::tryPopLocal(size_t index, task& current)
{
  std::list< task >& queue = threadsQueue_[index];
  std::shared_mutex& mutex = threadsQueueMutexes_[index];

  {
    std::shared_lock read(mutex);
    if (queue.empty()) {
      return false;
    }
  }

  std::unique_lock write(mutex);
  if (queue.empty()) {
    return false;
  }

  current = std::move(queue.front());
  queue.pop_front();
  return true;
}

void kuznetsov::ThreadPool::execute(size_t index)
{
  std::shared_lock globalRead(mx_);

  do {
    cv_.wait(globalRead,
        [this]
        {
          return stop_ || hasTasksLocked();
        });

    tryTakeBatch(index, globalRead);
    stealTasks(index);

    task current;
    if (!tryPopLocal(index, current)) {
      continue;
    }

    globalRead.unlock();
    cv_.notify_all();

    try {
      current();
    } catch (...) {
    }

    current = nullptr;
    globalRead.lock();
  } while (!stop_ || hasTasksLocked());
}

bool kuznetsov::ThreadPool::stealTasks(size_t index)
{
  std::list< task >& localQueue = threadsQueue_[index];
  std::shared_mutex& localMutex = threadsQueueMutexes_[index];

  {
    std::shared_lock read(localMutex);
    if (!localQueue.empty()) {
      return false;
    }
  }

  for (size_t i = 0; i < numThreads_; ++i) {
    if (i == index) {
      continue;
    }

    std::list< task >& donorQueue = threadsQueue_[i];
    std::shared_mutex& donorMutex = threadsQueueMutexes_[i];

    {
      std::shared_lock read(donorMutex);
      if (donorQueue.empty()) {
        continue;
      }
    }

    std::scoped_lock write(localMutex, donorMutex);
    if (!localQueue.empty()) {
      return false;
    }
    if (donorQueue.empty()) {
      continue;
    }

    const size_t count = std::max(size_t{1}, donorQueue.size() / 3);
    auto first = std::prev(donorQueue.end(), count);
    localQueue.splice(localQueue.end(), donorQueue, first, donorQueue.end());
    return true;
  }

  return false;
}
