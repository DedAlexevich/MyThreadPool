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

void kuznetsov::ThreadPool::execute(size_t index)
{
  std::list< task >& tasks = threadsQueue_[index];
  std::shared_mutex& localMutex = threadsQueueMutexes_[index];

  std::shared_lock lk(mx_);
  auto hasTasks = [this]()
  {
    if (!tasks_.empty()) {
      return true;
    }

    for (size_t i = 0; i < threadsQueue_.size(); ++i) {
      const std::shared_lock lock(threadsQueueMutexes_[i]);
      if (!threadsQueue_[i].empty()) {
        return true;
      }
    }
    return false;
  };

  auto proceed = [&]()
  {
    return stop_ || hasTasks();
  };

  do {
    cv_.wait(lk, proceed);

    {
      std::shared_lock localRead(localMutex);
      if (tasks.empty() && !tasks_.empty()) {
        localRead.unlock();
        lk.unlock();
        std::unique_lock globalWrite(mx_);
        std::unique_lock localWrite(localMutex);
        if (tasks.empty() && !tasks_.empty()) {
          const size_t count = std::max(size_t{1}, tasks_.size() / numThreads_);
          auto end = std::next(tasks_.begin(), count);
          tasks.splice(tasks.begin(), tasks_, tasks_.begin(), end);
        }
        localWrite.unlock();
        globalWrite.unlock();
        lk.lock();
      }
    }

    stealTasks(index);

    task current;
    {
      {
        std::shared_lock read(localMutex);
        if (tasks.empty()) {
          continue;
        }
      }
      std::unique_lock lock(localMutex);
      if (tasks.empty()) {
        continue;
      }

      current = std::move(tasks.front());
      tasks.pop_front();
    }

    lk.unlock();
    cv_.notify_all();

    try {
      current();
    } catch (...) {
    }

    current = nullptr;
    lk.lock();
  } while (!stop_ || hasTasks());
}

bool kuznetsov::ThreadPool::stealTasks(size_t index)
{
  std::list< task >& tasks = threadsQueue_[index];
  std::shared_mutex& localMutex = threadsQueueMutexes_[index];

  for (size_t i = 0; i < numThreads_; ++i) {
    if (i == index) {
      continue;
    }

    {
      std::shared_lock read(localMutex);
      if (!tasks.empty()) {
        return false;
      }
    }
    {
      std::shared_lock read(threadsQueueMutexes_[i]);
      if (threadsQueue_[i].empty()) {
        continue;
      }
    }
    const std::scoped_lock lock(localMutex, threadsQueueMutexes_[i]);

    if (!tasks.empty()) {
      return false;
    }

    std::list< task >& othersTasks = threadsQueue_[i];
    if (othersTasks.empty()) {
      continue;
    }

    const size_t count = std::max(size_t{1}, othersTasks.size() / 3);
    auto first = std::prev(othersTasks.end(), count);
    tasks.splice(tasks.end(), othersTasks, first, othersTasks.end());
    return true;
  }

  return false;
}
