#include "threadPool.hpp"

kuznetsov::ThreadPool::ThreadPool(size_t numThreads, size_t maxThrds):
  maxThreads_(std::max(1ul, maxThrds)),
  numThreads_(std::min(std::max(1ul, numThreads), maxThreads_)),
  stop_(false)
{
  threads_.reserve(numThreads_);
  auto run = [this]()
  {
    execute();
  };

  size_t i = 0;
  try {
    for (; i < numThreads_; ++i) {
      threads_.emplace_back(run);
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
    auto runner = [this]()
    {
      execute();
    };
    try {
      for (; i < newCount; ++i) {
        threads_.emplace_back(runner);
      }
    } catch (...) {
    }
    numThreads_ = i;
  }
  mx_.unlock();
  cv_.notify_one();
}

void kuznetsov::ThreadPool::execute()
{
  std::unique_lock lk(mx_);
  do {
    auto proceed = [&]()
    {
      return !tasks_.empty() || stop_;
    };
    cv_.wait(lk, proceed);
    if (tasks_.empty()) {
      continue;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop_front();
    lk.unlock();
    try {
      task();
    } catch (...) {
    }
    lk.lock();
  } while (!stop_ || !tasks_.empty());
}

