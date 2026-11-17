#include <future>
#include <iostream>
#include <vector>
#include "threadPool.hpp"
#include <random>
#include <chrono>
#include <cstdint>
#include <thread>

namespace kuznetsov {
  long long shortWork(long long value)
  {
    return value * value;
  }

  long long calculate(long long value, size_t iterations)
  {
    long long result = value;

    for (size_t i = 0; i < iterations; ++i) {
      result ^= result << 13;
      result ^= result >> 7;
      result ^= result << 17;
    }

    return result % 1'000'000'007;
  }

  long long mediumWork(long long value)
  {
    return calculate(value, 20'000);
  }

  long long longWork(long long value)
  {
    return calculate(value, 500'000);
  }

  long long waitingWork(long long value)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    return value;
  }
}

int main()
{
  constexpr size_t numThreads = 4;
  constexpr size_t maxThreads = 16;
  constexpr size_t taskCount = 10000;

  std::default_random_engine eng;
  std::uniform_int_distribution< long long > dist(1, 100000);

  kuznetsov::ThreadPool threadPool{numThreads, maxThreads};
  std::vector< std::future< long long > > res;
  res.reserve(taskCount);

  for (size_t i = 0; i < taskCount; ++i) {
    const long long value = dist(eng);
    const size_t type = (i / 100) % 10;

    auto task = [value, type]()
    {
      if (type < 6) {
        return kuznetsov::shortWork(value);
      }
      if (type < 8) {
        return kuznetsov::mediumWork(value);
      }
      if (type == 8) {
        return kuznetsov::longWork(value);
      }
      return kuznetsov::waitingWork(value);
    };

    res.emplace_back(threadPool.submit< long long >(task));
  }

  for (std::future< long long >& fut : res) {
    fut.get();
  }

  return 0;
}

