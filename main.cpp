#include <future>
#include <iostream>
#include <vector>
#include "threadPool.hpp"

#include <random>

namespace kuznetsov {
  long long work(long long i)
  {
    return i * i;
  }
}

int main()
{
  constexpr size_t numThreads = 4;
  constexpr size_t maxThreads = 16;
  constexpr size_t taskCount = 10'000'000;

  std::default_random_engine eng;
  std::uniform_int_distribution< long long > dist(1, 100000);

  kuznetsov::ThreadPool threadPool{numThreads, maxThreads};
  std::vector< std::future< long long > > res;
  res.reserve(taskCount);

  for (size_t i = 0; i < taskCount; ++i) {
    const long long value = dist(eng);
    auto task = [value]()
    {
      return kuznetsov::work(value);
    };
    res.emplace_back(threadPool.submit< long long >(task));
  }

  for (std::future< long long >& fut : res) {
    std::cout << fut.get() << "\n";
  }

  return 0;
}

