#ifndef MYTHREADPOOL_THREADPOOL_HPP
#define MYTHREADPOOL_THREADPOOL_HPP
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <future>
#include <condition_variable>
#include <vector>

namespace kuznetsov {

  class ThreadPool {
  public:
    using task = std::function< void() >;
    ThreadPool(size_t numThreads, size_t maxThreads);
    ~ThreadPool();
    void submit(task task);
    template< class Result >
    std::future< Result > submit(std::function< Result() > function);
  private:
    void execute(size_t);
    bool stealTasks(size_t index);
    const size_t maxThreads_;
    std::vector< std::thread > threads_;
    std::list< task > tasks_;
    std::vector< std::list< task > > threadsQueue_;
    std::vector< std::mutex > threadsQueueMutexes_;
    std::mutex mx_;
    std::condition_variable cv_;
    size_t numThreads_;
    bool stop_;
  };
}

template< class Result >
std::future< Result > kuznetsov::ThreadPool::submit(std::function< Result() > function)
{
  auto promise = std::make_shared< std::promise< Result > >();
  std::future< Result > future = promise->get_future();

  auto wrapper = [promise, function]()
  {
    try {
      promise->set_value(function());
    } catch (...) {
      promise->set_exception(std::current_exception());
    }
  };
  submit(std::move(wrapper));
  return future;
}

#endif
