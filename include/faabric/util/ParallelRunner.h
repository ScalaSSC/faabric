#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace faabric::util {

/**
 * Runs a batch of tasks side by side and returns once all have run: what
 * spawning a thread per task and joining them all does, on threads kept for
 * the purpose instead of created and joined every time. The last task runs
 * on the calling thread, the others on the kept ones, of which there are as
 * many as the largest batch so far needed. Tasks must not throw.
 *
 * Meant for one caller at a time; with more, each still returns only once
 * its own tasks have run.
 */
class ParallelRunner
{
  public:
    ParallelRunner() = default;

    ParallelRunner(const ParallelRunner&) = delete;
    ParallelRunner& operator=(const ParallelRunner&) = delete;

    ~ParallelRunner()
    {
        {
            std::lock_guard<std::mutex> lock(mx);
            stopping = true;
        }
        taskReady.notify_all();
        for (auto& thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    void run(std::vector<std::function<void()>>& tasks)
    {
        if (tasks.empty()) {
            return;
        }
        size_t others = tasks.size() - 1;
        if (others > 0) {
            std::lock_guard<std::mutex> lock(mx);
            while (threads.size() < others) {
                threads.emplace_back([this]() { loop(); });
            }
            for (size_t i = 0; i < others; i++) {
                queue.push_back(&tasks[i]);
            }
            pending += others;
        }
        if (others > 0) {
            taskReady.notify_all();
        }
        tasks.back()();
        if (others > 0) {
            std::unique_lock<std::mutex> lock(mx);
            allDone.wait(lock, [this]() { return pending == 0; });
        }
    }

  private:
    std::mutex mx;
    std::condition_variable taskReady;
    std::condition_variable allDone;
    // Tasks not yet picked up, owned by the caller of run, which waits for
    // them.
    std::deque<std::function<void()>*> queue;
    size_t pending = 0;
    bool stopping = false;
    std::vector<std::thread> threads;

    void loop()
    {
        std::unique_lock<std::mutex> lock(mx);
        while (true) {
            taskReady.wait(lock,
                           [this]() { return stopping || !queue.empty(); });
            if (queue.empty()) {
                // Stopping, with nothing left to run.
                return;
            }
            auto* task = queue.front();
            queue.pop_front();
            lock.unlock();
            (*task)();
            lock.lock();
            if (--pending == 0) {
                allDone.notify_all();
            }
        }
    }
};
}
