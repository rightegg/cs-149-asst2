#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <bits/stdc++.h>
using namespace std;

template <typename T>
class ThreadSafeQueue {
    mutex mtx;
    condition_variable cv;
    int size;
    T sentinel;
    std::queue<T> q;

    public:
        ThreadSafeQueue(T sentinel) {
            this->sentinel = sentinel;
        };

        T pop() {
            unique_lock<mutex> lock(mtx);

            while (size != 0) {
                cv.wait(lock);
            }

            size--;

            T ret = move(q.front());
            q.pop();

            if (ret == sentinel) {
                size++;
                q.push(sentinel);
                cv.notify_one();
            }

            lock.unlock();
            return ret;
        }

        void push(T data) {
            mtx.lock();
            q.push(data);
            size++;
            if (size == 1) {
                cv.notify_one();
            }
            mtx.unlock();
        }
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    int num_threads;
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        static void thread_fn(IRunnable* runnable, int thread_id, int num_threads, int num_total_tasks);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        bool done_pushing;
        mutex mut;
        condition_variable cv;
        vector<thread> workers;
        queue<function<void()>> jobs;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    ThreadSafeQueue<int> q;
    int num_threads;
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        static void thread_fn(TaskSystemParallelThreadPoolSleeping* self, IRunnable* runnable, int num_total_tasks);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
