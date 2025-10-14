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
        ThreadSafeQueue(T sentinel) : sentinel(sentinel) {
            size = 0;
        };

        T pop() {
            unique_lock<mutex> lock(mtx);

            while (size == 0) {
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
                cv.notify_all();
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
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
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
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        struct SleepingTask {
            IRunnable* runnable;
            int idx;
            int num_total_tasks;
            int taskid;

            SleepingTask(IRunnable* runnable, int idx, int num_total_tasks, int taskid) {
                this->runnable = runnable;
                this->idx = idx;
                this->num_total_tasks = num_total_tasks;
                this->taskid = taskid;
            }

            bool operator==(SleepingTask& a) {
                return a.runnable == this->runnable && a.idx == this->idx && a.num_total_tasks == this->num_total_tasks;
            }
        };

        struct Task {
            IRunnable* runnable;
            int num_total_tasks;

            Task() {
                this->runnable = NULL;
                this->num_total_tasks = -1;
            }

            Task(IRunnable* runnable, int num_total_tasks) {
                this->runnable = runnable;
                this->num_total_tasks = num_total_tasks;
            }
        };
    private:
    ThreadSafeQueue<SleepingTask> q;
    int num_threads;
    thread* threads;

    mutex mtx;
    condition_variable cv;
    int num_tasks_left;

    int curId;
    unordered_set<int> completed;
    unordered_map<int, unordered_set<int>> dependencies;
    unordered_map<int, Task> tasks;
    mutex dependencies_mutex;

    unordered_map<int, atomic<int>> remaining;
    mutex remaining_mtx;

    public:

        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        static void thread_fn(TaskSystemParallelThreadPoolSleeping* self);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
