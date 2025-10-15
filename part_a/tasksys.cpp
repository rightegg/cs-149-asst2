#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::thread_fn(IRunnable* runnable, int thread_id, int num_threads, int num_total_tasks) {
    for (int i = thread_id; i < num_total_tasks; i += num_threads) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    thread threads[num_threads];

    for (int i =0 ; i < num_threads; i++) {
        threads[i] = thread(TaskSystemParallelSpawn::thread_fn, runnable, i, num_threads, num_total_tasks);
    }

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), program_done(false) {
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this] {
            while (!program_done) {
                Task job;
                bool found_job = false;

                {
                    unique_lock<mutex> lock(mut);
                    if (!jobs.empty()) {
                        job = move(jobs.front());
                        jobs.pop();
                        found_job = true;
                    }
                }

                if (found_job) {
                    job.runnable->runTask(job.idx, job.num_total_tasks);

                    {
                        unique_lock<mutex> lock(mut);
                        if (counter.fetch_sub(1) == 1) {
                            cv.notify_one();
                        }
                    }
                } else {
                    this_thread::yield();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    program_done = true;

    for (thread &worker : workers) {
        worker.join();
    }
    workers.clear();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    {
        unique_lock<mutex> lock(mut);

        counter.store(num_total_tasks);
        for (int i = 0; i < num_total_tasks; i++) {
            jobs.push({runnable, i, num_total_tasks});
        }
    }

    unique_lock<mutex> lock(mut);
    cv.wait(lock, [this] {
        return counter.load() == 0;
    });
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), program_done(false) {
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this] {
            while (true) {
                Task job;
                {
                    unique_lock<mutex> lock(mut);
                    if (counter == 0) {
                        jobs_done_cv.notify_one();
                    }

                    queue_empty_cv.wait(lock, [this] {
                        return program_done || !jobs.empty();
                    });

                    if (program_done && jobs.empty()) {
                        return;
                    }

                    job = move(jobs.front());
                    jobs.pop();
                }

                job.runnable->runTask(job.idx, job.num_total_tasks);
                counter.fetch_sub(1);
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    program_done = true;
    queue_empty_cv.notify_all();

    for (thread &worker : workers) {
        worker.join();
    }
    workers.clear();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    counter.store(num_total_tasks);
    {
        unique_lock<mutex> lock(mut);
        for (int i = 0; i < num_total_tasks; i++) {
            jobs.push({runnable, i, num_total_tasks});
        }
    }
    queue_empty_cv.notify_all();

    unique_lock<mutex> lock(mut);
    jobs_done_cv.wait(lock, [this] {
        return counter.load() == 0;
    });

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
