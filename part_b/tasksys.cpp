#include "tasksys.h"
#include "CycleTimer.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

#define cout cout << "[" << this_thread::get_id() << "] "
#define now() CycleTimer::currentSeconds()

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), program_done(false), job_counter(0), current_id(0) {
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this] {
            vector<Task> to_push;
            while (true) {
                Task job;
                bool push = !to_push.empty();

                {
                    unique_lock<mutex> lock(main_mutex);
                    if (job_counter.load() == 0) {
                        jobs_done_cv.notify_one();
                    }

                    if (push) {
                        for (Task job : to_push) {
                            for (int i = 0; i < job.num_total_tasks; i++) {
                                jobs.push({job.runnable, i, job.num_total_tasks, job.id});
                            }
                        }
                        to_push.clear();
                    } else {
                        queue_empty_cv.wait(lock, [this] {
                            return program_done || !jobs.empty();
                        });

                        if (program_done && jobs.empty()) {
                            return;
                        }
                    }

                    job = move(jobs.front());
                    jobs.pop();
                }

                if (push) {
                    queue_empty_cv.notify_all();
                }

                job.runnable->runTask(job.idx, job.num_total_tasks);
                job_counter.fetch_sub(1);

                {
                    unique_lock<mutex> lock(dep_mutex);

                    TaskID id = job.id;
                    if (--jobs_left[id] == 0) {
                        for (TaskID child : dependencies[id]) {
                            if (--remaining_dependencies[child] == 0) {
                                to_push.push_back(tasks[child]);
                            }
                        }
                    }
                }
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
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    vector<TaskID> empty_deps;
    runAsyncWithDeps(runnable, num_total_tasks, empty_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    TaskID id = current_id++;
    bool can_start = true;

    {
        unique_lock<mutex> lock(dep_mutex);
        jobs_left.push_back(num_total_tasks);
        remaining_dependencies.push_back(0);
        dependencies.push_back({});
        tasks.push_back({runnable, -1, num_total_tasks, id});

        for (TaskID parent : deps) {
            if (jobs_left[parent] > 0) {
                dependencies[parent].push_back(id);
                remaining_dependencies[id]++;
                can_start = false;
            }
        }
    }

    {
        unique_lock<mutex> lock(main_mutex);
        job_counter += num_total_tasks;

        if (can_start) {
            for (int i = 0; i < num_total_tasks; i++) {
                jobs.push({runnable, i, num_total_tasks, id});
            }
        }
    }

    if (can_start) {
        queue_empty_cv.notify_all();
    }

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    unique_lock<mutex> lock(main_mutex);
    jobs_done_cv.wait(lock, [this] {
        return job_counter.load() == 0;
    });
}
