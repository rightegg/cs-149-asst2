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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), program_done(false), syncing(false), job_counter(0), current_id(0) {
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this] {
            while (true) {
                Task job;
                {
                    unique_lock<mutex> lock(mut);
                    if (syncing && job_counter == 0) {
                        syncing = false;
                        jobs_done_cv.notify_one();
                    }

                    queue_empty_cv.wait(lock, [this] {
                        return program_done || !jobs.empty();
                    });

                    if (program_done && jobs.empty()) {
                        return;
                    }

                    // cout << "job size: " << jobs.size() << endl;
                    job = move(jobs.front());
                    jobs.pop();

                    if (job.idx == -1) {
                        for (int i = 0; i < job.num_total_tasks; i++) {
                            jobs.push({job.runnable, i, job.num_total_tasks, job.id});
                        }
                        // cout << "queued " << job.id << endl;
                        continue;
                    }
                }

                // cout << "started task " << job.id << ", idx " << job.idx << endl;
                if (job.runnable == nullptr) {
                    // cout << "!!! Task " << job.id << ", idx " << job.idx << " is bad" << endl;
                    assert(false);
                }
                job.runnable->runTask(job.idx, job.num_total_tasks);
                // cout << "ended task " << job.id << ", idx " << job.idx << endl;

                job_counter.fetch_sub(1);
                // cout << "now we have " << job_counter << " remaining jobs" << endl;
                // cout << "we also have " << jobs.size() << " remaining tasks" << endl;

                {
                    unique_lock<mutex> lock(mut);
                    for (int i = 0; i < current_id; i++) {
                        if (jobs_left[i] > 0 && i == 256) {
                             // cout << "job " << i << " still has " << jobs_left[i] << " tasks";
                             // cout << " and " << remaining_dependencies[i] << " deps" << endl;
                        }
                    }

                    TaskID id = job.id;
                    if (--jobs_left[id] == 0) {
                        // cout << "done with " << id << endl;
                        for (TaskID child : dependencies[id]) {
                            // cout << "child " << child << " with " << remaining_dependencies[child] << "endl";
                            if (--remaining_dependencies[child] == 0) {
                                jobs.push(tasks[child]);
                                // cout << "pushed " << child << endl;
                            }
                        }
                    }
                }
            }
        });
    }
}


TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    // cout << "Destroying..." << endl;

    program_done = true;
    queue_empty_cv.notify_all();

    for (thread &worker : workers) {
        worker.join();
    }
    workers.clear();

    // cout << "Destroyed" << endl;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    vector<TaskID> empty_deps;
    runAsyncWithDeps(runnable, num_total_tasks, empty_deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    unique_lock<mutex> lock(mut);

    TaskID id = current_id++;
    jobs_left.push_back(num_total_tasks);
    remaining_dependencies.push_back(0);
    dependencies.push_back({});
    tasks.push_back({runnable, -1, num_total_tasks, id});
    job_counter += num_total_tasks;

    bool can_start = true;
    for (TaskID parent : deps) {
        if (jobs_left[parent] > 0) {
            dependencies[parent].push_back(id);
            can_start = false;
            remaining_dependencies[id]++;
        }
    }

    if (can_start) {
        jobs.push(tasks.back());
        queue_empty_cv.notify_one();
    }


    // cout << "processed task " << id << " " << deps.size() << endl;

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    syncing = true;
    queue_empty_cv.notify_all();

    // cout << "waiting to sync..." << endl;

    unique_lock<mutex> lock(mut);
    jobs_done_cv.wait(lock, [this] {
        return !syncing;
    });
    // cout << "done syncing!" << endl;
}
