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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    done_pushing = true;
    run_ended = program_done = false;
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back([this, i] {
            while (true) {
                {
                    unique_lock<mutex> lock(this->mut);
                    this->run_start.wait(lock, [this] {
                        return !this->done_pushing || this->program_done;
                    });
                }

                if (this->program_done) {
                    return;
                }

                while (true) {
                    function<void()> job;
                    {
                        unique_lock<mutex> lock(this->mut);
                        this->cv.wait(lock, [this] {
                            return this->done_pushing || !this->jobs.empty();
                        });

                        if (this->done_pushing && this->jobs.empty()) {
                            if (!run_ended) {
                                run_ended = true;
                                run_end.notify_one();
                            }

                            break;
                        }

                        job = move(this->jobs.front());
                        this->jobs.pop();
                    }
                    cout << "running job on thread " << i << endl;
                    job();
                    cout << "finished job on thread " << i << endl;
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        unique_lock<mutex> lock(mut);
        program_done = true;
    }
    run_start.notify_all();

    for (thread &worker : workers) {
        worker.join();
    }
    cout << "all workers done" << endl;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    cout << "function called with " << num_total_tasks << " tasks"  << endl;
    {
        unique_lock<mutex> lock(mut);
        done_pushing = run_ended = false;
    }

    run_start.notify_all();
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return;
    for (int i = 0; i < num_total_tasks; i++) {
        {
            unique_lock<mutex> lock(mut);
            jobs.push([this, runnable, i, num_total_tasks] {
                runnable->runTask(i, num_total_tasks);
            });
            cout << "pushed job " << i << endl;
        }

        cv.notify_one();
    }

    {
        unique_lock<mutex> lock(mut);
        done_pushing = true;
    }

    {
        unique_lock<mutex> lock(mut);
        run_end.wait(lock, [this] {
            return this->run_ended;
        });
    }

    cout << "function done!" << endl;
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), q(SleepingTask(NULL, -1, 0)) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    this->num_threads = num_threads;
    threads = new thread[num_threads];
    for (int i =0 ; i < num_threads; i++) {
        threads[i] = thread(thread_fn, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    q.push(SleepingTask(NULL, -1, 0));

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    delete[] threads;
}

void TaskSystemParallelThreadPoolSleeping::thread_fn(TaskSystemParallelThreadPoolSleeping* self) {
    while (true) {
        SleepingTask val = self->q.pop();

        if (val.runnable == NULL) {
            return;
        }

        val.runnable->runTask(val.idx, val.num_total_tasks);

        if (self->counter.fetch_sub(1) == 1) {
            self->cv.notify_one();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    counter.store(num_total_tasks);

    for (int i = 0; i < num_total_tasks; i++) {
        q.push(SleepingTask(runnable, i, num_total_tasks));
    }

    unique_lock<mutex> lock(mtx);

    while (counter.load() != 0) {
        cv.wait(lock);
    }
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
