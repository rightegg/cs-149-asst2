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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), q(SleepingTask(NULL, -1, 0, -1)) {
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
    q.push(SleepingTask(NULL, -1, 0, -1));

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    delete[] threads;
    cout <<" hmm " << endl;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelThreadPoolSleeping::thread_fn(TaskSystemParallelThreadPoolSleeping* self) {
    while (true) {
        SleepingTask val = self->q.pop();
        cout << "got task " << endl;

        if (val.runnable == NULL) {
            return;
        }

        int id = val.taskid;

        val.runnable->runTask(val.idx, val.num_total_tasks);
        cout << "waiting on remaining_mtx lock " << endl;
        self->remaining_mtx.lock();
        cout << "got remaining_mtx lock " << endl;
        if (--(self->remaining[id]) == 0) {
            cout << "done! " << endl;
            self->mtx.lock();
            cout << "got main mtx " <<endl;
            if (--self->num_tasks_left == 0) {
                cout << "notiying " << endl;
                 self->cv.notify_all();
            }
            cout << "unlocking main mtx " << endl;
            self->mtx.unlock();

            self->dependencies_mutex.lock();
            cout << "got dependencies mtx " << endl;
            for (auto it = self->dependencies.begin(); it != self->dependencies.end();) {
                it->second.erase(id);

                if (it->second.size() == 0) {
                    Task t = self->tasks[it->first];
                    int nextid = it->first;
                    it = self->dependencies.erase(it);
                    for (int i = 0; i < t.num_total_tasks; i++) {
                        self->q.push(SleepingTask(t.runnable, i, t.num_total_tasks, nextid));
                    }
                } else {
                    ++it;
                }
            }
            self->dependencies_mutex.unlock();
        }

        self->remaining_mtx.unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    mtx.lock();
    int id = curId++;
    num_tasks_left++;
    mtx.unlock();

    remaining_mtx.lock();
    remaining[id] = num_total_tasks;
    remaining_mtx.unlock();

    unordered_set<int> thedeps;
    dependencies_mutex.lock();
    for (int i : deps) {
        if (completed.find(i) != completed.end()) {
            continue;
        }

        thedeps.insert(i);
    }

    if (thedeps.size() == 0) {
        cout << "pushing in main " << endl;
        for (int i = 0; i < num_total_tasks; i++) {
            cout << "push " << endl;
            q.push(SleepingTask(runnable, i, num_total_tasks, id));
        }
    } else {
        dependencies[id] = move(thedeps);
        Task t(runnable, num_total_tasks);
        tasks[id] = t;
    }

    dependencies_mutex.unlock();

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    unique_lock<mutex> lock(mtx);

    while (num_tasks_left != 0) {
        cout << "waiting on sync lock" << endl;
        cv.wait(lock);
    }
    cout << "DONE DIOFJ ODIJFO AISJD OJ " << endl;
    return;
}
