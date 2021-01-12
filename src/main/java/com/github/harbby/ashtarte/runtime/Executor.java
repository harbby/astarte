package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.api.Task;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Executor {
    private final ExecutorService pool;
    private final ConcurrentMap<Long, TaskRunner> runningTasks = new ConcurrentHashMap<>();
    private final ExecutorBackend executorBackend;

    public Executor() {
        int vcores = 2;
        pool = Executors.newFixedThreadPool(vcores);
        this.executorBackend = ExecutorBackend.getInstance();
    }

    public void runTask(Task<?> task) {
        Future<?> future = pool.submit(() -> {
            Object result = task.runTask(null);
            TaskEvent event = new TaskEvent(task.getClass(), result);
            executorBackend.updateState(event);
        });
        runningTasks.put(task.getTaskId(), new TaskRunner(task, future));
    }

    public static class TaskRunner {
        private final Task<?> task;
        private final Future<?> future;

        public TaskRunner(Task<?> task, Future<?> future) {
            this.task = task;
            this.future = future;
        }

        public Future<?> getFuture() {
            return future;
        }

        public Task<?> getTask() {
            return task;
        }
    }
}
