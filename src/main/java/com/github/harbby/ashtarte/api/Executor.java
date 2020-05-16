package com.github.harbby.ashtarte.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Executor {
    private final ExecutorService pool;
    private final ConcurrentMap<Long, Task<?>> runningTasks = new ConcurrentHashMap<>();

    public Executor() {
        int vcores = 2;
        pool = Executors.newFixedThreadPool(vcores);
    }

    public <R> void runTask(Task<R> task) {
        runningTasks.put(task.getTaskId(), task);
        pool.submit(() -> {
            R result = task.runTask(null);
            //driver.updateState(result);
        });
    }
}
