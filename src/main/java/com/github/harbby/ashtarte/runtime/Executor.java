package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Executor
{
    private final ExecutorService pool;
    private final ConcurrentMap<Long, TaskRunner> runningTasks = new ConcurrentHashMap<>();
    private final ExecutorBackend executorBackend;

    public Executor()
            throws Exception
    {
        int vcores = 2;
        pool = Executors.newFixedThreadPool(vcores);

        ShuffleManagerService service = new ShuffleManagerService();
        SocketAddress shuffleServiceAddress = service.start();

        this.executorBackend = new ExecutorBackend(this);
        executorBackend.start();

        executorBackend.updateState(new ExecutorEvent.ExecutorInitSuccessEvent(shuffleServiceAddress));
        service.join();
    }

    public static void main(String[] args)
            throws Exception
    {
        new Executor();
    }

    public void runTask(Task<?> task)
    {
        Future<?> future = pool.submit(() -> {
            try {
                Stage stage = task.getStage();
                Set<SocketAddress> shuffleServices = stage.getShuffleServices();
                TaskContext taskContext = TaskContext.of(stage.getStageId(), stage.getDeps());
                Object result = task.runTask(taskContext);
                TaskEvent event = new TaskEvent(task.getClass(), result);
                executorBackend.updateState(event);
            }
            catch (Exception e) {
                //task failed
                e.printStackTrace();
            }
        });
        runningTasks.put(task.getTaskId(), new TaskRunner(task, future));
    }

    public static class TaskRunner
    {
        private final Task<?> task;
        private final Future<?> future;

        public TaskRunner(Task<?> task, Future<?> future)
        {
            this.task = task;
            this.future = future;
        }

        public Future<?> getFuture()
        {
            return future;
        }

        public Task<?> getTask()
        {
            return task;
        }
    }
}
