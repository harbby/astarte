package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Executor
{
    private static final Logger logger = LoggerFactory.getLogger(Executor.class);
    private final String executorUUID = UUID.randomUUID().toString();
    private final ExecutorService pool;
    private final ConcurrentMap<Long, TaskRunner> runningTasks = new ConcurrentHashMap<>();
    private final ExecutorBackend executorBackend;

    public Executor()
            throws Exception
    {
        int vcores = 2;
        pool = Executors.newFixedThreadPool(vcores);

        ShuffleManagerService service = new ShuffleManagerService(executorUUID);
        SocketAddress shuffleServiceAddress = service.start();

        this.executorBackend = new ExecutorBackend(this);
        executorBackend.start(shuffleServiceAddress);
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
                Thread.currentThread().setName("ashtarte-task-" + task.getStage().getStageId()+"_"+ task.getTaskId());
                logger.info("starting... task {}", task);
                TaskEvent event;

                try {
                    Stage stage = task.getStage();
                    Set<SocketAddress> shuffleServices = stage.getShuffleServices();
                    ShuffleClientManager shuffleClient = ShuffleClientManager.start(shuffleServices);

                    TaskContext taskContext = TaskContext.of(stage.getStageId(), stage.getDeps(), shuffleClient, executorUUID);
                    Object result = task.runTask(taskContext);
                    event = new TaskEvent(task.getClass(), result);
                }
                catch (Exception e) {
                    event = new TaskEvent(task.getClass(), null);
                }
                executorBackend.updateState(event);
                logger.info("task {} success", task);
                Thread.currentThread().setName(Thread.currentThread().getName() + "_done");
            }
            catch (Exception e) {
                //task failed
                logger.error("task failed", e);
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
