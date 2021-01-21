package com.github.harbby.astarte.runtime;

import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Stage;
import com.github.harbby.astarte.api.Task;
import com.github.harbby.gadtry.base.Throwables;
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
    private final ConcurrentMap<Integer, TaskRunner> runningTasks = new ConcurrentHashMap<>();
    private final ExecutorBackend executorBackend;
    private final ShuffleManagerService shuffleService;

    public Executor(int vcores)
            throws Exception
    {
        pool = Executors.newFixedThreadPool(vcores);

        this.shuffleService = new ShuffleManagerService(executorUUID);
        SocketAddress shuffleServiceAddress = shuffleService.start();

        this.executorBackend = new ExecutorBackend(this);
        executorBackend.start(shuffleServiceAddress);
    }

    public void join()
            throws InterruptedException
    {
        shuffleService.join();
    }

    public void runTask(Task<?> task)
    {
        shuffleService.updateCurrentJobId(task.getStage().getJobId());
        Future<?> future = pool.submit(() -> {
            try {
                Thread.currentThread().setName("ashtarte-task-" + task.getStage().getStageId() + "_" + task.getTaskId());
                logger.info("starting... task {}", task);
                TaskEvent event;
                Stage stage = task.getStage();
                try {
                    Set<SocketAddress> shuffleServices = stage.getShuffleServices();
                    ShuffleClient shuffleClient = ShuffleClient.getClusterShuffleClient(shuffleServices);
                    TaskContext taskContext = TaskContext.of(stage.getJobId(), stage.getStageId(), stage.getDeps(), shuffleClient, executorUUID);
                    Object result = task.runTask(taskContext);
                    event = TaskEvent.success(task.getTaskId(), result);
                }
                catch (Exception e) {
                    logger.error("task {} 执行失败", task, e);
                    String errorMsg = Throwables.getStackTraceAsString(e);
                    event = TaskEvent.failed(stage.getJobId(), errorMsg);
                }
                executorBackend.updateState(event);
                logger.info("task {} success", task);
                Thread.currentThread().setName(Thread.currentThread().getName() + "_done");
            }
            catch (Exception e) {
                //task failed
                logger.error("task failed", e);
            }
            finally {
                runningTasks.remove(task.getTaskId());
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
