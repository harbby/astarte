package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

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
    {
        int vcores = 2;
        pool = Executors.newFixedThreadPool(vcores);
        this.executorBackend = new ExecutorBackend(this);

        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception
                    {
                        ch.pipeline().addLast(executorBackend);
                    }
                });
        bootstrap.connect("localhost", 7079);
    }

    public static void main(String[] args)
    {
        new Executor();
    }

    public void runTask(Task<?> task)
    {
        Future<?> future = pool.submit(() -> {
            try {
                Thread.currentThread().setName("ashtarte-task-" + task.getTaskId());
                logger.info("starting... task {}", task);
                TaskEvent event;
                try {
                    Stage stage = task.getStage();
                    Set<SocketAddress> shuffleServices = stage.getShuffleServices();
                    ShuffleClientManager shuffleClient = new ShuffleClientManager();
                    shuffleClient.start(shuffleServices);
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
            catch (Throwable e) {
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
