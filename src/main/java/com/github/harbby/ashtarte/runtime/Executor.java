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
                Stage stage = task.getStage();
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
