/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.runtime;

import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Task;
import com.github.harbby.gadtry.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Executor
        implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(Executor.class);
    private final File shuffleBaseDir;
    private final ExecutorService pool;
    private final ConcurrentMap<Integer, TaskRunner> runningTasks = new ConcurrentHashMap<>();
    private final ExecutorBackend executorBackend;
    private final ShuffleManagerService shuffleService;

    public Executor(int vcores, SocketAddress driverManagerAddress)
            throws Exception
    {
        pool = Executors.newFixedThreadPool(vcores);
        this.shuffleBaseDir = new File(System.getProperty("java.io.tmpdir"), "astarte-" + UUID.randomUUID().toString());
        this.shuffleService = new ShuffleManagerService(shuffleBaseDir);
        this.executorBackend = new ExecutorBackend(this, driverManagerAddress);
        executorBackend.start(shuffleService.getShuffleServiceBindAddress());
    }

    public void join()
            throws InterruptedException
    {
        shuffleService.join();
    }

    public void runTask(Task<?> task)
    {
        shuffleService.updateCurrentJobId(task.getJobId());
        Future<?> future = pool.submit(() -> {
            try {
                Thread.currentThread().setName("astarte-task-" + task.getStageId() + "_" + task.getTaskId());
                logger.info("starting... task {}", task);
                TaskEvent event;
                try (ShuffleClient shuffleClient = ShuffleClient.getClusterShuffleClient(task.getDependMapTasks())) {
                    TaskContext taskContext = TaskContext.of(task.getJobId(), task.getStageId(), task.getDependStages(), shuffleClient, shuffleBaseDir);
                    Object result = task.runTask(taskContext);
                    event = TaskEvent.success(task.getJobId(), task.getTaskId(), result);
                }
                catch (Exception e) {
                    logger.error("task {} 执行失败", task, e);
                    String errorMsg = Throwables.getStackTraceAsString(e);
                    event = TaskEvent.failed(task.getJobId(), task.getTaskId(), errorMsg);
                }
                executorBackend.updateState(event);
                logger.info("task {} success", task);
                Thread.currentThread().setName(Thread.currentThread().getName() + "_done");
            }
            catch (Exception e) {
                //task failed
                logger.error("task failed", e);
            }
            catch (Error e) {
                //todo: executor exit code
                e.printStackTrace();
                System.exit(-1);
            }
            finally {
                runningTasks.remove(task.getTaskId());
            }
        });
        runningTasks.put(task.getTaskId(), new TaskRunner(task, future));
    }

    @Override
    public void close()
    {
        pool.shutdownNow();
        shuffleService.stop();
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
