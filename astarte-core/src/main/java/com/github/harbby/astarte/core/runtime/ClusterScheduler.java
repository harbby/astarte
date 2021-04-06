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

import com.github.harbby.astarte.core.JobScheduler;
import com.github.harbby.astarte.core.MapTaskState;
import com.github.harbby.astarte.core.ResultStage;
import com.github.harbby.astarte.core.ResultTask;
import com.github.harbby.astarte.core.ShuffleMapStage;
import com.github.harbby.astarte.core.ShuffleMapTask;
import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.Task;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ClusterScheduler
        extends JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterScheduler.class);
    private final ExecutorManager executorManager;
    private final DriverNetManager driverNetManager;

    public ClusterScheduler(AstarteConf astarteConf, int vcores, int executorNum)
    {
        // start driver manager port
        this.driverNetManager = new DriverNetManager(executorNum);
        int executorMemMb = astarteConf.getInt(Constant.EXECUTOR_MEMORY_CONF, 1024);

        //启动所有Executor
        this.executorManager = ExecutorManager.createExecutorManager(vcores, executorMemMb, executorNum, driverNetManager.getBindAddress());
        executorManager.start();

        //wait 等待所有exector上线
        try {
            driverNetManager.awaitAllExecutorRegistered();
        }
        catch (InterruptedException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public void stop()
    {
        driverNetManager.stop();
        executorManager.stop();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap)
    {
        driverNetManager.initState();

        Object[] result = null;
        //stageID, mapId,dataLength
        Map<Integer, Map<Integer, long[]>> stageMapState = new HashMap<>();
        Map<Integer, Map<Integer, InetSocketAddress>> mapTaskNotes = new HashMap<>();
        for (Stage stage : jobStages) {
            Map<Integer, long[]> currentStageState = new HashMap<>();
            stageMapState.put(stage.getStageId(), currentStageState);
            Map<Integer, InetSocketAddress> mapTaskRunningExecutor = submitStage(stage, action, mapTaskNotes, stageMap.get(stage));
            mapTaskNotes.put(stage.getStageId(), mapTaskRunningExecutor);
            if (stage instanceof ResultStage) {
                result = new Object[stage.getNumPartitions()];
            }

            //等待stage执行结束,所有task成功. 如果task失败，应重新调度一次
            //todo: 失败分为： executor挂掉, task单独失败但executor正常
            //这里采用简单的方式，先不考虑executor挂掉
            for (int taskDone = 0; taskDone < stage.getNumPartitions(); ) {
                TaskEvent taskEvent = driverNetManager.awaitTaskEvent();
                if (taskEvent.getJobId() != jobId) {
                    continue;
                }
                taskDone++;
                if (taskEvent instanceof TaskEvent.TaskFailed) {
                    TaskEvent.TaskFailed taskFailed = (TaskEvent.TaskFailed) taskEvent;
                    throw new AstarteException("task" + taskEvent.getTaskId() + " failed: " + taskFailed.getError());
                }
                checkState(taskEvent instanceof TaskEvent.TaskSuccess);
                if (stage instanceof ShuffleMapStage) {
                    TaskEvent.TaskSuccess taskSuccess = (TaskEvent.TaskSuccess) taskEvent;
                    MapTaskState mapTaskState = (MapTaskState) taskSuccess.getTaskResult();
                    currentStageState.put(mapTaskState.getMapId(), mapTaskState.getSegmentEnds());
                }
                else if (stage instanceof ResultStage) {
                    TaskEvent.TaskSuccess taskSuccess = (TaskEvent.TaskSuccess) taskEvent;
                    result[taskSuccess.getTaskId()] = taskSuccess.getTaskResult();
                }
            }
            if (stage instanceof ResultStage) {
                return Arrays.asList((R[]) result);
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }

    private <E, R> Map<Integer, InetSocketAddress> submitStage(Stage stage,
            Mapper<Iterator<E>, R> action,
            Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks,
            Map<Integer, Integer> dependStages)
    {
        List<Task<?>> tasks = new ArrayList<>();
        if (stage instanceof ShuffleMapStage) {
            logger.info("starting... shuffleMapStage: {}, stageId {}", stage, stage.getStageId());
            for (Partition partition : stage.getPartitions()) {
                Task<MapTaskState> task = new ShuffleMapTask(
                        stage.getJobId(),
                        stage.getStageId(),
                        partition, ((ShuffleMapStage) stage).getFinalOperator(),
                        dependMapTasks,
                        dependStages);
                tasks.add(task);
            }
        }
        else {
            //result stage ------
            checkState(stage instanceof ResultStage, "Unknown stage " + stage);
            logger.info("starting... ResultStage: {}, stageId {}", stage, stage.getStageId());

            for (Partition partition : stage.getPartitions()) {
                Task<R> task = new ResultTask<>(
                        stage.getJobId(),
                        stage.getStageId(),
                        (Operator<E>) stage.getFinalOperator(),
                        action, partition,
                        dependMapTasks,
                        dependStages);
                tasks.add(task);
            }
        }

        Map<Integer, InetSocketAddress> mapTaskRunningExecutor = new HashMap<>();
        tasks.forEach(task -> {
            InetSocketAddress address = driverNetManager.submitTask(task);
            mapTaskRunningExecutor.put(task.getTaskId(), address);
        });
        return mapTaskRunningExecutor;
    }
}
