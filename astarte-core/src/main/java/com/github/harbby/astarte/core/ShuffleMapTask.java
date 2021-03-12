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
package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Task;
import com.github.harbby.astarte.core.operator.ShuffleMapOperator;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

public class ShuffleMapTask
        implements Task<MapTaskState>
{
    private final int jobId;
    private final int stageId;
    private final Partition partition;
    private final ShuffleMapOperator<?, ?> shuffleMapOperator;
    private final Map<Integer, Map<Integer, SocketAddress>> dependMapTasks;
    private final Map<Integer, Integer> dependStages;

    public ShuffleMapTask(int jobId,
            int stageId,
            Partition partition,
            ShuffleMapOperator<?, ?> shuffleMapOperator,
            Map<Integer, Map<Integer, SocketAddress>> dependMapTasks,
            Map<Integer, Integer> dependStages)
    {
        this.jobId = jobId;
        this.stageId = stageId;
        this.partition = partition;
        this.shuffleMapOperator = shuffleMapOperator;
        this.dependMapTasks = dependMapTasks;
        this.dependStages = dependStages;
    }

    @Override
    public Map<Integer, Map<Integer, SocketAddress>> getDependMapTasks()
    {
        return dependMapTasks;
    }

    @Override
    public Map<Integer, Integer> getDependStages()
    {
        return dependStages;
    }

    @Override
    public int getTaskId()
    {
        return partition.getId();
    }

    @Override
    public MapTaskState runTask(TaskContext taskContext)
    {
        ByteBuffer header = shuffleMapOperator.doMapTask(partition, taskContext);
        return new MapTaskState(header, partition.getId());
    }

    @Override
    public int getJobId()
    {
        return jobId;
    }

    @Override
    public int getStageId()
    {
        return stageId;
    }
}
