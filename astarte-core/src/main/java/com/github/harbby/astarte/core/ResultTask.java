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
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.Operator;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;

public class ResultTask<E, R>
        implements Task<R>
{
    private final int jobId;
    private final int stageId;
    private final Operator<E> operator;
    private final Mapper<Iterator<E>, R> func;
    private final Partition partition;
    private final Map<Integer, Map<Integer, SocketAddress>> dependMapTasks;
    private final Map<Integer, Integer> dependStages;

    public ResultTask(
            int jobId,
            int stageId,
            Operator<E> operator,
            Mapper<Iterator<E>, R> func,
            Partition partition,
            Map<Integer, Map<Integer, SocketAddress>> dependMapTasks,
            Map<Integer, Integer> dependStages)
    {
        this.jobId = jobId;
        this.stageId = stageId;
        this.func = func;
        this.partition = partition;
        this.dependMapTasks = dependMapTasks;
        this.operator = operator;
        this.dependStages = dependStages;
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
    public R runTask(TaskContext taskContext)
    {
        Iterator<E> iterator = operator.computeOrCache(partition, taskContext);
        R r = func.map(iterator);
        return r;
    }
}
