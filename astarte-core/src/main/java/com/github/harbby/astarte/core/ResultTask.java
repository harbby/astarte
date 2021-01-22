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
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.Task;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.Operator;

import java.util.Iterator;

public class ResultTask<E, R>
        implements Task<R>
{
    private final Stage stage;
    private final Mapper<Iterator<E>, R> func;
    private final Partition partition;

    public ResultTask(
            Stage stage,
            Mapper<Iterator<E>, R> func,
            Partition partition)
    {
        this.stage = stage;
        this.func = func;
        this.partition = partition;
    }

    @Override
    public Stage getStage()
    {
        return stage;
    }

    @Override
    public int getTaskId()
    {
        return partition.getId();
    }

    @Override
    public R runTask(TaskContext taskContext)
    {
        @SuppressWarnings("unchecked")
        ResultStage<E> resultStage = (ResultStage<E>) stage;
        Operator<E> operator = resultStage.getFinalOperator();
        Iterator<E> iterator = operator.computeOrCache(partition, taskContext);
        R r = func.map(iterator);
        return r;
    }
}
