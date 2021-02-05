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
package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;

import java.util.Iterator;

public class IteratorSourceOperator<E>
        extends Operator<E>
{
    private final Partition[] partitions;
    private final Iterator<E> source;

    public IteratorSourceOperator(BatchContext batchContext, Iterator<E> source, int parallelism)
    {
        super(batchContext);
        final Partition[] partitions = new Partition[parallelism];
        for (int i = 0; i < parallelism; i++) {
            partitions[i] = new Partition(i);
        }
        this.partitions = partitions;
        this.source = source;
    }

    @Override
    public Partition[] getPartitions()
    {
        return partitions;
    }

    @Override
    protected Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        return source;
    }
}
