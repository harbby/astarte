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
import com.github.harbby.astarte.core.api.DataSetSource;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Split;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class DataSourceOperator<E>
        extends Operator<E>
{
    private final DataSetSource<E> dataSetSource;
    private final transient Partition[] partitions;

    public DataSourceOperator(BatchContext context, DataSetSource<E> dataSetSource, int parallelism)
    {
        super(context);
        this.dataSetSource = requireNonNull(dataSetSource, "dataSetSource is null");
        Split[] splits = dataSetSource.trySplit(parallelism);
        checkState(parallelism == -1 || splits.length <= parallelism);

        this.partitions = new Partition[splits.length];
        for (int i = 0; i < splits.length; i++) {
            partitions[i] = new DataSourcePartition(i, splits[i]);
        }
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return Collections.emptyList();
    }

    @Override
    public Partition[] getPartitions()
    {
        return partitions;
    }

    @Override
    public int numPartitions()
    {
        return partitions.length;
    }

    @Override
    protected Iterator<E> compute(Partition partition, TaskContext taskContext)
    {
        DataSourcePartition collectionPartition = (DataSourcePartition) partition;
        return dataSetSource.phyPlan(collectionPartition.split);
    }

    private static class DataSourcePartition
            extends Partition
    {
        private final Split split;

        public DataSourcePartition(int id, Split split)
        {
            super(id);
            this.split = split;
        }

        public Split getSplit()
        {
            return split;
        }
    }
}
