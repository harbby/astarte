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
import com.github.harbby.gadtry.base.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CollectionSource<E>
        extends Operator<E>
{
    private final Partition[] split;

    public CollectionSource(BatchContext context, List<E> collection, int parallelism)
    {
        super(context);
        this.split = slice(collection, parallelism);
    }

    @Override
    public Partition[] getPartitions()
    {
        return split;
    }

    @Override
    public int numPartitions()
    {
        return split.length;
    }

    @Override
    public Iterator<E> compute(Partition partition, TaskContext taskContext)
    {
        @SuppressWarnings("unchecked")
        ParallelCollectionPartition<E> collectionPartition = (ParallelCollectionPartition<E>) partition;
        return collectionPartition.getIterator();
    }

    private static Partition[] slice(List<?> data, int parallelism)
    {
        long length = data.size();
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            int start = (int) ((i * length) / parallelism);
            int end = (int) (((i + 1) * length) / parallelism);
            if (end > start) {
                partitions.add(new ParallelCollectionPartition<>(partitions.size(), data.subList(start, end).toArray()));
            }
        }
        return partitions.toArray(new Partition[0]);
    }

    private static class ParallelCollectionPartition<R>
            extends Partition
    {
        private final R[] data;

        public ParallelCollectionPartition(int id, R[] data)
        {
            super(id);
            this.data = data;
        }

        public Iterator<R> getIterator()
        {
            return Iterators.of(data);
        }
    }
}
