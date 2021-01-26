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
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectionSource<E>
        extends Operator<E>
{
    private final int parallelism;
    private final Collection<E> collection;

    public CollectionSource(BatchContext context, Collection<E> collection, int parallelism)
    {
        super(context);
        this.collection = collection;
        this.parallelism = parallelism;
    }

    @Override
    public Partition[] getPartitions()
    {
        return slice(collection, parallelism);
    }

    @Override
    public Iterator<E> compute(Partition partition, TaskContext taskContext)
    {
        ParallelCollectionPartition<E> collectionPartition = (ParallelCollectionPartition<E>) partition;
        return collectionPartition.getCollection().iterator();
    }

    private static <E> Partition[] slice(Collection<E> collection, int parallelism)
    {
        long length = collection.size();
        List<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            int start = (int) ((i * length) / parallelism);
            int end = (int) (((i + 1) * length) / parallelism);
            tuple2s.add(Tuple2.of(start, end));
        }

        List<E> list = ImmutableList.copy(collection);
        ParallelCollectionPartition[] partitions = new ParallelCollectionPartition[tuple2s.size()];
        for (int i = 0; i < partitions.length; i++) {
            Tuple2<Integer, Integer> a1 = tuple2s.get(i);
            partitions[i] = new ParallelCollectionPartition<>(i, list.subList(a1.f1(), a1.f2()));
        }
        return partitions;
    }

    private static class ParallelCollectionPartition<R>
            extends Partition
    {
        private final Collection<R> collection;

        public ParallelCollectionPartition(int index, Collection<R> collection)
        {
            super(index);
            this.collection = collection;
        }

        public Collection<R> getCollection()
        {
            return collection;
        }
    }
}
