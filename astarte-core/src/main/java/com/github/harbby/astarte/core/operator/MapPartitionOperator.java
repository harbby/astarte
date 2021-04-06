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

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.KvMapper;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.gadtry.collection.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class MapPartitionOperator<I, O>
        extends Operator<O>
{
    private final Mapper<Iterator<I>, Iterator<O>> flatMapper;
    private final KvMapper<Integer, Iterator<I>, Iterator<O>> flatMapperWithId;
    private final Operator<I> dataSet;
    private final boolean holdPartitioner;

    protected MapPartitionOperator(Operator<I> dataSet,
            Mapper<Iterator<I>, Iterator<O>> f,
            boolean holdPartitioner)
    {
        super(dataSet.getContext());
        this.flatMapper = requireNonNull(f, "f is null");
        this.flatMapperWithId = null;

        this.dataSet = unboxing(dataSet);
        this.holdPartitioner = holdPartitioner;
    }

    protected MapPartitionOperator(Operator<I> dataSet,
            KvMapper<Integer, Iterator<I>, Iterator<O>> f,
            boolean holdPartitioner)
    {
        super(dataSet.getContext());
        this.flatMapper = null;
        this.flatMapperWithId = requireNonNull(f, "f is null");
        this.dataSet = unboxing(dataSet);
        this.holdPartitioner = holdPartitioner;
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return ImmutableList.of(dataSet);
    }

    @Override
    public Partitioner getPartitioner()
    {
        if (holdPartitioner) {
            return dataSet.getPartitioner();
        }
        return null;
    }

    @Override
    public Iterator<O> compute(Partition split, TaskContext taskContext)
    {
        Iterator<O> iterator;

        if (flatMapper != null) {
            iterator = flatMapper.map(dataSet.computeOrCache(split, taskContext));
        }
        else {
            iterator = flatMapperWithId.map(split.getId(), dataSet.computeOrCache(split, taskContext));
        }
        return requireNonNull(iterator, "MapPartition function return null,your use Iterators.empty()");
    }
}
