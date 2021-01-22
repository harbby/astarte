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
import com.github.harbby.astarte.core.deprecated.JoinExperiment;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * pre-shuffle join
 */
public class LocalJoinOperator<K>
        extends Operator<Tuple2<K, Iterable<?>[]>>
{
    private final Operator<Tuple2<K, Object>>[] kvDataSets;

    @SuppressWarnings("unchecked")
    @SafeVarargs
    protected LocalJoinOperator(Operator<? extends Tuple2<K, ?>> leftDataSet,
            Operator<? extends Tuple2<K, ?>>... otherDataSets)
    {
        super(requireNonNull(leftDataSet, "leftDataSet is null").getContext());
        checkState(otherDataSets.length > 0, "must otherDataSets.length > 0");
        this.kvDataSets = MutableList.<Operator<? extends Tuple2<K, ?>>>builder()
                .add(leftDataSet)
                .addAll(otherDataSets)
                .build()
                .stream()
                .map(x -> {
                    checkState(Objects.equals(leftDataSet.getPartitioner(), x.getPartitioner()));
                    return unboxing(x);
                }).toArray(Operator[]::new);
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return Arrays.asList(kvDataSets);
    }

    @Override
    public Partition[] getPartitions()
    {
        return kvDataSets[0].getPartitions();
    }

    @Override
    public int numPartitions()
    {
        return kvDataSets[0].numPartitions();
    }

    /**
     * 可能存在return null
     */
    @Override
    public Partitioner getPartitioner()
    {
        return kvDataSets[0].getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, Iterable<?>[]>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Iterator<Tuple2<K, Object>>> iterators = Stream.of(kvDataSets)
                .map(operator -> operator.computeOrCache(split, taskContext))
                .iterator();
        return JoinExperiment.join(iterators, kvDataSets.length);
    }
}
