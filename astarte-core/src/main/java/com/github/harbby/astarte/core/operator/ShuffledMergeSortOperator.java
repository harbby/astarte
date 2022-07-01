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
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

public class ShuffledMergeSortOperator<K, V>
        extends Operator<Tuple2<K, V>>
{
    private final Partitioner partitioner;
    private final Encoder<Tuple2<K, V>> encoder;
    private final Comparator<K> comparator;
    private final int shuffleMapOperatorId;
    private final transient ShuffleMapOperator<K, V> dependOperator;

    public ShuffledMergeSortOperator(ShuffleMapOperator<K, V> operator, Partitioner partitioner)
    {
        super(operator.getContext());
        this.partitioner = partitioner;
        this.dependOperator = operator;
        this.encoder = operator.getShuffleMapRowEncoder();
        this.comparator = operator.getComparator();
        this.shuffleMapOperatorId = operator.getId();
    }

    @Override
    public Partitioner getPartitioner()
    {
        return this.partitioner;
    }

    @Override
    public int numPartitions()
    {
        return this.partitioner.numPartitions();
    }

    @Override
    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public List<Operator<?>> getDependencies()
    {
        return ImmutableList.of(dependOperator);
    }

    @Override
    protected Encoder<Tuple2<K, V>> getRowEncoder()
    {
        return encoder;
    }

    @Override
    protected Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        int depShuffleId = taskContext.getDependShuffleId(shuffleMapOperatorId);
        try {
            return taskContext.getShuffleClient().createShuffleReader(comparator, encoder, depShuffleId, split.getId());
        }
        catch (IOException e) {
            throw Throwables.throwThrowable(e);
        }
    }
}
