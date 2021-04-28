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
import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.runtime.ShuffleClient;
import com.github.harbby.astarte.core.utils.JoinUtil;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.ImmutableList;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public class ShuffleJoinOperator<K, V1, V2>
        extends Operator<Tuple2<K, Tuple2<V1, V2>>>
{
    private final Partitioner partitioner;
    private final JoinUtil.JoinMode joinMode;
    private final Encoder<Tuple2<K, V1>> leftEncoder;
    private final Encoder<Tuple2<K, V2>> rightEncoder;
    private final Comparator<K> comparator;
    private final int leftShuffleMapId;
    private final int rightShuffleMapId;

    private final transient List<ShuffleMapOperator<K, ?>> dependencies;

    protected ShuffleJoinOperator(Partitioner partitioner,
            JoinUtil.JoinMode joinMode,
            Operator<Tuple2<K, V1>> leftDataSet,
            Operator<Tuple2<K, V2>> rightDataSet,
            Comparator<K> comparator)
    {
        super(leftDataSet.getContext());
        this.partitioner = requireNonNull(partitioner, "requireNonNull");
        this.joinMode = requireNonNull(joinMode, "joinMode is null");

        ShuffleMapOperator<K, V1> leftShuffleMapOperator = new ShuffleMapOperator<>(unboxing(leftDataSet), partitioner, comparator, null);
        ShuffleMapOperator<K, V2> rightShuffleMapOperator = new ShuffleMapOperator<>(unboxing(rightDataSet), partitioner, comparator, null);
        this.dependencies = ImmutableList.of(leftShuffleMapOperator, rightShuffleMapOperator);
        this.leftShuffleMapId = leftShuffleMapOperator.getId();
        this.rightShuffleMapId = rightShuffleMapOperator.getId();
        this.leftEncoder = leftShuffleMapOperator.getShuffleMapRowEncoder();
        this.rightEncoder = rightShuffleMapOperator.getShuffleMapRowEncoder();
        this.comparator = leftShuffleMapOperator.getComparator();
    }

    @Override
    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    public int numPartitions()
    {
        return partitioner.numPartitions();
    }

    @Override
    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return dependencies;
    }

    @Override
    public Iterator<Tuple2<K, Tuple2<V1, V2>>> compute(Partition split, TaskContext taskContext)
    {
        int leftShuffleId = taskContext.getDependShuffleId(leftShuffleMapId);
        int rightShuffleId = taskContext.getDependShuffleId(rightShuffleMapId);
        ShuffleClient shuffleClient = taskContext.getShuffleClient();
        try {
            Iterator<Tuple2<K, V1>> left = shuffleClient.createShuffleReader(comparator, leftEncoder, leftShuffleId, split.getId());
            Iterator<Tuple2<K, V2>> right = shuffleClient.createShuffleReader(comparator, rightEncoder, rightShuffleId, split.getId());
            return JoinUtil.mergeJoin(joinMode, comparator, left, right);
        }
        catch (IOException e) {
            throw Throwables.throwsThrowable(e);
        }
    }
}
