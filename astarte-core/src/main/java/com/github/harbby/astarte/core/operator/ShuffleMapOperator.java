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

import com.github.harbby.astarte.core.HashPartitioner;
import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.ShuffleWriter;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.api.function.Reducer;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static com.github.harbby.astarte.core.operator.SortShuffleWriter.MERGE_FILE_NAME;
import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * 按宽依赖将state进行划分
 * state之间串行执行
 */
public class ShuffleMapOperator<K, V>
        extends Operator<Void>
{
    private final Operator<Tuple2<K, V>> operator;
    private final Partitioner partitioner;
    private final Encoder<Tuple2<K, V>> encoder;
    private final Comparator<K> comparator;
    private final Reducer<V> combine;
    private int stageId = -1;

    public ShuffleMapOperator(
            Operator<Tuple2<K, V>> operator,
            Partitioner partitioner,
            Comparator<K> comparator,
            Reducer<V> combine)
    {
        //use default HashPartitioner
        super(operator.getContext());
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        this.operator = unboxing(operator);
        this.encoder = requireNonNull(operator.getRowEncoder(), "row Encoder is null");
        this.comparator = requireNonNull(comparator, "k comparator is null");
        this.combine = combine;
    }

    public ShuffleMapOperator(
            Operator<Tuple2<K, V>> operator,
            int numPartitions,
            Comparator<K> comparator,
            Reducer<V> combine)
    {
        this(operator, new HashPartitioner(numPartitions), comparator, combine);
    }

    public void setStageId(int stageId)
    {
        this.stageId = stageId;
    }

    public Integer getStageId()
    {
        checkState(stageId != -1, "ShuffleMapOperator " + this + " not set StageId");
        return stageId;
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return ImmutableList.of(operator);
    }

    @Override
    protected Encoder<Void> getRowEncoder()
    {
        throw new UnsupportedOperationException("use getShuffleMapRowEncoder()");
    }

    protected Encoder<Tuple2<K, V>> getShuffleMapRowEncoder()
    {
        return encoder;
    }

    public Comparator<K> getComparator()
    {
        return comparator;
    }

    @Override
    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    public DataSet<Void> cache(CacheManager.CacheMode cacheMode)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unCache()
    {
        throw new UnsupportedOperationException();
    }

    public Operator<Tuple2<K, V>> getOperator()
    {
        return operator;
    }

    public ByteBuffer doMapTask(Partition partition, TaskContext taskContext)
    {
        String filePrefix = String.format("shuffle_%s_%s_", taskContext.getStageId(), partition.getId());
        File shuffleWorkDir = new File(taskContext.shuffleWorkDir(), String.valueOf(taskContext.getJobId()));
        try (ShuffleWriter<K, V> shuffleWriter = new SortShuffleWriter<>(shuffleWorkDir, filePrefix,
                String.format(MERGE_FILE_NAME, taskContext.getStageId(), partition.getId()),
                partitioner, encoder, comparator, combine)) {
            Iterator<? extends Tuple2<K, V>> iterator = operator.computeOrCache(partition, taskContext);
            return shuffleWriter.write(iterator);
        }
        catch (IOException e) {
            throw new AstarteException("shuffle map task failed", e);
        }
    }

    @Override
    public Iterator<Void> compute(Partition split, TaskContext taskContext)
    {
        throw new UnsupportedOperationException();
    }
}
