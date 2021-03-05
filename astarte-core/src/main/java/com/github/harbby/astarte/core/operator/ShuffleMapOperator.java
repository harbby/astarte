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
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.ShuffleWriter;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.IOException;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * 按宽依赖将state进行划分
 * state之间串行执行
 */
public class ShuffleMapOperator<K, V>
        extends Operator<Void>
{
    private static final long serialVersionUID = 4055609687203936202L;
    private final Operator<Tuple2<K, V>> operator;
    private final Partitioner partitioner;
    private final Encoder<Tuple2<K, V>> encoder;

    public ShuffleMapOperator(
            Operator<Tuple2<K, V>> operator,
            Partitioner partitioner)
    {
        //use default HashPartitioner
        super(operator);
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        this.operator = unboxing(operator);
        Encoder<Tuple2<K, V>> rowEncoder = operator.getRowEncoder();
        if (rowEncoder == null) {
            rowEncoder = Encoders.javaEncoder();
        }
        this.encoder = rowEncoder;
    }

    public ShuffleMapOperator(Operator<Tuple2<K, V>> operator, int numReducePartitions)
    {
        //use default HashPartitioner
        this(operator, new HashPartitioner(numReducePartitions));
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
    public Iterator<Void> compute(Partition split, TaskContext taskContext)
    {
        try (ShuffleWriter<K, V> shuffleWriter = ShuffleWriter.createShuffleWriter(
                taskContext.shuffleWorkDir(),
                taskContext.getJobId(),
                taskContext.getStageId(),
                split.getId(),
                partitioner,
                encoder)) {
            Iterator<? extends Tuple2<K, V>> iterator = operator.computeOrCache(split, taskContext);
            shuffleWriter.write(iterator);
        }
        catch (IOException e) {
            throw new AstarteException("shuffle map task failed", e);
        }
        return Iterators.empty();
    }
}
