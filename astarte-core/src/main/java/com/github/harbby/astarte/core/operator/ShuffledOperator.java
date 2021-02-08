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
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.runtime.ShuffleClient;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * 每个stage只需包含自己相关算子的引用。这样序列化dag时将只会包含自己相关引用
 * 以此目前Stage仅有的两个firstOperator是[ShuffledOperator, ShuffleJoinOperator]
 * 我们在[ShuffledOperator, ShuffleJoinOperator]算子里不能包含任何Operator的引用.
 * see: clearOperatorDependencies
 *
 * <p>
 * shuffle Reducer reader
 */
public class ShuffledOperator<K, V>
        extends Operator<Tuple2<K, V>>
{
    private final Partitioner partitioner;
    private final int shuffleMapOperatorId;

    /**
     * 清理ShuffledOperator和ShuffleJoinOperator的Operator依赖
     * 清理依赖后,每个stage将只包含自己相关的Operator引用
     * <p>
     * 为什么要清理? 如果不清理，序列化stage时则会抛出StackOverflowError
     * demo: pageRank demo迭代数可以超过120了,不清理则会抛出StackOverflowError
     */
    private final transient Operator<?> dependOperator;

    private final Encoder<Tuple2<K, V>> encoder;

    public ShuffledOperator(ShuffleMapOperator<K, V> operator, Partitioner partitioner)
    {
        super(operator.getContext()); //不再传递依赖
        this.shuffleMapOperatorId = operator.getId();
        this.partitioner = partitioner;
        this.dependOperator = operator;
        this.encoder = operator.getShuffleMapRowEncoder();
    }

    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    protected Encoder<Tuple2<K, V>> getRowEncoder()
    {
        return encoder;
    }

    @Override
    public Partitioner getPartitioner()
    {
        // ShuffledOperator在设计中应该为一切shuffle的后端第一个Operator
        //这里我们提供明确的Partitioner给后续Operator
        return partitioner;
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return ImmutableList.of(dependOperator);
    }

    @Override
    public int numPartitions()
    {
        return partitioner.numPartitions();
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Integer shuffleId = taskContext.getDependStages().get(shuffleMapOperatorId);
        checkState(shuffleId != null);
        ShuffleClient shuffleClient = taskContext.getShuffleClient();
        return shuffleClient.readShuffleData(encoder, shuffleId, split.getId());
    }
}
