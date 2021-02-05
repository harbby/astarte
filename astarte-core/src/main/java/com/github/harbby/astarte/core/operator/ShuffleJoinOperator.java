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
import com.github.harbby.astarte.core.deprecated.JoinExperiment;
import com.github.harbby.astarte.core.runtime.ShuffleClient;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * 每个stage只需包含自己相关算子的引用。这样序列化dag时将只会包含自己相关引用
 * 以此目前Stage仅有的两个firstOperator是[ShuffledOperator, ShuffleJoinOperator]
 * 我们在[ShuffledOperator, ShuffleJoinOperator]算子里不能包含任何Operator的引用。
 * see: clearOperatorDependencies
 * <p>
 * shuffle join
 */
public class ShuffleJoinOperator<K>
        extends Operator<Tuple2<K, Iterable<?>[]>>
{
    private final Partitioner partitioner;
    private final int dataSetNum;
    private final int[] shuffleMapIds;

    private final transient List<ShuffleMapOperator<K, Object>> dependencies;

    private final Map<Integer, Encoder<Tuple2<K, Object>>> encoders = new HashMap<>();

    @SafeVarargs
    protected ShuffleJoinOperator(Partitioner partitioner, Operator<? extends Tuple2<K, ?>> leftDataSet,
            Operator<? extends Tuple2<K, ?>>... otherDataSets)
    {
        super(leftDataSet.getContext()); //不再传递依赖
        this.partitioner = requireNonNull(partitioner, "requireNonNull");
        this.dataSetNum = 1 + otherDataSets.length;
        this.dependencies = ImmutableList.copy(createShuffleMapOps(partitioner, leftDataSet, otherDataSets));
        this.shuffleMapIds = dependencies.stream().mapToInt(Operator::getId).toArray();
        for (int i = 0; i < dataSetNum; i++) {
            ShuffleMapOperator<K, Object> shuffleMapOperator = dependencies.get(i);
            encoders.put(shuffleMapOperator.getId(), shuffleMapOperator.getShuffleMapRowEncoder());
        }
    }

    private static <K> List<ShuffleMapOperator<K, Object>> createShuffleMapOps(
            Partitioner partitioner,
            Operator<? extends Tuple2<K, ?>> leftDataSet,
            Operator<? extends Tuple2<K, ?>>... otherDataSets)
    {
        requireNonNull(partitioner, "partitioner is null");
        requireNonNull(leftDataSet, "leftDataSet is null");
        checkState(otherDataSets.length > 0, "must otherDataSets.length > 0");

        return MutableList.<Operator<? extends Tuple2<K, ?>>>builder()
                .add(leftDataSet)
                .addAll(otherDataSets)
                .build()
                .stream()
                .map(x -> {
                    @SuppressWarnings("unchecked")
                    Operator<Tuple2<K, Object>> operator = (Operator<Tuple2<K, Object>>) unboxing(x);
                    return new ShuffleMapOperator<>(operator, partitioner);
                }).collect(Collectors.toList());
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
    public Iterator<Tuple2<K, Iterable<?>[]>> compute(Partition split, TaskContext taskContext)
    {
        Map<Integer, Integer> deps = taskContext.getDependStages();
        for (Integer shuffleId : deps.values()) {
            checkState(shuffleId != null, "shuffleId is null");
        }
        ShuffleClient shuffleClient = taskContext.getShuffleClient();
        Iterator<Iterator<Tuple2<K, Object>>> iterators = IntStream.of(shuffleMapIds)
                .mapToObj(operator -> {
                    int shuffleId = deps.get(operator);
                    Encoder<Tuple2<K, Object>> encoder = encoders.get(operator);
                    return shuffleClient.readShuffleData(encoder, shuffleId, split.getId());
                }).iterator();

        return JoinExperiment.join(iterators, dataSetNum);
    }
}
