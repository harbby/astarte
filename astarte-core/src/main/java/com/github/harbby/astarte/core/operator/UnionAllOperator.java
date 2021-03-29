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

import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.gadtry.collection.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class UnionAllOperator<E>
        extends Operator<E>
{
    private final Map<Integer, Operator<E>> dataSets;
    private final Partition[] partitions;

    @SafeVarargs
    protected UnionAllOperator(Operator<E>... inputs)
    {
        super(inputs[0].getContext());
        this.dataSets = Stream.of(inputs).map(Operator::unboxing).collect(Collectors.toMap(Operator::getId, v -> v));

        List<Partition> partitions = new ArrayList<>();
        for (Operator<? extends E> operator : this.dataSets.values()) {
            for (Partition partition : operator.getPartitions()) {
                Partition unionAllPartition = new UnionAllPartition(partitions.size(), operator.getId(), partition);
                partitions.add(unionAllPartition);
            }
        }
        this.partitions = partitions.toArray(new Partition[0]);
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return ImmutableList.copy(dataSets.values());
    }

    @Override
    public Partition[] getPartitions()
    {
        return partitions;
    }

    @Override
    public int numPartitions()
    {
        return partitions.length;
    }

    @Override
    protected Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        UnionAllPartition unionAllPartition = (UnionAllPartition) split;
        Operator<E> operator = requireNonNull(dataSets.get(unionAllPartition.operatorId));
        return operator.computeOrCache(unionAllPartition.partition, taskContext);
    }

    public static class UnionAllPartition
            extends Partition
    {
        private final int operatorId;
        private final Partition partition;

        public UnionAllPartition(int index, int operatorId, Partition partition)
        {
            super(index);
            this.operatorId = operatorId;
            this.partition = partition;
        }
    }
}
