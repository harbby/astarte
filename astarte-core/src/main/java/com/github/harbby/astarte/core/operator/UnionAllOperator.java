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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class UnionAllOperator<E>
        extends Operator<E>
{
    private final Operator<E>[] kvDataSets;

    @SuppressWarnings("unchecked")
    @SafeVarargs
    protected UnionAllOperator(Operator<E>... kvDataSets)
    {
        super(kvDataSets);
        this.kvDataSets = (Operator<E>[]) unboxing(kvDataSets);
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

    @Override
    public Partition[] getPartitions()
    {
        int i = 0;
        List<Partition> partitions = new ArrayList<>();
        for (Operator<? extends E> operator : kvDataSets) {
            for (Partition partition : operator.getPartitions()) {
                Partition unionAllPartition = new UnionAllPartition(i, operator.getId(), partition);
                partitions.add(unionAllPartition);
                i++;
            }
        }
        return partitions.toArray(new Partition[0]);
    }

    @Override
    public int numPartitions()
    {
        return Stream.of(kvDataSets)
                .mapToInt(x -> x.numPartitions())
                .sum();
    }

    @Override
    protected Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        UnionAllPartition unionAllPartition = (UnionAllPartition) split;
        for (Operator<E> operator : kvDataSets) {
            if (unionAllPartition.operatorId == operator.getId()) {
                return operator.computeOrCache(unionAllPartition.partition, taskContext);
            }
        }
        throw new IllegalStateException();
    }
}
