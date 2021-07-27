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
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.utils.ReduceUtil;
import com.github.harbby.gadtry.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * pre-shuffle join
 */
public class LocalJoinOperator<K, V1, V2>
        extends Operator<Tuple2<K, Tuple2<V1, V2>>>
{
    protected final Operator<Tuple2<K, V1>> leftDataSet;
    protected final Operator<Tuple2<K, V2>> rightDataSet;
    protected final ReduceUtil.JoinMode joinMode;
    protected final Comparator<K> comparator;
    private final Function2<Partition, TaskContext, Iterator<Tuple2<K, Tuple2<V1, V2>>>> physicalPlan;

    protected LocalJoinOperator(ReduceUtil.JoinMode joinMode,
            Operator<Tuple2<K, V1>> leftDataSet,
            Operator<Tuple2<K, V2>> rightDataSet,
            Comparator<K> comparator)
    {
        super(requireNonNull(leftDataSet, "leftDataSet is null").getContext());

        this.joinMode = requireNonNull(joinMode, "joinMode is null");
        this.leftDataSet = unboxing(leftDataSet);
        this.rightDataSet = unboxing(rightDataSet);
        this.comparator = requireNonNull(comparator, "comparator is null");
        checkState(Objects.equals(leftDataSet.getPartitioner(), rightDataSet.getPartitioner()));
        this.physicalPlan = optimizerPlan(leftDataSet, rightDataSet, joinMode, comparator);
    }

    public static <K, V1, V2> Function2<Partition, TaskContext, Iterator<Tuple2<K, Tuple2<V1, V2>>>> optimizerPlan(
            Operator<Tuple2<K, V1>> leftDataSet,
            Operator<Tuple2<K, V2>> rightDataSet,
            ReduceUtil.JoinMode joinMode,
            Comparator<K> comparator)
    {
        if ((Object) leftDataSet == rightDataSet) {
            return (partition, taskContext) -> {
                Iterator<Tuple2<K, V1>> left = leftDataSet.computeOrCache(partition, taskContext);
                return ReduceUtil.sameJoin(left);
            };
        }
        List<? extends Operator<?>> leftOperators = getOperatorStageDependencies(leftDataSet);
        List<? extends Operator<?>> rightOperators = getOperatorStageDependencies(rightDataSet);
        Optional<Operator<?>> sameOperator = findLastSameOperator(leftOperators, rightOperators);
        if (!sameOperator.isPresent()) {
            return (partition, taskContext) -> {
                Iterator<Tuple2<K, V1>> left = leftDataSet.computeOrCache(partition, taskContext);
                Iterator<Tuple2<K, V2>> right = rightDataSet.computeOrCache(partition, taskContext);
                return ReduceUtil.mergeJoin(joinMode, comparator, left, right);
            };
        }
        List<CalcOperator<?, ?>> leftCalcOperators = leftOperators.subList(0, leftOperators.indexOf(sameOperator.get())).stream()
                .map(x -> ((CalcOperator<?, ?>) x)).collect(Collectors.toList());
        Collections.reverse(leftCalcOperators); // 倒序排列
        List<CalcOperator<?, ?>> rightCalcOperators = rightOperators.subList(0, rightOperators.indexOf(sameOperator.get())).stream()
                .map(x -> ((CalcOperator<?, ?>) x)).collect(Collectors.toList());
        Collections.reverse(rightCalcOperators); // 倒序排列

        Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V1>>> leftCalc = it -> CalcOperator.doCodeGen(it, leftCalcOperators);
        Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V2>>> rightCalc = it -> CalcOperator.doCodeGen(it, rightCalcOperators);

        @SuppressWarnings("unchecked")
        Operator<Tuple2<K, ?>> operator = (Operator<Tuple2<K, ?>>) sameOperator.get();
        return (partition, taskContext) -> ReduceUtil.sameJoin(operator.computeOrCache(partition, taskContext), leftCalc, rightCalc);
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        if ((Object) leftDataSet == rightDataSet) {
            return Collections.singletonList(leftDataSet);
        }
        return Arrays.asList(leftDataSet, rightDataSet);
    }

    @Override
    public Partition[] getPartitions()
    {
        return leftDataSet.getPartitions();
    }

    @Override
    public int numPartitions()
    {
        return leftDataSet.numPartitions();
    }

    /**
     * 可能存在return null
     */
    @Override
    public Partitioner getPartitioner()
    {
        return leftDataSet.getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, Tuple2<V1, V2>>> compute(Partition partition, TaskContext taskContext)
    {
        return physicalPlan.apply(partition, taskContext);
    }

    public static List<Operator<?>> getOperatorStageDependencies(Operator<?> operator)
    {
        List<Operator<?>> result = new ArrayList<>();
        result.add(operator);
        if (operator instanceof ShuffledMergeSortOperator) {
            return result;
        }
        List<? extends Operator<?>> deps = operator.getDependencies();
        while (true) {
            if (deps.size() != 1) {
                return result;
            }
            Operator<?> child = deps.get(0);
            result.add(child);
            if (!(child instanceof CalcOperator) || !((CalcOperator<?, ?>) child).holdPartitioner()) {
                return result;
            }
            deps = child.getDependencies();
        }
    }

    public static Optional<Operator<?>> findLastSameOperator(
            List<? extends Operator<?>> leftOperators,
            List<? extends Operator<?>> rightOperators)
    {
        Iterator<? extends Operator<?>> leftIterator = leftOperators.iterator();
        Iterator<? extends Operator<?>> rightIterator = rightOperators.iterator();
        Operator<?> right = rightIterator.next();
        while (leftIterator.hasNext()) {
            Operator<?> left = leftIterator.next();
            if (left.getId() < right.getId()) {
                while (rightIterator.hasNext()) {
                    right = rightIterator.next();
                    if (right.getId() <= left.getId()) {
                        break;
                    }
                }
            }
            if (left.getId() == right.getId()) {
                return Optional.of(left);
            }
        }
        return Optional.empty();
    }

    protected static class OnePartitionLocalJoin<K, V1, V2>
            extends LocalJoinOperator<K, V1, V2>
    {
        private final Partition[] partitions;

        protected OnePartitionLocalJoin(ReduceUtil.JoinMode joinMode, Operator<Tuple2<K, V1>> leftDataSet, Operator<Tuple2<K, V2>> rightDataSet, Comparator<K> comparator)
        {
            super(joinMode, leftDataSet, rightDataSet, comparator);
            this.partitions = new Partition[leftDataSet.numPartitions()];
            Partition[] leftPartitions = leftDataSet.getPartitions();
            Partition[] rightPartitions = rightDataSet.getPartitions();
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = new LocalJoinPartition(i, leftPartitions[i], rightPartitions[i]);
            }
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
        public Iterator<Tuple2<K, Tuple2<V1, V2>>> compute(Partition partition, TaskContext taskContext)
        {
            LocalJoinPartition localJoinPartition = (LocalJoinPartition) partition;
            if ((Object) leftDataSet == rightDataSet) {
                Iterator<Tuple2<K, V1>> left = leftDataSet.computeOrCache(localJoinPartition.left, taskContext);
                return ReduceUtil.sameJoin(left);
            }

            Iterator<Tuple2<K, V1>> left = leftDataSet.computeOrCache(localJoinPartition.left, taskContext);
            Iterator<Tuple2<K, V2>> right = rightDataSet.computeOrCache(localJoinPartition.right, taskContext);
            return ReduceUtil.mergeJoin(joinMode, comparator, left, right);
        }
    }

    private static class LocalJoinPartition
            extends Partition
    {
        private final Partition left;
        private final Partition right;

        public LocalJoinPartition(int index, Partition left, Partition right)
        {
            super(index);
            this.left = left;
            this.right = right;
        }
    }
}
