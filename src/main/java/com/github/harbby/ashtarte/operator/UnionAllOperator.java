package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class UnionAllOperator<E>
        extends Operator<E>
{
    private final Operator<E>[] kvDataSets;

    @SafeVarargs
    protected UnionAllOperator(Operator<E>... kvDataSets)
    {
        super(kvDataSets);
        this.kvDataSets = kvDataSets;
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
        for (Operator<E> operator : kvDataSets) {
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

        int[] depShuffles = taskContext.getDependStages();

        for (int i = 0; i < kvDataSets.length; i++) {
            if (unionAllPartition.operatorId == kvDataSets[i].getId()) {
                int depShuffleId = depShuffles[i];
                TaskContext context = TaskContext.of(taskContext.getStageId(), depShuffleId);
                return kvDataSets[i].computeOrCache(unionAllPartition.partition, context);
            }
        }
        throw new IllegalStateException();
    }
}
