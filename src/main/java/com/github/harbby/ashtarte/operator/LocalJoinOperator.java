package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * pre-shuffle join
 */
public class LocalJoinOperator<K>
        extends Operator<Tuple2<K, Iterable<?>[]>>
{
    private final Operator<Tuple2<K, Object>>[] kvDataSets;
    private final Partitioner partitioner;

    @SafeVarargs
    protected LocalJoinOperator(Partitioner partitioner,
            Operator<? extends Tuple2<K, ?>>... kvDataSets)
    {
        super(kvDataSets[0].getContext());
        this.kvDataSets = (Operator<Tuple2<K, Object>>[]) kvDataSets;
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return Arrays.asList(kvDataSets);
    }

    @Override
    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public int numPartitions()
    {
        return partitioner.numPartitions();
    }

    @Override
    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    public Iterator<Tuple2<K, Iterable<?>[]>> compute(Partition split, TaskContext taskContext)
    {
        int[] deps = taskContext.getDependStages();

        //checkState(deps.length == kvDataSets.length);
        if (deps.length == 1) {
            deps = new int[] {5, deps[0]};
        }
        int[] finalDeps = deps;

        Iterator<Iterator<Tuple2<K, Object>>> iterators = IntStream.range(0, finalDeps.length)
                .mapToObj(i -> {
                    int shuffleId = finalDeps[i];
                    TaskContext context = TaskContext.of(taskContext.getStageId()
                            , ImmutableList.of(shuffleId));
                    return kvDataSets[i].computeOrCache(split, context);
                }).iterator();

        return Iterators.join(iterators, kvDataSets.length);
    }
}
