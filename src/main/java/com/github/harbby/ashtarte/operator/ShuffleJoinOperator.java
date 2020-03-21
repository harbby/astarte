package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleManager;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ShuffleJoinOperator<K>
        extends Operator<Tuple2<K, Iterable<?>[]>>
{
    private final DataSet<? extends Tuple2<K, ?>>[] kvDataSets;
    private final Partitioner partitioner;

    @SuppressWarnings("unchecked")
    @SafeVarargs
    protected ShuffleJoinOperator(Partitioner partitioner,
            Operator<? extends Tuple2<K, ?>>... kvDataSets)
    {
        super(kvDataSets[0].getContext());

        this.kvDataSets = Stream.of(kvDataSets).map(x-> unboxing(x))
        .toArray(Operator[]::new);

        this.partitioner = partitioner;
    }

    @Override
    public List<Operator<?>> getDependencies()
    {
        List<Operator<?>> deps = new ArrayList<>(kvDataSets.length);
        for (DataSet<? extends Tuple2<K, ?>> dataSet : kvDataSets) {
            Operator<Tuple2<K, Object>> operator = (Operator<Tuple2<K, Object>>) dataSet;
            deps.add(new ShuffleMapOperator<>(operator, partitioner));
        }
        return deps;
    }

    @Override
    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    /**
     * 最难算子...
     */
    @Override
    public Iterator<Tuple2<K, Iterable<?>[]>> compute(Partition split, TaskContext taskContext)
    {
        int[] deps = taskContext.getDependStages();
        Iterator<Iterator<Tuple2<K, Object>>> iterators = Arrays.stream(deps)
                .mapToObj(shuffleId -> ShuffleManager.<K, Object>getReader(shuffleId, split.getId()))
                .iterator();
        return Iterators.join(iterators, deps.length);
    }
}
