package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleManager;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ShuffleJoinOperator<K>
        extends Operator<Tuple2<K, Iterable<?>[]>>
{
    private final DataSet<? extends Tuple2<K, ?>>[] kvDataSets;
    private final Partitioner partitioner;
    private final List<ShuffleMapOperator<K, ?>> shuffleMapOperators;

    @SuppressWarnings("unchecked")
    @SafeVarargs
    protected ShuffleJoinOperator(Partitioner partitioner,
            Operator<? extends Tuple2<K, ?>>... kvDataSets)
    {
        super(kvDataSets[0].getContext());
        Operator<? extends Tuple2<K, ?>>[] unboxingOps = unboxing(kvDataSets);

        this.kvDataSets = unboxingOps;
        this.partitioner = partitioner;
        this.shuffleMapOperators = createShuffleMapOps(unboxingOps, partitioner);
    }

    private static <K> List<ShuffleMapOperator<K, ?>> createShuffleMapOps(
            Operator<? extends Tuple2<K, ?>>[] kvDataSets,
            Partitioner partitioner)
    {
        List<ShuffleMapOperator<K, ?>> deps = new ArrayList<>(kvDataSets.length);
        for (DataSet<? extends Tuple2<K, ?>> dataSet : kvDataSets) {
            @SuppressWarnings("unchecked")
            Operator<Tuple2<K, Object>> operator = (Operator<Tuple2<K, Object>>) dataSet;
            deps.add(new ShuffleMapOperator<>(operator, partitioner));
        }
        return deps;
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        return shuffleMapOperators;
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

    /**
     * 最难算子...
     */
    @Override
    public Iterator<Tuple2<K, Iterable<?>[]>> compute(Partition split, TaskContext taskContext)
    {
        Map<Integer, Integer> deps = taskContext.getDependStages();
        for (Integer shuffleId : deps.values()) {
            checkState(shuffleId != null, "shuffleId is null");
        }
        Iterator<Iterator<Tuple2<K, Object>>> iterators = shuffleMapOperators.stream().map(operator -> {
            int shuffleId = deps.get(operator.getId());
            return ShuffleManager.<K, Object>getReader(shuffleId, split.getId());
        }).iterator();

        return Iterators.join(iterators, kvDataSets.length);
    }
}
