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

import static java.util.Objects.requireNonNull;

public class ShuffleUnionOperator<E>
        extends Operator<E>
{
    private final Operator<? extends E>[] kvDataSets;
    private final Partitioner partitioner;

    @SafeVarargs
    protected ShuffleUnionOperator(Partitioner partitioner, Operator<E>... kvDataSets)
    {
        super(kvDataSets[0].getContext());
        this.kvDataSets = unboxing(kvDataSets);
        this.partitioner = requireNonNull(partitioner);
    }

    @Override
    public List<Operator<?>> getDependencies()
    {
        List<Operator<?>> deps = new ArrayList<>(kvDataSets.length);
        for (DataSet<? extends E> dataSet : kvDataSets) {
            Operator<? extends Tuple2<E, Object>> operator;
            operator = (Operator<? extends Tuple2<E, Object>>) dataSet.map(x -> new Tuple2<>(x, null));
            deps.add(new ShuffleMapOperator<>(operator, partitioner));
        }
        return deps;
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
    protected Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        int[] deps = taskContext.getDependStages();
        Iterator<Iterator<Tuple2<E, Object>>> iterators = Arrays.stream(deps)
                .mapToObj(shuffleId -> ShuffleManager.<E, Object>getReader(shuffleId, split.getId()))
                .iterator();

        return Iterators.map(Iterators.concat(iterators), x -> x.f1());
    }
}
