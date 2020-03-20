package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.*;

public class DistinctOperator<E> extends Operator<E> {

    private final ShuffledOperator<E, E> shuffledOperator;

    public DistinctOperator(Operator<E> dataSet) {
        super(dataSet.getContext());
        ShuffleMapOperator<E, E> shuffleMapOperator = new ShuffleMapOperator<>(
                dataSet.map(x -> new Tuple2<>(x, x)), dataSet.numPartitions());
        this.shuffledOperator = new ShuffledOperator<>(shuffleMapOperator);
    }

    @Override
    public List<Operator<?>> getDependencies() {

        return Collections.singletonList(shuffledOperator);
    }

    @Override
    public Iterator<E> compute(Partition split, TaskContext taskContext) {
        Iterator<Tuple2<E, E>> iterator = shuffledOperator.compute(split, taskContext);
        Set<E> set = new HashSet<>();
        while (iterator.hasNext()) {
            set.add(iterator.next().f2());
        }
        return set.iterator();
    }
}
