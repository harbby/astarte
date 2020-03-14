package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

public class RePartitionOperator<T>
        extends Operator<T>
{
    private final ShuffledOperator<T, T> shuffleReducer;

    protected RePartitionOperator(Operator<T> dataSet, int numPartition)
    {
        //todo: hash or number,but this is hash
        this(new ShuffleMapOperator<>(dataSet.map(x -> new Tuple2<>(x, x)), numPartition));
    }

    private RePartitionOperator(ShuffleMapOperator<T, T> dataSet)
    {
        this(new ShuffledOperator<>(dataSet));
    }

    private RePartitionOperator(ShuffledOperator<T, T> dataSet)
    {
        super(dataSet);
        this.shuffleReducer = dataSet;
    }

    @Override
    public Partition[] getPartitions()
    {
        return shuffleReducer.getPartitions();
    }

    @Override
    public int numPartitions()
    {
        return shuffleReducer.numPartitions();
    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext taskContext)
    {
        return Iterators.map(shuffleReducer.compute(split, taskContext), Tuple2::f2);
    }
}
