package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.function.Mapper;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;
import java.util.stream.Stream;

public class FlatMapOperator<I, O>
        extends Operator<O>
{
    private final Mapper<I, O[]> flatMapper;
    private final Operator<I> dataSet;

    protected FlatMapOperator(Operator<I> dataSet, Mapper<I, O[]> flatMapper)
    {
        super(dataSet);
        this.flatMapper = flatMapper;
        this.dataSet = unboxing(dataSet);
    }

    @Override
    public Iterator<O> compute(Partition partition, TaskContext taskContext)
    {
        return Iterators.concat(Iterators.map(dataSet.computeOrCache(partition, taskContext),
                row -> Stream.of(flatMapper.map(row)).iterator()));
    }
}
