package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;

public class FlatMapIteratorOperator<I, O>
        extends Operator<O>
{
    private final Mapper<I, Iterator<O>> flatMapper;
    private final Operator<I> dataSet;

    protected FlatMapIteratorOperator(Operator<I> dataSet, Mapper<I, Iterator<O>> flatMapper)
    {
        super(dataSet);
        this.flatMapper = flatMapper;
        this.dataSet = unboxing(dataSet);
    }

    @Override
    public Iterator<O> compute(Partition split, TaskContext taskContext)
    {
        return Iterators.concat(Iterators.map(dataSet.computeOrCache(split, taskContext),
                flatMapper::map));
    }
}
