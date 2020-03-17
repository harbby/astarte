package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class FlatMapIteratorOperator<IN, OUT>
        extends Operator<OUT>
{
    private final Mapper<IN, Iterator<OUT>> flatMapper;
    private final Operator<IN> dataSet;

    protected FlatMapIteratorOperator(Operator<IN> dataSet, Mapper<IN, Iterator<OUT>> flatMapper)
    {
        super(dataSet);
        this.flatMapper = flatMapper;
        this.dataSet = dataSet;
    }

    @Override
    public Iterator<OUT> compute(Partition split, TaskContext taskContext)
    {
        return Iterators
                .concat(Iterators.transform(dataSet.compute(split, taskContext),
                        flatMapper::map));
    }
}
