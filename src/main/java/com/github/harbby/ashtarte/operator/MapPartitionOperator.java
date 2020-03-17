package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class MapPartitionOperator<IN, OUT>
        extends Operator<OUT>
{
    private final Mapper<Iterator<IN>, Iterator<OUT>> flatMapper;
    private final Operator<IN> dataSet;

    protected MapPartitionOperator(Operator<IN> dataSet, Mapper<Iterator<IN>, Iterator<OUT>> flatMapper)
    {
        super(dataSet);
        this.flatMapper = flatMapper;
        this.dataSet = dataSet;
    }

    @Override
    public Iterator<OUT> compute(Partition split, TaskContext taskContext)
    {
        Iterator<OUT> iterator = flatMapper.map(dataSet.compute(split, taskContext));
        return requireNonNull(iterator, "MapPartition function return null,your use Iterators.empty()");
    }
}
