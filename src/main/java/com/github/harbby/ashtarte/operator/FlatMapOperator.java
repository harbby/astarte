package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;
import java.util.stream.Stream;

public class FlatMapOperator<IN, OUT>
        extends Operator<OUT> {
    private final Mapper<IN, OUT[]> flatMapper;
    private final Operator<IN> dataSet;

    protected FlatMapOperator(Operator<IN> dataSet, Mapper<IN, OUT[]> flatMapper) {
        super(dataSet);
        this.flatMapper = flatMapper;
        this.dataSet = dataSet;
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext) {
        return Iterators.concat(Iterators.map(dataSet.compute(partition, taskContext),
                row -> Stream.of(flatMapper.map(row)).iterator()));
    }
}
