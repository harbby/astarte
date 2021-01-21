package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.function.Mapper;
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
        this.dataSet = unboxing(dataSet);
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext) {
        return Iterators.concat(Iterators.map(dataSet.computeOrCache(partition, taskContext),
                row -> Stream.of(flatMapper.map(row)).iterator()));
    }
}
