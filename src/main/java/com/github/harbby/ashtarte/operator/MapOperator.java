package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.utils.CheckUtil;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;

@Deprecated
public class MapOperator<IN, OUT>
        extends Operator<OUT> {
    private final Operator<IN> dataSet;
    private final Mapper<IN, OUT> mapper;

    public MapOperator(Operator<IN> dataSet, Mapper<IN, OUT> mapper) {
        super(dataSet);
        this.dataSet = dataSet;
        this.mapper = CheckUtil.checkSerialize(mapper);
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext) {
        return Iterators.map(dataSet.compute(partition, taskContext), mapper::map);
    }
}
