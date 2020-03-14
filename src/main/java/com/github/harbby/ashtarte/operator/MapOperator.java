package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;

public class MapOperator<IN, OUT>
        extends Operator<OUT>
{
    private final Operator<IN> parentOp;
    private final Mapper<IN, OUT> mapper;

    public MapOperator(Operator<IN> parentOp, Mapper<IN, OUT> mapper)
    {
        super(parentOp);
        this.parentOp = parentOp;
        this.mapper = mapper;
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext)
    {
        return Iterators.map(parentOp.compute(partition, taskContext), mapper::map);
    }
}
