package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.FlatMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapPartitionOperator<IN, OUT>
        extends Operator<OUT>
{
    private final FlatMapper<Iterator<IN>, OUT> flatMapper;
    private final Operator<IN> parentOp;

    protected MapPartitionOperator(Operator<IN> oneParent, FlatMapper<Iterator<IN>, OUT> flatMapper)
    {
        super(oneParent);
        this.flatMapper = flatMapper;
        this.parentOp = oneParent;
    }

    @Override
    public Iterator<OUT> compute(Partition split)
    {
        List<OUT> list = new ArrayList<>();
        flatMapper.flatMap(parentOp.compute(split), list::add);
        return list.iterator();
    }
}
