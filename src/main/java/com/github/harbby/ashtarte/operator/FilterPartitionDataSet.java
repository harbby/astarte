package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Filter;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class FilterPartitionDataSet<ROW>
        extends Operator<ROW>
{
    private final Operator<ROW> parentOp;
    private final Filter<ROW> filter;

    public FilterPartitionDataSet(Operator<ROW> parentOp, Filter<ROW> filter)
    {
        super(parentOp);
        this.parentOp = parentOp;
        this.filter = filter;
    }

    @Override
    public Partition[] getPartitions()
    {
        return parentOp.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition)
    {
        return Iterators.filter(parentOp.compute(partition), filter::filter);
    }
}

