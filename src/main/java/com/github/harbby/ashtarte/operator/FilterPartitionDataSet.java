package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;

public class FilterPartitionDataSet<ROW>
        extends Operator<ROW> {
    private final Operator<ROW> dataSet;
    private final Filter<ROW> filter;

    public FilterPartitionDataSet(Operator<ROW> dataSet, Filter<ROW> filter) {
        super(dataSet);
        this.dataSet = dataSet;
        this.filter = filter;
    }

    @Override
    public Partition[] getPartitions() {
        return dataSet.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition, TaskContext taskContext) {
        return Iterators.filter(dataSet.compute(partition, taskContext), filter::filter);
    }
}

