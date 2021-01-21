package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.KvMapper;
import com.github.harbby.astarte.core.api.function.Mapper;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class MapPartitionOperator<I, O>
        extends Operator<O>
{
    private final Mapper<Iterator<I>, Iterator<O>> flatMapper;
    private final KvMapper<Integer, Iterator<I>, Iterator<O>> flatMapperWithId;
    private final Operator<I> dataSet;
    private final boolean holdPartitioner;

    protected MapPartitionOperator(Operator<I> dataSet,
            Mapper<Iterator<I>, Iterator<O>> f,
            boolean holdPartitioner)
    {
        super(dataSet);
        this.flatMapper = requireNonNull(f, "f is null");
        this.flatMapperWithId = null;

        this.dataSet = unboxing(dataSet);
        this.holdPartitioner = holdPartitioner;
    }

    protected MapPartitionOperator(Operator<I> dataSet,
            KvMapper<Integer, Iterator<I>, Iterator<O>> f,
            boolean holdPartitioner)
    {
        super(dataSet);
        this.flatMapper = null;
        this.flatMapperWithId = requireNonNull(f, "f is null");
        this.dataSet = unboxing(dataSet);
        this.holdPartitioner = holdPartitioner;
    }

    @Override
    public Partitioner getPartitioner()
    {
        if (holdPartitioner) {
            return dataSet.getPartitioner();
        }
        return null;
    }

    @Override
    public Iterator<O> compute(Partition split, TaskContext taskContext)
    {
        Iterator<O> iterator;
        if (flatMapper != null) {
            iterator = flatMapper.map(dataSet.computeOrCache(split, taskContext));
        }
        else {
            iterator = flatMapperWithId.map(split.getId(), dataSet.computeOrCache(split, taskContext));
        }
        return requireNonNull(iterator, "MapPartition function return null,your use Iterators.empty()");
    }
}
