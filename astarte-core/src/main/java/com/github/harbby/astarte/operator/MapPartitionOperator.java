package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.Partitioner;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.function.KvMapper;
import com.github.harbby.astarte.api.function.Mapper;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class MapPartitionOperator<IN, OUT>
        extends Operator<OUT>
{
    private final Mapper<Iterator<IN>, Iterator<OUT>> flatMapper;
    private final KvMapper<Integer, Iterator<IN>, Iterator<OUT>> flatMapperWithId;
    private final Operator<IN> dataSet;
    private final boolean holdPartitioner;

    protected MapPartitionOperator(Operator<IN> dataSet,
            Mapper<Iterator<IN>, Iterator<OUT>> f,
            boolean holdPartitioner)
    {
        super(dataSet);
        this.flatMapper = requireNonNull(f, "f is null");
        this.flatMapperWithId = null;

        this.dataSet = unboxing(dataSet);
        this.holdPartitioner = holdPartitioner;
    }

    protected MapPartitionOperator(Operator<IN> dataSet,
            KvMapper<Integer, Iterator<IN>, Iterator<OUT>> f,
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
    public Iterator<OUT> compute(Partition split, TaskContext taskContext)
    {
        Iterator<OUT> iterator;
        if (flatMapper != null) {
            iterator = flatMapper.map(dataSet.computeOrCache(split, taskContext));
        }
        else {
            iterator = flatMapperWithId.map(split.getId(), dataSet.computeOrCache(split, taskContext));
        }
        return requireNonNull(iterator, "MapPartition function return null,your use Iterators.empty()");
    }
}
