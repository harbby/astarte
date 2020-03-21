package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.gadtry.collection.immutable.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//import com.google.common.collect.MapMaker;

public class CacheOperator<E>
        extends Operator<E>
{
    private final Operator<E> dataSet;
    List<Operator<?>> list = new ArrayList<>();
    private final static Map<Integer, List<?>> cacheMemMap = new ConcurrentHashMap<>();

    protected CacheOperator(Operator<E> dataSet)
    {
        super(dataSet);
        this.dataSet = dataSet;
        list.add(dataSet);
    }

    @Override
    public Partitioner getPartitioner()
    {
        return dataSet.getPartitioner();
    }

    @Override
    public List<? extends Operator<?>> getDependencies()
    {
        try {
            return ImmutableList.copy(list);
        }
        finally {
            list.clear();
        }
    }

    @Override
    public Partition[] getPartitions()
    {
        return dataSet.getPartitions();
    }

    @Override
    public Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        List<E> cacheData = (List<E>) cacheMemMap.get(getId() * 100 + split.getId());
        if (cacheData != null) {
            return cacheData.iterator();
        }
        else {
            final List<E> data = new ArrayList<>();
            Iterator<E> iterator = dataSet.compute(split, taskContext);
            return new Iterator<E>()
            {
                @Override
                public boolean hasNext()
                {
                    boolean hasNext = iterator.hasNext();
                    if (!hasNext) {
                        cacheMemMap.put(getId() * 100 + split.getId(), data);
                    }
                    return hasNext;
                }

                @Override
                public E next()
                {
                    E row = iterator.next();
                    data.add(row);
                    return row;
                }
            };
        }
    }
}
