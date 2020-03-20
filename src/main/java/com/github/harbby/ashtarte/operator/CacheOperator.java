package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//import com.google.common.collect.MapMaker;

public class CacheOperator<E>
        extends Operator<E> {
    private final Operator<E> dataSet;

    private final Map<Integer, List<E>> cacheMemMap = new ConcurrentHashMap<>();

    protected CacheOperator(Operator<E> dataSet) {
        super(dataSet);
        this.dataSet = dataSet;
    }

    @Override
    public Iterator<E> compute(Partition split, TaskContext taskContext) {
        List<E> cacheData = cacheMemMap.get(split.getId());
        if (cacheData != null) {
            return cacheData.iterator();
        } else {
            final List<E> data = new ArrayList<>();
            Iterator<E> iterator = dataSet.compute(split, taskContext);
            return new Iterator<E>() {
                @Override
                public boolean hasNext() {
                    boolean hasNext = iterator.hasNext();
                    if (!hasNext) {
                        cacheMemMap.put(split.getId(), data);
                    }
                    return hasNext;
                }

                @Override
                public E next() {
                    E row = iterator.next();
                    data.add(row);
                    return row;
                }
            };
        }
    }
}
