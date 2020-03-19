package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//import com.google.common.collect.MapMaker;

/**
 * 存在设计问题，
 * 1. 因为每个Job会单独反序列化。缓存不能放到Operator中
 * 2. 并发job线程安全问题
 */
public class CacheOperator<E>
        extends Operator<E> {
    private final Operator<E> dataSet;

    protected CacheOperator(Operator<E> dataSet) {
        super(dataSet);
        this.dataSet = dataSet;
    }

    @Override
    public DataSet<E> cache() {
        return this;
    }

    @Override
    public Iterator<E> compute(Partition split, TaskContext taskContext) {
        int key = getId() * 10 + split.hashCode();
        List<E> cacheData = (List<E>) CacheManager.getCacheData(key);
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
                        CacheManager.addCache(key, data);
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
