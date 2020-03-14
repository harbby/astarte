package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.google.common.collect.MapMaker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * 存在设计问题，
 * 1. 因为每个Job会单独反序列化。缓存不能放到Operator中
 * 2. 并发job线程安全问题
 */
public class CacheOperator<E>
        extends Operator<E>
{
    private final Operator<E> oneParent;
    private static final ConcurrentMap<Integer, DataSet<?>> cacheDataSet = new MapMaker().weakValues().makeMap();

    protected CacheOperator(Operator<E> oneParent)
    {
        super(oneParent);
        this.oneParent = oneParent;
        cacheDataSet.put(getId(), this);
    }

    @Override
    public DataSet<E> cache()
    {
        return this;
    }

    @Override
    public Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        int key = getId() * 10 + split.hashCode();
        List<E> cacheData = (List<E>) CacheManager.getCacheData(key);
        if (cacheData != null) {
            return cacheData.iterator();
        }
        else {
            List<E> data = new ArrayList<>();
            Iterator<E> iterator = oneParent.compute(split, taskContext);
            return new Iterator<E>()
            {
                @Override
                public boolean hasNext()
                {
                    boolean hasNext = iterator.hasNext();
                    if (!hasNext) {
                        CacheManager.addCache(key, data);
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

                @Override
                public void remove()
                {
                    iterator.remove();
                }
            };
        }
    }
}
