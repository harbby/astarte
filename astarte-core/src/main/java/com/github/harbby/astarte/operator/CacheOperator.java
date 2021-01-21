package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.Partitioner;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//import com.google.common.collect.MapMaker;

@Deprecated
public class CacheOperator<E>
        extends Operator<E>
{
    public enum CacheMode
    {
        MEM_ONLY,
        MEM_DISK,
        DISK_ONLY; //todo: checkpoint ?
    }

    private final Operator<E> dataSet;
    List<Operator<?>> list = new ArrayList<>();
    private final static Map<Integer, Iterable<?>[]> cacheMemMap = new ConcurrentHashMap<>();

    public CacheOperator(Operator<E> dataSet)
    {
        super(dataSet);
        this.dataSet = unboxing(dataSet);
        list.add(dataSet);
    }

    @Override
    public Partitioner getPartitioner()
    {
        return dataSet.getPartitioner();
    }

    @Override
    public Partition[] getPartitions()
    {
        return dataSet.getPartitions();
    }

    @Override
    public Iterator<E> compute(Partition split, TaskContext taskContext)
    {
        return compute(dataSet, getId(), split, taskContext);
    }

    public static void unCacheExec(int jobId)
    {
        if (cacheMemMap.remove(jobId) != null) {
            logger.info("clear cache data {}", jobId);
        }
    }

    public static <E> Iterator<E> compute(Operator<E> dataSet, int jobId, Partition split, TaskContext taskContext)
    {
        @SuppressWarnings("unchecked")
        Iterable<E>[] jobCachePartitons = (Iterable<E>[]) cacheMemMap.computeIfAbsent(jobId, key -> new Iterable[dataSet.numPartitions()]);

        Iterable<E> partitionCache = jobCachePartitons[split.getId()];
        if (partitionCache != null) {
            logger.debug("-----{} cached dep stage: {} cache hit---", dataSet, taskContext.getDependStages());
            return partitionCache.iterator();
        }
        else {
            logger.debug("-----{} cached dep stage: {} cache miss---", dataSet, taskContext.getDependStages());
            final List<E> data = new ArrayList<>();
            Iterator<E> iterator = dataSet.compute(split, taskContext);
            return new Iterator<E>()
            {
                @Override
                public boolean hasNext()
                {
                    boolean hasNext = iterator.hasNext();
                    if (!hasNext) {
                        jobCachePartitons[split.getId()] = data;  //原子操作，线程安全
                        logger.debug("-----{} cached dep stage: {} write done---", dataSet, taskContext.getDependStages());
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
