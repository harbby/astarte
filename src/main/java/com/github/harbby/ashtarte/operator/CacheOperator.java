package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//import com.google.common.collect.MapMaker;

@Deprecated
public class CacheOperator<E>
        extends Operator<E>
{
    private final Operator<E> dataSet;
    List<Operator<?>> list = new ArrayList<>();
    private final static Map<Integer, List<?>> cacheMemMap = new ConcurrentHashMap<>();

    protected CacheOperator(Operator<E> dataSet)
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

//    @Override
//    public List<? extends Operator<?>> getDependencies()
//    {
//        try {
//            return ImmutableList.copy(list);
//        }
//        finally {
//            list.clear();
//        }
//    }

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

    public static <E> Iterator<E> compute(Operator<E> dataSet, int jobId, Partition split, TaskContext taskContext)
    {
        int key = jobId * 100 + split.getId();
        List<E> cacheData = (List<E>) cacheMemMap.get(key);
        if (cacheData != null) {
            List<Integer> deps = IntStream.of(taskContext.getDependStages()).mapToObj(x->x).collect(Collectors.toList());
            System.out.println("-----" + dataSet.getId() +" dep:" + deps + "缓存命中---");
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
                        cacheMemMap.put(key, data);
                        System.out.println("-------------cacde done----------");
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
