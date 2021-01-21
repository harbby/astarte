package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.Partitioner;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.function.MapGroupFunc;
import com.github.harbby.astarte.api.function.Mapper;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * this full agg,not pipeline
 */
public class FullAggOperator<K, V, O>
        extends Operator<Tuple2<K, O>>
{
    private final Operator<Tuple2<K, V>> dataSet;
    private final MapGroupFunc<K, V, O> mapGroupFunc;

    protected FullAggOperator(
            Operator<Tuple2<K, V>> dataSet,
            Mapper<Iterable<V>, O> agg)
    {
        super(dataSet);
        this.dataSet = requireNonNull(unboxing(dataSet), "dataSet is null");
        this.mapGroupFunc = (k, iterable) -> agg.map(iterable);
    }

    protected FullAggOperator(
            Operator<Tuple2<K, V>> dataSet,
            MapGroupFunc<K, V, O> mapGroupFunc)
    {
        super(dataSet);
        this.dataSet = requireNonNull(unboxing(dataSet), "dataSet is null");
        this.mapGroupFunc = requireNonNull(mapGroupFunc);
    }

    @Override
    public Partitioner getPartitioner()
    {
        return dataSet.getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, O>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> input = dataSet.computeOrCache(split, taskContext);
        //todo: 非增量计算,需实现溢写功能
        Map<K, Collection<V>> kGroup = new HashMap<>();
        while (input.hasNext()) {
            Tuple2<K, V> tp = input.next();
            Collection<V> values = kGroup.computeIfAbsent(tp.f1(), k -> new LinkedList<>());
            values.add(tp.f2());
        }
        return kGroup.entrySet().stream()
                .map(x -> new Tuple2<>(x.getKey(), mapGroupFunc.apply(x.getKey(), x.getValue()))).iterator();
    }
}
