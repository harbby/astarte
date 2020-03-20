package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.*;

/**
 * this full agg,not pipeline
 */
public class FullAggOperator<K, V, OUT>
        extends Operator<Tuple2<K, OUT>> {
    private final Operator<Tuple2<K, V>> operator;
    private final Mapper<Iterable<V>, OUT> agg;


    protected FullAggOperator(Operator<Tuple2<K, V>> operator, Mapper<Iterable<V>, OUT> agg) {
        super(operator);
        this.operator = operator;
        this.agg = agg;
    }

    @Override
    public Iterator<Tuple2<K, OUT>> compute(Partition split, TaskContext taskContext) {
        Iterator<Tuple2<K, V>> input = operator.compute(split, taskContext);

        //todo: 未增量计算
        Map<K, List<V>> kGroup = new HashMap<>();
        while (input.hasNext()) {
            Tuple2<K, V> tp = input.next();
            List<V> values = kGroup.computeIfAbsent(tp.f1(), k -> new ArrayList<>());
            values.add(tp.f2());
        }
        return kGroup.entrySet().stream()
                .map(x -> new Tuple2<>(x.getKey(), agg.map(x.getValue()))).iterator();
    }
}