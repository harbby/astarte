package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * this pipiline agg Operator
 */
public class AggOperator<K, V>
        extends Operator<Tuple2<K, V>> {
    private final Operator<Tuple2<K, V>> operator;
    private final Reducer<V> reducer;

    protected AggOperator(Operator<Tuple2<K, V>> operator, Reducer<V> reducer) {
        super(operator);
        this.operator = unboxing(operator);
        this.reducer = reducer;
    }

    @Override
    public Partitioner getPartitioner() {
        // Reducer<V> reducer 聚合不会发生Key的变化因此，我们可以传递Partitioner下去
        return operator.getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext) {
        Iterator<Tuple2<K, V>> input = operator.compute(split, taskContext);
        // 这里是增量计算的 复杂度= O(1) + log(m)
        Map<K, V> aggState = new HashMap<>();
        while (input.hasNext()) {
            Tuple2<K, V> tp = input.next();
            V value = aggState.get(tp.f1());
            if (value == null) {
                value = tp.f2();
            } else {
                value = reducer.reduce(value, tp.f2());
            }
            aggState.put(tp.f1(), value);
        }
        return aggState.entrySet().stream()
                .map(x -> new Tuple2<>(x.getKey(), x.getValue()))
                .iterator();
    }
}
