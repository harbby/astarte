package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.Partitioner;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple1;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * this pipiline agg Operator
 */
public class AggOperator<K, V>
        extends Operator<Tuple2<K, V>>
{
    private final Operator<Tuple2<K, V>> operator;
    private final Reducer<V> reducer;

    protected AggOperator(Operator<Tuple2<K, V>> operator, Reducer<V> reducer)
    {
        super(operator);
        this.operator = unboxing(operator);
        this.reducer = reducer;
    }

    @Override
    public Partitioner getPartitioner()
    {
        // Reducer<V> reducer 聚合不会发生Key的变化因此，我们可以传递Partitioner下去
        return operator.getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> input = operator.computeOrCache(split, taskContext);
        // 这里是增量计算的 复杂度= O(1) + log(m)
        Map<K, Tuple1<V>> aggState = new HashMap<>();
        int count = 0;
        while (input.hasNext()) {
            Tuple2<K, V> tp = input.next();
            Tuple1<V> value = aggState.get(tp.f1());
            if (value == null) {
                aggState.put(tp.f1, new Tuple1<>(tp.f2));
            }
            else {
                value.set(reducer.reduce(value.get(), tp.f2));
            }
            count++;
        }
        logger.info("AggOperator `convergent validity` is {}% {}/{}", aggState.size() * 100.0 / count, aggState.size(), count);
        return aggState.entrySet().stream()
                .map(x -> new Tuple2<>(x.getKey(), x.getValue().get()))
                .iterator();
    }
}
