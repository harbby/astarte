package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.Partitioner;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Collector;
import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.function.KeyGroupState;
import com.github.harbby.astarte.api.function.Mapper;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PipeLineAggOperator<K, V, OUT>
        extends Operator<Tuple2<K, OUT>>
{
    private final Operator<Tuple2<K, V>> operator;
    private final Mapper<KeyGroupState<K, OUT>, Collector<V>> groupCollector;

    protected PipeLineAggOperator(Operator<Tuple2<K, V>> operator, Mapper<KeyGroupState<K, OUT>, Collector<V>> groupCollector)
    {
        super(operator);
        this.operator = unboxing(operator);
        this.groupCollector = groupCollector;
    }

    @Override
    public Partitioner getPartitioner()
    {
        // Reducer<V> reducer 聚合不会发生Key的变化因此，我们可以传递Partitioner下去
        return operator.getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, OUT>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> input = operator.computeOrCache(split, taskContext);
        // 这里是增量计算的 复杂度= O(1) + log(m)
        Map<K, Tuple2<Collector<V>, KeyGroupState<K, OUT>>> aggState = new HashMap<>();  //这里是纯内存计算的, spark在1.x之后实现了内存溢出功能
        int count = 0;
        while (input.hasNext()) {
            Tuple2<K, V> tp = input.next();
            Tuple2<Collector<V>, KeyGroupState<K, OUT>> keyGroup = aggState.get(tp.f1);
            if (keyGroup == null) {
                KeyGroupState<K, OUT> keyGroupState = KeyGroupState.createKeyGroupState(tp.f1);
                keyGroup = new Tuple2<>(groupCollector.map(keyGroupState), keyGroupState);
                aggState.put(tp.f1, keyGroup);
            }

            Collector<V> collector = keyGroup.f1;
            collector.collect(tp.f2);
            count++;
        }
        logger.debug("AggOperator `convergent validity` is {}% ex: {}/{}", aggState.size() * 100.0 / count, aggState.size(), count);
        return aggState.entrySet().stream().map(x -> new Tuple2<>(x.getKey(), x.getValue().f2.getState())).iterator();
    }
}
