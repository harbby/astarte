/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.Reducer;
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
