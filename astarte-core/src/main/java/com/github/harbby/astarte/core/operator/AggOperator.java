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
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.List;

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
        super(operator.getContext());
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
    public List<? extends Operator<?>> getDependencies()
    {
        return ImmutableList.of(operator);
    }

    @Override
    protected Encoder<Tuple2<K, V>> getRowEncoder()
    {
        return operator.getRowEncoder();
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> input = operator.computeOrCache(split, taskContext);
        //sort merge shuffle reducer
        return Iterators.reduceSorted(input, reducer);
    }
}
