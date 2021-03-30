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
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.MapGroupFunc;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.Tuple2Encoder;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * this full agg,not pipeline
 */
public class FullAggOperator<K, V, O>
        extends Operator<Tuple2<K, O>>
{
    private final Operator<Tuple2<K, V>> dataSet;
    private final MapGroupFunc<K, V, O> mapGroupFunc;
    private final Tuple2Encoder<K, O> encoder;
    private final boolean canCache;

    public FullAggOperator(
            Operator<Tuple2<K, V>> dataSet,
            MapGroupFunc<K, V, O> mapGroupFunc,
            Tuple2Encoder<K, O> encoder,
            boolean canCache)
    {
        super(dataSet);
        this.dataSet = requireNonNull(unboxing(dataSet), "dataSet is null");
        this.mapGroupFunc = requireNonNull(mapGroupFunc);
        this.encoder = encoder;
        this.canCache = canCache;
    }

    @Override
    public DataSet<Tuple2<K, O>> cache(CacheManager.CacheMode cacheMode)
    {
        checkState(canCache, "groupByKey() don't can Cache");
        return super.cache(cacheMode);
    }

    @Override
    protected Encoder<Tuple2<K, O>> getRowEncoder()
    {
        return encoder;
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
        return Iterators.mapGroupSorted(input, mapGroupFunc);
    }
}
