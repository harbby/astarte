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

import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.function.MapGroupFunc;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.api.function.Reducer;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

public class KeyValueGroupedOperator<K, R>
        implements Serializable
{
    private final Operator<R> dataSet;
    private final Mapper<R, K> mapFunc;

    public KeyValueGroupedOperator(Operator<R> dataSet, Mapper<R, K> mapFunc)
    {
        this.dataSet = dataSet;
        this.mapFunc = mapFunc;
    }

    public <O> DataSet<O> mapGroups(MapGroupFunc<K, R, O> mapGroupFunc)
    {
        // 进行shuffle
        Operator<Tuple2<K, R>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, R> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions(), Encoder.anyComparator(), null);
        ShuffledMergeSortOperator<K, R> shuffleReducer = new ShuffledMergeSortOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return new KvOperator<>(new FullAggOperator<>(shuffleReducer, mapGroupFunc)).values();
    }

    public KvDataSet<K, R> reduceGroups(Reducer<R> reducer)
    {
        return dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row))
                .reduceByKey(reducer);
    }

    public <O> DataSet<O> mapPartition(Mapper<Iterator<Tuple2<K, R>>, Iterator<O>> mapper)
    {
        Operator<Tuple2<K, R>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, R> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions(), Encoder.anyComparator(), null);
        ShuffledMergeSortOperator<K, R> shuffleReducer = new ShuffledMergeSortOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return shuffleReducer.mapPartition(mapper);
    }
}
