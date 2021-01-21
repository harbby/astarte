package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.api.Collector;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.function.KeyGroupState;
import com.github.harbby.astarte.core.api.function.MapGroupFunc;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.api.function.Reducer;
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
        ShuffleMapOperator<K, R> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions());
        ShuffledOperator<K, R> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return new KvOperator<>(new FullAggOperator<>(shuffleReducer, mapGroupFunc)).values();
    }

    public KvDataSet<K, R> reduceGroups(Reducer<R> reducer)
    {
        return dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row))
                .reduceByKey(reducer);
    }

    public <O> KvDataSet<K, O> partitionGroupsWithState(Mapper<KeyGroupState<K, O>, Collector<R>> collector)
    {
        Operator<Tuple2<K, R>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, R> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions());
        ShuffledOperator<K, R> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return new KvOperator<>(new PipeLineAggOperator<>(shuffleReducer, collector));
    }

    public <O> DataSet<O> mapPartition(Mapper<Iterator<Tuple2<K, R>>, Iterator<O>> mapper)
    {
        Operator<Tuple2<K, R>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, R> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions());
        ShuffledOperator<K, R> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return shuffleReducer.mapPartition(mapper);
    }
}
