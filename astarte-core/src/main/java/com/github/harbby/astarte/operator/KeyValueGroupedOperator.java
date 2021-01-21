package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.api.Collector;
import com.github.harbby.astarte.api.DataSet;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.astarte.api.function.KeyGroupState;
import com.github.harbby.astarte.api.function.MapGroupFunc;
import com.github.harbby.astarte.api.function.Mapper;
import com.github.harbby.astarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

public class KeyValueGroupedOperator<K, ROW>
        implements Serializable
{
    private final Operator<ROW> dataSet;
    private final Mapper<ROW, K> mapFunc;

    public KeyValueGroupedOperator(Operator<ROW> dataSet, Mapper<ROW, K> mapFunc)
    {
        this.dataSet = dataSet;
        this.mapFunc = mapFunc;
    }

    public <OUT> DataSet<OUT> mapGroups(MapGroupFunc<K, ROW, OUT> mapGroupFunc)
    {
        // 进行shuffle
        Operator<Tuple2<K, ROW>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, ROW> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions());
        ShuffledOperator<K, ROW> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return new KvOperator<>(new FullAggOperator<>(shuffleReducer, mapGroupFunc)).values();
    }

    public KvDataSet<K, ROW> reduceGroups(Reducer<ROW> reducer)
    {
        return dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row))
                .reduceByKey(reducer);
    }

    public <OUT> KvDataSet<K, OUT> partitionGroupsWithState(Mapper<KeyGroupState<K, OUT>, Collector<ROW>> collector)
    {
        Operator<Tuple2<K, ROW>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, ROW> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions());
        ShuffledOperator<K, ROW> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return new KvOperator<>(new PipeLineAggOperator<>(shuffleReducer, collector));
    }

    public <OUT> DataSet<OUT> mapPartition(Mapper<Iterator<Tuple2<K, ROW>>, Iterator<OUT>> mapper)
    {
        Operator<Tuple2<K, ROW>> kv = dataSet.kvDataSet(row -> new Tuple2<>(mapFunc.map(row), row));
        ShuffleMapOperator<K, ROW> shuffleMapper = new ShuffleMapOperator<>(kv, kv.numPartitions());
        ShuffledOperator<K, ROW> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return shuffleReducer.mapPartition(mapper);
    }
}
