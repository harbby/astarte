package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.ashtarte.operator.KvOperator;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

public interface KvDataSet<K, V>
        extends DataSet<Tuple2<K, V>>
{

    public static <K, V> KvDataSet<K, V> toKvDataSet(DataSet<Tuple2<K, V>> dataSet)
    {
        return new KvOperator<>((Operator<Tuple2<K, V>>) dataSet);
    }

    DataSet<K> keys();

    <OUT> KvDataSet<K, OUT> mapValues(Mapper<V, OUT> mapper);

    <OUT> KvDataSet<K, OUT> flatMapValues(Mapper<V, Iterator<OUT>> mapper);

    DataSet<V> values();

    public KvDataSet<K, V> rePartition(int numPartition);

    KvDataSet<K, V> cache();

    KvDataSet<K, V> distinct();

    KvDataSet<K, V> distinct(int numPartition);

    KvDataSet<K, Iterable<V>> groupByKey();

    public KvDataSet<K, V> partitionBy(Partitioner partitioner);

    public KvDataSet<K, V> partitionBy(int numPartitions);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, int numPartition);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, Partitioner partitioner);

    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet);

    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet);
}
