package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.function.Comparator;
import com.github.harbby.ashtarte.api.function.KvForeach;
import com.github.harbby.ashtarte.api.function.KvMapper;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.ashtarte.operator.CacheOperator;
import com.github.harbby.ashtarte.operator.KvOperator;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.Map;

public interface KvDataSet<K, V>
        extends DataSet<Tuple2<K, V>>
{
    public static <K, V> KvDataSet<K, V> toKvDataSet(DataSet<Tuple2<K, V>> dataSet)
    {
        if (dataSet instanceof KvDataSet) {
            return (KvDataSet<K, V>) dataSet;
        }
        return new KvOperator<>((Operator<Tuple2<K, V>>) dataSet);
    }

    void foreach(KvForeach<K, V> mapper);

    Map<K, V> collectMap();

    <OUT> DataSet<OUT> map(KvMapper<K, V, OUT> mapper);

    DataSet<K> keys();

    <OUT> KvDataSet<K, OUT> mapValues(Mapper<V, OUT> mapper);

    <OUT> KvDataSet<K, OUT> flatMapValues(Mapper<V, Iterator<OUT>> mapper);

    <K1> KvDataSet<K1, V> mapKeys(Mapper<K, K1> mapper);

    DataSet<V> values();

    public KvDataSet<K, V> rePartition(int numPartition);

    KvDataSet<K, V> cache();

    KvDataSet<K, V> cache(CacheOperator.CacheMode cacheMode);

    KvDataSet<K, V> unCache();

    KvDataSet<K, V> distinct();

    KvDataSet<K, V> distinct(int numPartition);

    KvDataSet<K, V> distinct(Partitioner partitioner);

    KvDataSet<K, Iterable<V>> groupByKey();

    public KvDataSet<K, V> partitionBy(Partitioner partitioner);

    public KvDataSet<K, V> partitionBy(int numPartitions);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, int numPartition);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, Partitioner partitioner);

    public KvDataSet<K, Double> avgValues(Mapper<V, Double> valueCast);

    public KvDataSet<K, Double> avgValues(Mapper<V, Double> valueCast, int numPartition);

    public KvDataSet<K, Double> avgValues(Mapper<V, Double> valueCast, Partitioner partitioner);

    public KvDataSet<K, Long> countByKey();

    public KvDataSet<K, Long> countByKey(int numPartition);

    public KvDataSet<K, Long> countByKey(Partitioner partitioner);

    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet);

    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet);

    public KvDataSet<K, V> union(DataSet<Tuple2<K, V>> kvDataSet);

    public KvDataSet<K, V> union(KvDataSet<K, V> kvDataSet, int numPartition);

    public KvDataSet<K, V> union(KvDataSet<K, V> kvDataSet, Partitioner partitioner);

    public KvDataSet<K, V> unionAll(DataSet<Tuple2<K, V>> kvDataSet);

    public KvDataSet<K, V> sortByKey(Comparator<K> comparator);

    public KvDataSet<K, V> sortByKey(Comparator<K> comparator, int numPartitions);

    public KvDataSet<K, V> sortByValue(Comparator<V> comparator);

    public KvDataSet<K, V> sortByValue(Comparator<V> comparator, int numPartitions);
}
