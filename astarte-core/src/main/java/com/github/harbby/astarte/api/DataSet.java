package com.github.harbby.astarte.api;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.Partitioner;
import com.github.harbby.astarte.api.function.Filter;
import com.github.harbby.astarte.api.function.Foreach;
import com.github.harbby.astarte.api.function.KvMapper;
import com.github.harbby.astarte.api.function.Mapper;
import com.github.harbby.astarte.api.function.Reducer;
import com.github.harbby.astarte.operator.CacheOperator;
import com.github.harbby.astarte.operator.KeyValueGroupedOperator;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public interface DataSet<R>
        extends Serializable
{
    public abstract Partition[] getPartitions();

    public int numPartitions();

    public BatchContext getContext();

    int getId();

    List<R> collect();

    long count();

    void print(int limit);

    void print();

    void foreach(Foreach<R> foreach);

    void foreachPartition(Foreach<Iterator<R>> partitionForeach);

    Optional<R> reduce(Reducer<R> reducer);

    public Partitioner getPartitioner();

    <K, V> KvDataSet<K, V> kvDataSet(Mapper<R, Tuple2<K, V>> kvMapper);

    <K> KeyValueGroupedOperator<K, R> groupByKey(Mapper<R, K> mapFunc);

    DataSet<R> cache(CacheOperator.CacheMode cacheMode);

    DataSet<R> cache();

    DataSet<R> unCache();

    DataSet<R> partitionLimit(int limit);

    DataSet<R> limit(int limit);

    DataSet<R> distinct();

    DataSet<R> distinct(int numPartition);

    public DataSet<R> distinct(Partitioner partitioner);

    public DataSet<R> rePartition(int numPartition);

    <O> DataSet<O> map(Mapper<R, O> mapper);

    <O> DataSet<O> flatMap(Mapper<R, O[]> flatMapper);

    <O> DataSet<O> flatMapIterator(Mapper<R, Iterator<O>> flatMapper);

    <O> DataSet<O> mapPartition(Mapper<Iterator<R>, Iterator<O>> flatMapper);

    <O> DataSet<O> mapPartitionWithId(KvMapper<Integer, Iterator<R>, Iterator<O>> flatMapper);

    DataSet<R> filter(Filter<R> filter);

    public DataSet<R> union(DataSet<R> dataSet);

    public DataSet<R> union(DataSet<R> dataSet, int numPartition);

    public DataSet<R> union(DataSet<R> dataSet, Partitioner partitioner);

    public DataSet<R> unionAll(DataSet<R> dataSet);

    public KvDataSet<R, Long> zipWithIndex();
}
