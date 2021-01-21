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

public interface DataSet<ROW>
        extends Serializable
{
    /**
     * driver exec
     */
    public abstract Partition[] getPartitions();

    public int numPartitions();

    public BatchContext getContext();

    int getId();

    List<ROW> collect();

    long count();

    void print(int limit);

    void print();

    void foreach(Foreach<ROW> foreach);

    void foreachPartition(Foreach<Iterator<ROW>> partitionForeach);

    Optional<ROW> reduce(Reducer<ROW> reducer);

    public Partitioner getPartitioner();

    <K, V> KvDataSet<K, V> kvDataSet(Mapper<ROW, Tuple2<K, V>> kvMapper);

    <K> KeyValueGroupedOperator<K, ROW> groupByKey(Mapper<ROW, K> mapFunc);

    DataSet<ROW> cache(CacheOperator.CacheMode cacheMode);

    DataSet<ROW> cache();

    DataSet<ROW> unCache();

    DataSet<ROW> distinct();

    DataSet<ROW> distinct(int numPartition);

    public DataSet<ROW> distinct(Partitioner partitioner);

    public DataSet<ROW> rePartition(int numPartition);

    <OUT> DataSet<OUT> map(Mapper<ROW, OUT> mapper);

    <OUT> DataSet<OUT> flatMap(Mapper<ROW, OUT[]> flatMapper);

    <OUT> DataSet<OUT> flatMapIterator(Mapper<ROW, Iterator<OUT>> flatMapper);

    <OUT> DataSet<OUT> mapPartition(Mapper<Iterator<ROW>, Iterator<OUT>> flatMapper);

    <OUT> DataSet<OUT> mapPartitionWithId(KvMapper<Integer, Iterator<ROW>, Iterator<OUT>> flatMapper);

    DataSet<ROW> filter(Filter<ROW> filter);

    public DataSet<ROW> union(DataSet<ROW> dataSet);

    public DataSet<ROW> union(DataSet<ROW> dataSet, int numPartition);

    public DataSet<ROW> union(DataSet<ROW> dataSet, Partitioner partitioner);

    public DataSet<ROW> unionAll(DataSet<ROW> dataSet);

    public KvDataSet<ROW, Long> zipWithIndex();
}
