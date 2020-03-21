package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.ashtarte.api.function.Foreach;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
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

    public MppContext getContext();

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

    DataSet<ROW> cache();

    DataSet<ROW> distinct();

    DataSet<ROW> distinct(int numPartition);

    public DataSet<ROW> rePartition(int numPartition);

    <OUT> DataSet<OUT> map(Mapper<ROW, OUT> mapper);

    <OUT> DataSet<OUT> flatMap(Mapper<ROW, OUT[]> flatMapper);

    <OUT> DataSet<OUT> flatMapIterator(Mapper<ROW, Iterator<OUT>> flatMapper);

    <OUT> DataSet<OUT> mapPartition(Mapper<Iterator<ROW>, Iterator<OUT>> flatMapper);

    DataSet<ROW> filter(Filter<ROW> filter);

    public DataSet<ROW> union(DataSet<ROW>... kvDataSets);

    public DataSet<ROW> unionAll(DataSet<ROW>... kvDataSets);

    <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter);

    <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter, int numReduce);

    <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter, Partitioner partitioner);
}
