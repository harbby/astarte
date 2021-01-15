package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.AshtarteConf;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.operator.CollectionSource;
import com.github.harbby.ashtarte.operator.KvOperator;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.operator.TextFileSource;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface BatchContext
{
    public AshtarteConf getConf();

    public default <K, V> KvDataSet<K, V> makeKvDataSet(Collection<Tuple2<K, V>> collection, int parallelism)
    {
        return new KvOperator<>(new CollectionSource<>(this, collection, parallelism));
    }

    public default <V> DataSet<V> makeEmptyDataSet()
    {
        return makeEmptyDataSet(1);
    }

    public default <V> DataSet<V> makeEmptyDataSet(int parallelism)
    {
        return new CollectionSource<>(this, Collections.emptyList(), parallelism);
    }

    public default <K, V> KvDataSet<K, V> makeKvDataSet(Collection<Tuple2<K, V>> collection)
    {
        return new KvOperator<>(new CollectionSource<>(this, collection, 1));
    }

    public default <E> DataSet<E> makeDataSet(Collection<E> collection)
    {
        return new CollectionSource<>(this, collection, 1);
    }

    public default <E> DataSet<E> makeDataSet(Collection<E> collection, int parallelism)
    {
        return new CollectionSource<>(this, collection, parallelism);
    }

    public default <E> DataSet<E> makeDataSet(E... e)
    {
        return makeDataSet(Arrays.asList(e), 1);
    }

    public default <E> DataSet<E> makeDataSet(E[] e, int parallelism)
    {
        return makeDataSet(Arrays.asList(e), parallelism);
    }

    public default DataSet<String> textFile(String dirPath)
    {
        return new TextFileSource(this, dirPath);
    }

    public void setParallelism(int parallelism);

    public int getParallelism();

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final BatchContext context = new BatchContextImpl();

        public Builder setParallelism(int parallelism)
        {
            context.setParallelism(parallelism);
            return this;
        }

        public BatchContext getOrCreate()
        {
            return context;
        }
    }

    public <E, R> List<R> runJob(Operator<E> dataSet, Mapper<Iterator<E>, R> action);
}
