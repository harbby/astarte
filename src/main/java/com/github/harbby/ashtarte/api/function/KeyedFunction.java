package com.github.harbby.ashtarte.api.function;

import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public interface KeyedFunction<K, ROW> {
    KvDataSet<K, Long> count();

    KvDataSet<K, Double> avg(Mapper<ROW, Double> keyGetter);

    /**
     * agg_if
     */
    <V> KvDataSet<K, V> agg(Mapper<ROW, V> keyGetter, Reducer<V> reducer);

    <V> KvDataSet<K, V> map(Mapper<Iterable<ROW>, V> mapperReduce);
}
