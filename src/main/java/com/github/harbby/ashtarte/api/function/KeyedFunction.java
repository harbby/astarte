package com.github.harbby.ashtarte.api.function;

import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

public interface KeyedFunction<KEY, ROW>
{
    DataSet<Tuple2<KEY, Long>> count();

    /**
     * sql: sum_if
     */
    DataSet<Tuple2<KEY, Double>> sum(KeyGetter<ROW, Double> keyGetter);

//    DataSet<Tuple2<KEY, Double>> avg(KeyGetter<ROW, Long> keyGetter);

    DataSet<Tuple2<KEY, Double>> avg(KeyGetter<ROW, Double> keyGetter);

    <VALUE> DataSet<Tuple2<KEY, VALUE>> agg(KeyGetter<ROW, VALUE> keyGetter, Reducer<VALUE> reducer);

    <VALUE> DataSet<Tuple2<KEY, VALUE>> map(Mapper<Iterator<ROW>, VALUE> mapperReduce);
}
