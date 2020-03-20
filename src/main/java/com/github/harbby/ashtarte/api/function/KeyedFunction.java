package com.github.harbby.ashtarte.api.function;

import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

public interface KeyedFunction<KEY, ROW>
{
    DataSet<Tuple2<KEY, Long>> count();

    DataSet<Tuple2<KEY, Double>> avg(Mapper<ROW, Double> keyGetter);

    /**
     * agg_if
     */
    <VALUE> DataSet<Tuple2<KEY, VALUE>> agg(Mapper<ROW, VALUE> keyGetter, Reducer<VALUE> reducer);

    <VALUE> DataSet<Tuple2<KEY, VALUE>> map(Mapper<Iterable<ROW>, VALUE> mapperReduce);
}
