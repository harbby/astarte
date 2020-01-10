package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.ashtarte.api.function.FlatMapper;
import com.github.harbby.ashtarte.api.function.Foreach;
import com.github.harbby.ashtarte.api.function.KeyGetter;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public interface DataSet<Row>
        extends Serializable
{
    /**
     * driver exec
     */
    public abstract Partition[] getPartitions();

    public MppContext getContext();

    int getId();

    List<Row> collect();

    long count();

    DataSet<Row> cache();

    <OUT> DataSet<OUT> map(Mapper<Row, OUT> mapper);

    <OUT> DataSet<OUT> flatMap(Mapper<Row, OUT[]> flatMapper);

    <OUT> DataSet<OUT> flatMap(FlatMapper<Row, OUT> flatMapper);

    <OUT> DataSet<OUT> mapPartition(FlatMapper<Iterator<Row>, OUT> flatMapper);

    DataSet<Row> filter(Filter<Row> filter);

    Optional<Row> reduce(Reducer<Row> reducer);

    <KEY> KeyedFunction<KEY, Row> groupBy(KeyGetter<Row, KEY> keyGetter);

    void foreach(Foreach<Row> foreach);

    void foreachPartition(Foreach<Iterator<Row>> partitionForeach);
}
