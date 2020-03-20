package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public interface KvDataSet<K, V>
        extends DataSet<Tuple2<K, V>> {

    DataSet<K> keys();

    DataSet<V> values();

    public KvDataSet<K, V> rePartition(int numPartition);

    KvDataSet<K, V> cache();

    KvDataSet<K, V> distinct();

    KvDataSet<K, Iterable<V>> groupByKey();

    default <OUT> KvDataSet<K, OUT> mapValues(Mapper<V, OUT> mapper) {
        return this.kvDataSet(kv -> new Tuple2<>(kv.f1(), mapper.map(kv.f2())));  //todo: obj copy
    }

    public KvDataSet<K, V> partitionBy(Partitioner<K> partitioner);

    public KvDataSet<K, V> partitionBy(int numPartitions);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer);

    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet);

    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet);
}
