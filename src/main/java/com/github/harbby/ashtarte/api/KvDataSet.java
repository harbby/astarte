package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public interface KvDataSet<K, V>
        extends DataSet<Tuple2<K, V>> {
    public KvDataSet<K, V> partitionBy(Partitioner<K> partitioner);

    public KvDataSet<K, V> partitionBy(int numPartitions);

    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer);

    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet);

    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet);
}
