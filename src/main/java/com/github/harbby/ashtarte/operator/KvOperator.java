package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class KvOperator<K, V>
        extends Operator<Tuple2<K, V>>
        implements KvDataSet<K, V> {
    private final Operator<Tuple2<K, V>> dataSet;
    private Partitioner<K> partitioner;

    public KvOperator(Operator<Tuple2<K, V>> dataSet) {
        super(dataSet);
        this.dataSet = dataSet;
        this.partitioner = new HashPartitioner<>(dataSet.numPartitions());
    }

    @Override
    public DataSet<K> keys() {
        return dataSet.map(Tuple2::f1);
    }

    @Override
    public DataSet<V> values() {
        return dataSet.map(Tuple2::f2);
    }

    @Override
    public KvOperator<K, V> distinct() {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.distinct();
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> cache() {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.cache();
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> rePartition(int numPartition) {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.rePartition(numPartition);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvDataSet<K, Iterable<V>> groupByKey() {
        Partitioner<K> partitioner = new HashPartitioner<>(dataSet.numPartitions());
        ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(this, partitioner);
        ShuffledOperator<K, V> shuffleReducer = new ShuffledOperator<>(shuffleMapper);

        return new KvOperator<>(new FullAggOperator<>(shuffleReducer, x -> x));
    }


    @Override
    public KvDataSet<K, V> partitionBy(Partitioner<K> partitioner) {
        //todo: return newKvOperator
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        return this;
    }

    @Override
    public KvDataSet<K, V> partitionBy(int numPartitions) {
        return partitionBy(new HashPartitioner<>(numPartitions));
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer) {
        ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, partitioner);
        ShuffledOperator<K, V> shuffledOperator = new ShuffledOperator<>(shuffleMapper);
        return new KvOperator<>(new AggOperator<>(shuffledOperator, reducer));
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet) {
        return join(kvDataSet, Iterators.JoinMode.LEFT_JOIN);
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet) {
        return join(kvDataSet, Iterators.JoinMode.INNER_JOIN);
    }

    private <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet, Iterators.JoinMode joinMode) {
        checkState(kvDataSet instanceof Operator, kvDataSet + "not instanceof Operator");
        Operator<Tuple2<K, Iterable<?>[]>> joinOperator = new JoinOperator<>(partitioner, dataSet, kvDataSet);

        Operator<Tuple2<K, Tuple2<V, W>>> operator = joinOperator.flatMapIterator(x -> {
            Iterable<V> v = (Iterable<V>) x.f2()[0];
            Iterable<W> w = (Iterable<W>) x.f2()[1];

            Iterator<Tuple2<V, W>> iterator = Iterators.cartesian(v, w, joinMode);
            return Iterators.map(iterator, it -> new Tuple2<>(x.f1(), it));
        });
        return new KvOperator<>(operator);
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext) {
        return dataSet.compute(split, taskContext);
    }
}
