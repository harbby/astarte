package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.AggFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.mutable.MutableSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class KvOperator<K, V>
        extends Operator<Tuple2<K, V>>
        implements KvDataSet<K, V>
{
    private final Operator<Tuple2<K, V>> dataSet;

    public KvOperator(Operator<Tuple2<K, V>> dataSet)
    {
        super(dataSet);
        this.dataSet = dataSet;
    }

    @Override
    public Partitioner getPartitioner()
    {
        return dataSet.getPartitioner();
    }

    @Override
    public DataSet<K> keys()
    {
        return new MapPartitionOperator<>(dataSet,
                it -> Iterators.map(it, Tuple2::f1),
                false); //如果想需要保留分区器，则请使用mapValues
    }

    @Override
    public <OUT> KvDataSet<K, OUT> mapValues(Mapper<V, OUT> mapper)
    {
        Operator<Tuple2<K, OUT>> out = new MapPartitionOperator<>(
                this.dataSet,
                it -> Iterators.map(it, kv -> new Tuple2<>(kv.f1(), mapper.map(kv.f2()))),
                true);
        return new KvOperator<>(out);
    }

    @Override
    public <OUT> KvDataSet<K, OUT> flatMapValues(Mapper<V, Iterator<OUT>> mapper)
    {
        Mapper<Iterator<Tuple2<K, V>>, Iterator<Tuple2<K, OUT>>> flatMapper =
                input -> Iterators.flatMap(input,
                        kv -> Iterators.map(mapper.map(kv.f2()), o -> new Tuple2<>(kv.f1(), o)));

        Operator<Tuple2<K, OUT>> dataSet = new MapPartitionOperator<>(
                this.dataSet,
                flatMapper,
                true);
        return new KvOperator<>(dataSet);
    }

    @Override
    public DataSet<V> values()
    {
        return dataSet.map(Tuple2::f2);
    }

    @Override
    public KvDataSet<K, V> distinct()
    {
        return this.distinct(this.numPartitions());
    }

    @Override
    public KvDataSet<K, V> distinct(int numPartition)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.distinct(numPartition);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> cache()
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.cache();
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> rePartition(int numPartition)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.rePartition(numPartition);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvDataSet<K, Iterable<V>> groupByKey()
    {
        Partitioner partitioner = dataSet.getPartitioner();
        if (new HashPartitioner(dataSet.numPartitions()).equals(partitioner)) {
            // 因为上一个stage已经按照相同的分区器, 将数据分好，因此这里我们无需shuffle
            return new KvOperator<>(new FullAggOperator<>(dataSet, x -> x));
        }
        else {
            // 进行shuffle
            ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, dataSet.numPartitions());
            ShuffledOperator<K, V> shuffleReducer = new ShuffledOperator<>(shuffleMapper);
            return new KvOperator<>(new FullAggOperator<>(shuffleReducer, x -> x));
        }
    }

    @Override
    public KvDataSet<K, V> partitionBy(Partitioner partitioner)
    {
        ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, partitioner);
        ShuffledOperator<K, V> shuffledOperator = new ShuffledOperator<>(shuffleMapper);
        return new KvOperator<>(shuffledOperator);
    }

    @Override
    public KvDataSet<K, V> partitionBy(int numPartitions)
    {
        return partitionBy(new HashPartitioner(numPartitions));
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer)
    {
        return reduceByKey(reducer, dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, int numPartition)
    {
        return reduceByKey(reducer, new HashPartitioner(dataSet.numPartitions()));
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, Partitioner partitioner)
    {
        if (partitioner.equals(dataSet.getPartitioner())) {
            // 因为上一个stage已经按照相同的分区器, 将数据分好，因此这里我们无需shuffle
            return new KvOperator<>(new AggOperator<>(dataSet, reducer));
        }
        else {
            // 进行shuffle
            ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, partitioner);
            ShuffledOperator<K, V> shuffledOperator = new ShuffledOperator<>(shuffleMapper);
            return new KvOperator<>(new AggOperator<>(shuffledOperator, reducer));
        }
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet)
    {
        return join(kvDataSet, Iterators.JoinMode.LEFT_JOIN);
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet)
    {
        return join(kvDataSet, Iterators.JoinMode.INNER_JOIN);
    }

    private <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> rightDataSet, Iterators.JoinMode joinMode)
    {
        checkState(rightDataSet instanceof Operator, rightDataSet + "not instanceof Operator");

        Operator<Tuple2<K, Iterable<?>[]>> joinOperator;
        Partitioner leftPartitioner = null; //dataSet.getPartitioner();
        Partitioner rightPartitioner = rightDataSet.getPartitioner();
        if (leftPartitioner != null && leftPartitioner.equals(rightPartitioner)) {
            // 因为上一个stage已经按照相同的分区器, 将数据分好，因此这里我们无需shuffle
            joinOperator = new LocalJoinOperator<>(leftPartitioner, dataSet, (Operator<Tuple2<K, W>>) rightDataSet);
        }
        else {
            Partitioner partitioner = new HashPartitioner(dataSet.numPartitions());
            joinOperator = new ShuffleJoinOperator<>(partitioner, dataSet, rightDataSet);
        }

        Operator<Tuple2<K, Tuple2<V, W>>> operator = joinOperator.flatMapIterator(x -> {
            @SuppressWarnings("unchecked")
            Iterable<V> v = (Iterable<V>) x.f2()[0];
            @SuppressWarnings("unchecked")
            Iterable<W> w = (Iterable<W>) x.f2()[1];

            Iterator<Tuple2<V, W>> iterator = Iterators.cartesian(v, w, joinMode);
            return Iterators.map(iterator, it -> new Tuple2<>(x.f1(), it));
        });
        return new KvOperator<>(operator);
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        return dataSet.compute(split, taskContext);
    }
}
