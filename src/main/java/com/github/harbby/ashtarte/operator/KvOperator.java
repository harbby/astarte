package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class KvOperator<K, V>
        extends Operator<Tuple2<K, V>>
        implements KvDataSet<K, V>
{
    private final Operator<Tuple2<K, V>> dataSet;
    private Partitioner<K> partitioner;

    public KvOperator(Operator<Tuple2<K, V>> dataSet)
    {
        super(dataSet);
        this.dataSet = dataSet;
        this.partitioner = new HashPartitioner<>(dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, V> partitionBy(Partitioner<K> partitioner)
    {
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        return this;
    }

    @Override
    public KvDataSet<K, V> partitionBy(int numPartitions)
    {
        return partitionBy(new HashPartitioner<>(numPartitions));
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer)
    {
        ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, partitioner);
        ShuffledOperator<K, V> shuffledOperator = new ShuffledOperator<>(shuffleMapper);
        return new KvOperator<>(new AggOperator<>(shuffledOperator, reducer));
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet)
    {
        checkState(kvDataSet instanceof Operator, kvDataSet + "not instanceof Operator");
        Operator<Tuple2<K, Iterable<?>[]>> joinOperator = new JoinOperator<>(partitioner, dataSet, kvDataSet);

        DataSet<Tuple2<K, Tuple2<V, W>>> operator = joinOperator.map(x -> {
            Iterable<V> v = (Iterable<V>) x.f2()[0];
            Iterable<W> w = (Iterable<W>) x.f2()[1];
            return new Tuple2<>(x.f1(), new Tuple2<>(v, w));
        }).flatMapIterator(x -> {
            Tuple2<Iterable<V>, Iterable<W>> values = x.f2();
            return StreamSupport.stream(values.f1().spliterator(), false)
                    .flatMap(x2 -> StreamSupport.stream(values.f2().spliterator(), false).map(x3 -> new Tuple2<>(x2, x3)))
                    .map(x3 -> new Tuple2<>(x.f1(), x3))
                    .iterator();
        });
        return operator.kvDataSet(k -> k.f1(), v -> v.f2());
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> iterator = dataSet.compute(split, taskContext);
        return iterator;
    }
}
