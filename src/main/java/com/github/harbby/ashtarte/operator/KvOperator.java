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

        ShuffleMapOperator<K, V> shuffleMapper1 = new ShuffleMapOperator<>(dataSet, partitioner);
        ShuffleMapOperator<K, W> shuffleMapper2 = new ShuffleMapOperator<>((Operator<Tuple2<K, W>>) kvDataSet
                , partitioner);

        Operator<Tuple2<K, Tuple2<V, W>>> operator = new JoinOperator<>(shuffleMapper1, shuffleMapper2);
        return operator.kvDataSet(k->k.f1(), v->v.f2());
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> iterator = dataSet.compute(split, taskContext);
        return iterator;
    }
}
