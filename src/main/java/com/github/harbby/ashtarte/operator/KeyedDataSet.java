package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

/**
 * shuffle
 * <p>
 * 这个类不支持序列化，不能在这里类里面写lambda。(但可以static方法中写)
 */
@Deprecated
public class KeyedDataSet<K, ROW>
        implements KeyedFunction<K, ROW>
{
    private final Mapper<ROW, K> keyGetter;
    private final Operator<ROW> dataSet;
    private final Partitioner partitioner;

    protected KeyedDataSet(Operator<ROW> dataSet, Mapper<ROW, K> keyGetter, int numReduce)
    {
        this(dataSet, keyGetter, new HashPartitioner(numReduce));
    }

    protected KeyedDataSet(Operator<ROW> dataSet, Mapper<ROW, K> keyGetter)
    {
        this(dataSet, keyGetter, new HashPartitioner(dataSet.numPartitions()));
    }

    protected KeyedDataSet(Operator<ROW> dataSet, Mapper<ROW, K> keyGetter, Partitioner partitioner)
    {
        this.keyGetter = keyGetter;
        this.dataSet = dataSet;
        this.partitioner = partitioner;
    }

    @Override
    public KvDataSet<K, Long> count()
    {
        return agg(x -> 1L, (x, y) -> x + y);
    }

    @Override
    public KvDataSet<K, Double> avg(Mapper<ROW, Double> aggIf)
    {
        final Mapper<Iterable<Double>, Double> reducer = iterable -> {
            int cnt = 0;
            double sum = 0.0d;
            for (double v : iterable) {
                sum += v;
                cnt++;
            }
            return cnt == 0 ? 0 : sum / cnt;
        };

        Operator<Tuple2<K, Double>> kvDs = dataSet.kvDataSet(x ->
                new Tuple2<>(keyGetter.map(x), aggIf.map(x)));
        ShuffleMapOperator<K, Double> shuffleMapper = new ShuffleMapOperator<>(kvDs, partitioner);
        ShuffledOperator<K, Double> shuffleReducer = new ShuffledOperator<>(shuffleMapper, partitioner);
        return new KvOperator<>(new FullAggOperator<>(shuffleReducer, reducer));
    }

    @Override
    public <VALUE> KvDataSet<K, VALUE> map(Mapper<Iterable<ROW>, VALUE> mapperReduce)
    {
        Operator<Tuple2<K, ROW>> kvDs = dataSet.kvDataSet(x -> new Tuple2<>(keyGetter.map(x), x));

        ShuffleMapOperator<K, ROW> shuffleMapper = new ShuffleMapOperator<>(kvDs, partitioner);
        ShuffledOperator<K, ROW> shuffleReducer = new ShuffledOperator<>(shuffleMapper, partitioner);
        return new KvOperator<>(new FullAggOperator<>(shuffleReducer, mapperReduce));
    }

    @Override
    public <V> KvDataSet<K, V> agg(Mapper<ROW, V> aggIf, Reducer<V> reducer)
    {
        Operator<Tuple2<K, V>> kvDs = toKvDataSet(dataSet, keyGetter, aggIf);
        return new KvOperator<>(kvDs).reduceByKey(reducer);
    }

    private static <ROW, K, V> Operator<Tuple2<K, V>> toKvDataSet(Operator<ROW> dataSet, Mapper<ROW, K> keyGetter, Mapper<ROW, V> valueGetter)
    {
        return dataSet.map(x -> new Tuple2<>(keyGetter.map(x), valueGetter.map(x)));
    }
}
