package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

/**
 * shuffle
 * <p>
 * 这个类不支持序列化，不能在这里类里面写lambda。(但可以static方法中写)
 */
public class KeyedDataSet<KEY, ROW>
        implements KeyedFunction<KEY, ROW>
{
    private final Mapper<ROW, KEY> keyGetter;
    private final Operator<ROW> dataSet;
    private final Partitioner<KEY> partitioner;

    protected KeyedDataSet(Operator<ROW> dataSet, Mapper<ROW, KEY> keyGetter, int numReduce)
    {
        this(dataSet, keyGetter, new HashPartitioner<>(numReduce));
    }

    protected KeyedDataSet(Operator<ROW> dataSet, Mapper<ROW, KEY> keyGetter)
    {
        this(dataSet, keyGetter, new HashPartitioner<>(dataSet.numPartitions()));
    }

    protected KeyedDataSet(Operator<ROW> dataSet, Mapper<ROW, KEY> keyGetter, Partitioner<KEY> partitioner)
    {
        this.keyGetter = keyGetter;
        this.dataSet = dataSet;
        this.partitioner = partitioner;
    }

    @Override
    public DataSet<Tuple2<KEY, Long>> count()
    {
        return agg(x -> 1L, (x, y) -> x + y);
    }

    @Override
    public DataSet<Tuple2<KEY, Double>> avg(Mapper<ROW, Double> aggIf)
    {
        final Mapper<Iterator<Double>, Double> mapper = iterator -> {
            int cnt = 0;
            double sum = 0.0d;
            while (iterator.hasNext()) {
                sum += iterator.next();
                cnt++;
            }
            return cnt == 0 ? 0 : sum / cnt;
        };

        Operator<Tuple2<KEY, Double>> kvDs = dataSet.kvDataSet(keyGetter, aggIf);
        ShuffleMapOperator<KEY, Double> shuffleMapper = new ShuffleMapOperator<>(kvDs, partitioner);
        ShuffledOperator<KEY, Double> shuffleReducer = new ShuffledOperator<>(shuffleMapper);
        return new AggOperator<>(shuffleReducer, mapper);
    }

    @Override
    public <VALUE> DataSet<Tuple2<KEY, VALUE>> map(Mapper<Iterator<ROW>, VALUE> mapperReduce)
    {
        Partitioner<KEY> partitioner = new HashPartitioner<>(dataSet.numPartitions());
        Operator<Tuple2<KEY, ROW>> kvDs = dataSet.kvDataSet(keyGetter, x -> x);

        ShuffleMapOperator<KEY, ROW> shuffleMapper = new ShuffleMapOperator<>(kvDs, partitioner);
        ShuffledOperator<KEY, ROW> shuffleReducer = new ShuffledOperator<>(shuffleMapper);
        return new FullAggOperator<>(shuffleReducer, mapperReduce);
    }

    @Override
    public <VALUE> DataSet<Tuple2<KEY, VALUE>> agg(Mapper<ROW, VALUE> aggIf, Reducer<VALUE> reducer)
    {
        Operator<Tuple2<KEY, VALUE>> kvDs = dataSet.kvDataSet(keyGetter, aggIf);
        ShuffleMapOperator<KEY, VALUE> shuffleMapper = new ShuffleMapOperator<>(kvDs, partitioner);
        ShuffledOperator<KEY, VALUE> shuffleReducer = new ShuffledOperator<>(shuffleMapper);
        return new AggOperator<>(shuffleReducer, reducer);
    }
}
