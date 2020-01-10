package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.utils.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.ashtarte.api.function.KeyGetter;
import com.github.harbby.ashtarte.api.function.Reducer;

import java.util.Iterator;

/**
 * shuffle
 */
public class KeyedDataSet<KEY, ROW>
        implements KeyedFunction<KEY, ROW>
{
    private final KeyGetter<ROW, KEY> keyGetter;
    private final Operator<ROW> oneParent;

    protected KeyedDataSet(Operator<ROW> oneParent, KeyGetter<ROW, KEY> keyGetter)
    {
        this.keyGetter = keyGetter;
        this.oneParent = oneParent;
    }

    @Override
    public DataSet<Tuple2<KEY, Long>> count()
    {
        return agg(x -> 1L, (x, y) -> x + y);
    }

    @Override
    public DataSet<Tuple2<KEY, Double>> sum(KeyGetter<ROW, Double> keyGetter)
    {
        return agg(keyGetter, (x, y) -> x + y);
    }

//    @Override
//    public <VALUE> DataSet<Tuple2<KEY, VALUE>> max(KeyGetter<ROW, VALUE> keyGetter)
//    {
//        return null;
//    }

    @Override
    public DataSet<Tuple2<KEY, Double>> avg(KeyGetter<ROW, Double> keyGetter)
    {
        return agg(keyGetter, iterator -> {
            int cnt = 0;
            double sum = 0.0d;
            while (iterator.hasNext()) {
                sum += iterator.next();
                cnt++;
            }
            return cnt == 0 ? 0 : sum / cnt;
        });
    }

    @Override
    public <VALUE> DataSet<Tuple2<KEY, VALUE>> map(Mapper<Iterator<ROW>, VALUE> mapperReduce)
    {
        return agg(x -> x, mapperReduce);
    }

    @Override
    public <VALUE> DataSet<Tuple2<KEY, VALUE>> agg(KeyGetter<ROW, VALUE> aggIf, Reducer<VALUE> reducer)
    {
        Mapper<Iterator<VALUE>, VALUE> mapperReduce = iterator -> Iterators.reduce(iterator, reducer::reduce);
        return agg(aggIf, mapperReduce);
    }

    private <AggValue, VALUE> DataSet<Tuple2<KEY, VALUE>> agg(KeyGetter<ROW, AggValue> aggIf, Mapper<Iterator<AggValue>, VALUE> mapperReduce)
    {
        return new ShuffleOperator<>(oneParent, keyGetter, aggIf, mapperReduce);
    }
}
