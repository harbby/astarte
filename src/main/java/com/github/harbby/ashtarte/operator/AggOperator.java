package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * this pipiline agg Operator
 */
public class AggOperator<K, V>
        extends Operator<Tuple2<K, V>>
{
    private final Operator<Tuple2<K, V>> operator;
    private final Mapper<Iterator<V>, V> agg;

    public AggOperator(Operator<Tuple2<K, V>> operator, Reducer<V> reducer)
    {
        super(operator);
        this.operator = operator;
        this.agg = iterator -> Iterators.reduce(iterator, reducer::reduce);
    }

    public AggOperator(Operator<Tuple2<K, V>> operator, Mapper<Iterator<V>, V> reducer)
    {
        super(operator);
        this.operator = operator;
        this.agg = reducer;
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Iterator<Tuple2<K, V>> input = operator.compute(split, taskContext);

        //todo: 需要增量计算
        Iterator<Tuple2<K, Iterator<V>>> input1 = Iterators.toStream(input)
                .collect(Collectors.groupingBy(Tuple2::f1))
                .entrySet().stream()
                .map(x -> new Tuple2<>(x.getKey(), x.getValue().stream().map(Tuple2::f2).iterator()))
                .iterator();

        return Iterators.map(input1, x -> new Tuple2<>(x.f1(), agg.map(x.f2())));
    }
}
