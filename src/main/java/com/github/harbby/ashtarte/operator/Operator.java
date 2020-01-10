package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.ashtarte.api.function.FlatMapper;
import com.github.harbby.ashtarte.api.function.Foreach;
import com.github.harbby.ashtarte.api.function.KeyGetter;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.ashtarte.utils.Iterators;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public abstract class Operator<ROW>
        implements DataSet<ROW>
{
    private static final AtomicInteger nextDataSetId = new AtomicInteger(0);  //发号器
    private final transient MppContext yarkContext;
    private final Operator<?> oneParent;
    private final int id = nextDataSetId.getAndIncrement();

    protected Operator(MppContext yarkContext)
    {
        this(yarkContext, null);
    }

    private Operator(MppContext yarkContext, Operator<?> oneParent)
    {
        this.yarkContext = yarkContext;
        this.oneParent = oneParent;
    }

    protected Operator(Operator<?> oneParent)
    {
        this(oneParent.getContext(), oneParent);
    }

    @Override
    public int getId()
    {
        return id;
    }

    @Override
    public MppContext getContext()
    {
        return yarkContext;
    }

    public Operator<?> firstParent()
    {
        return oneParent;
    }

    @Override
    public Partition[] getPartitions()
    {
        checkState(oneParent != null, this.getClass() + " Parent Operator is null, Source Operator mush @Override Method");
        return oneParent.getPartitions();
    }

    public abstract Iterator<ROW> compute(Partition split);

    @Override
    public DataSet<ROW> cache()
    {
        return new CacheOperator<>(this);
    }

    @Override
    public <OUT> DataSet<OUT> map(Mapper<ROW, OUT> mapper)
    {
        return new MapDataSet<>(this, mapper);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(Mapper<ROW, OUT[]> flatMapper)
    {
        return new FlatMapDataSet<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(FlatMapper<ROW, OUT> flatMapper)
    {
        return new FlatMapDataSet<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> mapPartition(FlatMapper<Iterator<ROW>, OUT> flatMapper)
    {
        return new MapPartitionOperator<>(this, flatMapper);
    }

    @Override
    public DataSet<ROW> filter(Filter<ROW> filter)
    {
        return new FilterPartitionDataSet<>(this, filter);
    }

    @Override
    public <KEY> KeyedFunction<KEY, ROW> groupBy(KeyGetter<ROW, KEY> keyGetter)
    {
        return new KeyedDataSet<>(this, keyGetter);
    }

    //---action operator
    @Override
    public List<ROW> collect()
    {
        //todo: 使用其他比ImmutableList复杂度更低的操作
        return yarkContext.runJob(this, ImmutableList::copyOf).stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public long count()
    {
        return yarkContext.runJob(this, Iterators::size).stream().mapToLong(x -> x).sum();
    }

    @Override
    public Optional<ROW> reduce(Reducer<ROW> reducer)
    {
        return yarkContext.runJob(this, iterator -> Iterators.reduce(iterator, reducer::reduce))
                .stream().reduce(reducer::reduce);
    }

    @Override
    public void foreach(Foreach<ROW> foreach)
    {
        yarkContext.runJob(this, iterator -> {
            while (iterator.hasNext()) {
                foreach.apply(iterator.next());
            }
            return true;
        });
    }

    @Override
    public void foreachPartition(Foreach<Iterator<ROW>> partitionForeach)
    {
        yarkContext.runJob(this, iterator -> {
            partitionForeach.apply(iterator);
            return true;
        });
    }
}
