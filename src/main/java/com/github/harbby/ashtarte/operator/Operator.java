package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.ashtarte.api.function.FlatMapper;
import com.github.harbby.ashtarte.api.function.Foreach;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.ashtarte.utils.CheckUtil;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
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
    private final transient MppContext context;
    private final Operator<?> dataSet;
    private final int id = nextDataSetId.getAndIncrement();

    protected Operator(MppContext context)
    {
        this(context, null);
    }

    private Operator(MppContext context, Operator<?> dataSet)
    {
        this.context = context;
        this.dataSet = CheckUtil.checkSerialize(dataSet);
    }

    protected Operator(Operator<?> dataSet)
    {
        this(dataSet.getContext(), dataSet);
    }

    @Override
    public final int getId()
    {
        return id;
    }

    @Override
    public MppContext getContext()
    {
        return context;
    }

    @Override
    public int numPartitions()
    {
        return getPartitions().length;
    }

    @Override
    public Partitioner<?> getPartitioner()
    {
        return null;
    }

    public Operator<?> lastParent()
    {
        return dataSet;
    }

    @Override
    public Partition[] getPartitions()
    {
        checkState(dataSet != null, this.getClass() + " Parent Operator is null, Source Operator mush @Override Method");
        return dataSet.getPartitions();
    }

    public abstract Iterator<ROW> compute(Partition split, TaskContext taskContext);

    @Override
    public DataSet<ROW> cache()
    {
        return new CacheOperator<>(this);
    }

    @Override
    public <K, V> KvOperator<K, V> kvDataSet(Mapper<ROW, K> keyMapper, Mapper<ROW, V> valueMapper)
    {
        Operator<Tuple2<K, V>> mapOperator = this.map(x -> new Tuple2<>(keyMapper.map(x), valueMapper.map(x)));
        return new KvOperator<>(mapOperator);
    }

    @Override
    public DataSet<ROW> rePartition(int numPartition)
    {
        return new RePartitionOperator<>(this, numPartition);
    }

    @Override
    public <OUT> Operator<OUT> map(Mapper<ROW, OUT> mapper)
    {
        return new MapOperator<>(this, mapper);
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
    public <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter)
    {
        return new KeyedDataSet<>(this, keyGetter);
    }

    @Override
    public <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter, int numReduce)
    {
        return new KeyedDataSet<>(this, keyGetter, numReduce);
    }

    @Override
    public <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter, Partitioner<KEY> partitioner)
    {
        return new KeyedDataSet<>(this, keyGetter, partitioner);
    }

    //---action operator
    @Override
    public List<ROW> collect()
    {
        //todo: 使用其他比ImmutableList复杂度更低的操作
        return context.runJob(this, ImmutableList::copyOf).stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public long count()
    {
        return context.runJob(this, Iterators::size).stream().mapToLong(x -> x).sum();
    }

    @Override
    public Optional<ROW> reduce(Reducer<ROW> reducer)
    {
        return context.runJob(this, iterator -> Iterators.reduce(iterator, reducer::reduce))
                .stream().reduce(reducer::reduce);
    }

    @Override
    public void foreach(Foreach<ROW> foreach)
    {
        context.runJob(this, iterator -> {
            while (iterator.hasNext()) {
                foreach.apply(iterator.next());
            }
            return true;
        });
    }

    @Override
    public void print()
    {
        context.runJob(this, iterator -> {
            while (iterator.hasNext()) {
                System.out.println((iterator.next()));
            }
            return true;
        });
    }

    @Override
    public void foreachPartition(Foreach<Iterator<ROW>> partitionForeach)
    {
        context.runJob(this, iterator -> {
            partitionForeach.apply(iterator);
            return true;
        });
    }
}
