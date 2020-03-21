package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.ashtarte.api.function.Foreach;
import com.github.harbby.ashtarte.api.function.KeyedFunction;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public abstract class Operator<ROW>
        implements DataSet<ROW>
{
    private static final AtomicInteger nextDataSetId = new AtomicInteger(0);  //发号器
    private final transient MppContext context;
    private final int id = nextDataSetId.getAndIncrement();
    private final Operator<?>[] dataSets;

    public Operator(Operator<?>... dataSets)
    {
        checkState(dataSets != null && dataSets.length > 0, "dataSet is Empty");
        this.dataSets = Stream.of(dataSets).map(Operator::unboxing).toArray(Operator<?>[]::new);
        this.context = requireNonNull(dataSets[0].getContext(), "context is null");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected static <E> Operator<E> unboxing(Operator<E> operator)
    {
        if (operator instanceof KvOperator) {
            return ((KvOperator) operator).getDataSet();
        }
        else {
            return operator;
        }
    }

    protected Operator(MppContext context)
    {
        this.context = requireNonNull(context, "context is null");
        this.dataSets = new Operator<?>[0];
    }

    @Override
    public final int getId()
    {
        return id;
    }

    @Override
    public final MppContext getContext()
    {
        return context;
    }

    @Override
    public int numPartitions()
    {
        return getPartitions().length;
    }

    @Override
    public Partitioner getPartitioner()
    {
        return null;
    }

    protected final Operator<?> lastParent()
    {
        return getDependencies().get(getDependencies().size() - 1);
    }

    public List<? extends Operator<?>> getDependencies()
    {
        return Arrays.asList(dataSets);
    }

    @Override
    public Partition[] getPartitions()
    {
        Operator<?> dataSet = lastParent();
        checkState(dataSet != null, this.getClass()
                + " Parent Operator is null, Source Operator must @Override this Method");
        return dataSet.getPartitions();
    }

    protected abstract Iterator<ROW> compute(Partition split, TaskContext taskContext);

    private final AtomicBoolean markedCache = new AtomicBoolean(false);

    public final Iterator<ROW> computeOrCache(Partition split, TaskContext taskContext)
    {
        if (markedCache.get()) {
            return CacheOperator.compute(this, id, split, taskContext);
        }
        else {
            return this.compute(split, taskContext);
        }
    }

    @Override
    public DataSet<ROW> cache()
    {
//        markedCache.set(true);
//        return this;
        return new CacheOperator<>(this);
    }

    @Override
    public DataSet<ROW> distinct()
    {
        return this.distinct(numPartitions());
    }

    @Override
    public DataSet<ROW> distinct(int numPartition)
    {
        return this.kvDataSet(x -> new Tuple2<>(x, null))
                .reduceByKey((x, y) -> x, numPartition)
                .map(Tuple2::f1);  //使用map()更安全，因为我们不能将Partitioner传递下去
        //.keys();  // 这里不推荐使用keys(),使用.map() 更加强调不会传递Partitioner
    }

    @Override
    public DataSet<ROW> rePartition(int numPartition)
    {
        ShuffleMapOperator<ROW, ROW> shuffleMapOperator =
                new ShuffleMapOperator<>(this.map(x -> new Tuple2<>(x, null)), numPartition);
        ShuffledOperator<ROW, ROW> shuffleReducer = new ShuffledOperator<>(shuffleMapOperator, shuffleMapOperator.getPartitioner());
        return shuffleReducer.map(Tuple2::f1);
    }

    @Override
    public <K, V> KvOperator<K, V> kvDataSet(Mapper<ROW, Tuple2<K, V>> kvMapper)
    {
        Operator<Tuple2<K, V>> mapOperator = this.map(kvMapper);
        return new KvOperator<>(mapOperator);
    }

    @Override
    public <OUT> Operator<OUT> map(Mapper<ROW, OUT> mapper)
    {
        return new MapPartitionOperator<>(this,
                it -> Iterators.map(it, mapper::map)
                , false);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(Mapper<ROW, OUT[]> flatMapper)
    {
        return new FlatMapOperator<>(this, flatMapper);
    }

    @Override
    public <OUT> Operator<OUT> flatMapIterator(Mapper<ROW, Iterator<OUT>> flatMapper)
    {
        return new FlatMapIteratorOperator<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> mapPartition(Mapper<Iterator<ROW>, Iterator<OUT>> flatMapper)
    {
        return new MapPartitionOperator<>(this, flatMapper, false);
    }

    @Override
    public DataSet<ROW> filter(Filter<ROW> filter)
    {
        return new MapPartitionOperator<>(
                this,
                it -> Iterators.filter(it, filter::filter),
                false);
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
    public <KEY> KeyedFunction<KEY, ROW> groupBy(Mapper<ROW, KEY> keyGetter, Partitioner partitioner)
    {
        return new KeyedDataSet<>(this, keyGetter, partitioner);
    }

    //---action operator
    @Override
    public List<ROW> collect()
    {
        //todo: 使用其他比ImmutableList复杂度更低的操作
        return context.runJob(this, x -> x)
                .stream()
                .flatMap(Iterators::toStream)
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
    public void foreachPartition(Foreach<Iterator<ROW>> partitionForeach)
    {
        context.runJob(this, iterator -> {
            partitionForeach.apply(iterator);
            return true;
        });
    }

    @Override
    public void print(int limit)
    {
        context.runJob(this, iterator -> {
            Iterator<ROW> limitIterator = Iterators.limit(iterator, limit);
            while (limitIterator.hasNext()) {
                System.out.println((limitIterator.next()));
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
}
