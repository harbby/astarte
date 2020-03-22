package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.Filter;
import com.github.harbby.ashtarte.api.function.Foreach;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.api.function.Reducer;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public abstract class Operator<ROW>
        implements DataSet<ROW>
{
    protected static final Logger logger = LoggerFactory.getLogger(Operator.class);

    private static final AtomicInteger nextDataSetId = new AtomicInteger(0);  //发号器
    private final transient MppContext context;
    private final int id = nextDataSetId.getAndIncrement();
    private final Operator<?>[] dataSets;

    public Operator(Operator<?>... dataSets)
    {
        checkState(dataSets != null && dataSets.length > 0, "dataSet is Empty");
        this.dataSets = Stream.of(dataSets).map(Operator::unboxing).toArray(Operator<?>[]::new);
        this.context = requireNonNull(dataSets[0].getContext(), "getContext is null " + dataSets[0]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected static <E> Operator<E> unboxing(Operator<E> operator)
    {
        requireNonNull(operator, "operator is null");
        if (operator instanceof KvOperator) {
            return ((KvOperator) operator).getDataSet();
        }
        else {
            return operator;
        }
    }

    @SuppressWarnings({"unchecked"})
    protected static <E> Operator<? extends E>[] unboxing(Operator<? extends E>[] operators)
    {
        Operator<? extends E>[] outArray = new Operator[operators.length];
        for (int i = 0; i < operators.length; i++) {
            outArray[i] = unboxing(operators[i]);
        }
        return outArray;
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

    public boolean isMarkedCache()
    {
        return markedCache;
    }

    protected abstract Iterator<ROW> compute(Partition split, TaskContext taskContext);

    private boolean markedCache = false;

    public final Iterator<ROW> computeOrCache(Partition split, TaskContext taskContext)
    {
        if (markedCache) {
            return CacheOperator.compute(this, id, split, taskContext);
        }
        else {
            return this.compute(split, taskContext);
        }
    }

    @Override
    public DataSet<ROW> cache(CacheOperator.CacheMode cacheMode)
    {
        checkState(cacheMode == CacheOperator.CacheMode.MEM_ONLY, "目前只支持mem模式");
        markedCache = true;
        return this;
    }

    /**
     * todo: 需重新实现
     */
    @Override
    public DataSet<ROW> unCache()
    {
        checkState(!(this instanceof KvOperator) && this.isMarkedCache(),
                "this DataSet not cached");
        //blocking = true
        //todo: 通过job触发 代价比较重(会出发额外的stage多计算)，后续应该改为通信解决(斩断dag血缘)
        context.runJob(unboxing(this), iterator -> {
            CacheOperator.unCacheExec(id);
            return true;
        });
        markedCache = false;
        return this;
    }

    @Override
    public DataSet<ROW> distinct()
    {
        return this.distinct(numPartitions());
    }

    @Override
    public DataSet<ROW> distinct(int numPartition)
    {
        return distinct(new HashPartitioner(numPartition));
    }

    @Override
    public DataSet<ROW> distinct(Partitioner partitioner)
    {
        return this.kvDataSet(x -> new Tuple2<>(x, null))
                .reduceByKey((x, y) -> x, partitioner)
                .map(Tuple2::f1);  //使用map()更安全，因为我们不能将Partitioner传递下去
        //.keys();  // 这里不推荐使用keys(),使用.map() 更加强调不会传递Partitioner;
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
    public DataSet<ROW> union(DataSet<ROW> dataSet)
    {
        return unionAll(dataSet).distinct();
    }

    @Override
    public DataSet<ROW> union(DataSet<ROW> dataSet, int numPartition)
    {
        return union(dataSet, new HashPartitioner(numPartition));
    }

    @Override
    public DataSet<ROW> union(DataSet<ROW> dataSets, Partitioner partitioner)
    {
        return unionAll(dataSets).distinct(partitioner);
    }

    @Override
    public DataSet<ROW> unionAll(DataSet<ROW> dataSet)
    {
        requireNonNull(dataSet, "dataSet is null");
        checkState(dataSet instanceof Operator, dataSet + "not instanceof Operator");
        return new UnionAllOperator<>(this, (Operator<ROW>) dataSet);
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

    //---action operator
    @Override
    public List<ROW> collect()
    {
        //todo: 使用其他比ImmutableList复杂度更低的操作
        return context.runJob(unboxing(this), x -> x)
                .stream()
                .flatMap(Iterators::toStream)
                .collect(Collectors.toList());
    }

    @Override
    public long count()
    {
        return context.runJob(unboxing(this), Iterators::size).stream().mapToLong(x -> x).sum();
    }

    @Override
    public Optional<ROW> reduce(Reducer<ROW> reducer)
    {
        return context.runJob(unboxing(this), iterator -> Iterators.reduce(iterator, reducer::reduce))
                .stream().reduce(reducer::reduce);
    }

    @Override
    public void foreach(Foreach<ROW> foreach)
    {
        context.runJob(unboxing(this), iterator -> {
            while (iterator.hasNext()) {
                foreach.apply(iterator.next());
            }
            return true;
        });
    }

    @Override
    public void foreachPartition(Foreach<Iterator<ROW>> partitionForeach)
    {
        context.runJob(unboxing(this), iterator -> {
            partitionForeach.apply(iterator);
            return true;
        });
    }

    @Override
    public void print(int limit)
    {
        context.runJob(unboxing(this), iterator -> {
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
        context.runJob(unboxing(this), iterator -> {
            while (iterator.hasNext()) {
                System.out.println((iterator.next()));
            }
            return true;
        });
    }
}
