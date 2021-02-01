/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.HashPartitioner;
import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.Utils;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.Filter;
import com.github.harbby.astarte.core.api.function.Foreach;
import com.github.harbby.astarte.core.api.function.KvMapper;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.api.function.Reducer;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public abstract class Operator<R>
        implements DataSet<R>
{
    protected static final Logger logger = LoggerFactory.getLogger(Operator.class);

    private static final AtomicInteger nextDataSetId = new AtomicInteger(0);  //发号器
    private final transient BatchContext context;
    private final int id = nextDataSetId.getAndIncrement();
    private final List<Operator<?>> dataSets;

    public Operator(Operator<?>... dataSets)
    {
        checkState(dataSets != null && dataSets.length > 0, "dataSet is Empty");
        this.dataSets = Stream.of(dataSets).map(Operator::unboxing).collect(Collectors.toList());
        this.context = requireNonNull(dataSets[0].getContext(), "getContext is null " + dataSets[0]);
    }

    protected Operator(BatchContext context)
    {
        this.context = requireNonNull(context, "context is null");
        this.dataSets = new ArrayList<>();
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

    @Override
    public final int getId()
    {
        return id;
    }

    @Override
    public final BatchContext getContext()
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
        return ImmutableList.copy(dataSets);
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

    protected abstract Iterator<R> compute(Partition split, TaskContext taskContext);

    private boolean markedCache = false;

    public final Iterator<R> computeOrCache(Partition split, TaskContext taskContext)
    {
        if (markedCache) {
            return CacheOperator.compute(this, id, split, taskContext);
        }
        else {
            return this.compute(split, taskContext);
        }
    }

    @Override
    public DataSet<R> cache()
    {
        return this.cache(CacheOperator.CacheMode.MEM_ONLY);
    }

    @Override
    public DataSet<R> cache(CacheOperator.CacheMode cacheMode)
    {
        checkState(cacheMode == CacheOperator.CacheMode.MEM_ONLY, "目前只支持mem模式");
        markedCache = true;
        return this;
    }

    /**
     * todo: 需重新实现
     */
    @Override
    public DataSet<R> unCache()
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
    public DataSet<R> partitionLimit(int limit)
    {
        return this.mapPartition(input -> Iterators.limit(input, limit));
    }

    @Override
    public DataSet<R> limit(int limit)
    {
        //todo: 如果上一个算子是排序,则还可以进一步下推优化
        int[] partitionSize = new int[numPartitions()];
        int[] partitionLimit = new int[partitionSize.length];
        this.mapPartitionWithId((id, iterator) ->
                Iterators.of(new int[] {id, (int) Iterators.size(Iterators.limit(iterator, limit))}))
                .collect().forEach(it -> partitionSize[it[0]] = it[1]);
        int diff = limit;
        for (int i = 0; i < numPartitions() && diff > 0; i++) {
            if (diff >= partitionSize[i]) {
                partitionLimit[i] = partitionSize[i];
                diff = diff - partitionSize[i];
            }
            else {
                partitionLimit[i] = diff;
                break;
            }
        }
        return this.mapPartitionWithId((id, iterator) -> Iterators.limit(iterator, partitionLimit[id]));
    }

    @Override
    public DataSet<R> distinct()
    {
        return this.distinct(numPartitions());
    }

    @Override
    public DataSet<R> distinct(int numPartition)
    {
        return distinct(new HashPartitioner(numPartition));
    }

    @Override
    public DataSet<R> distinct(Partitioner partitioner)
    {
        return this.kvDataSet(x -> new Tuple2<>(x, null))
                .reduceByKey((x, y) -> x, partitioner)
                .map(Tuple2::f1);  //使用map()更安全，因为我们不能将Partitioner传递下去
        //.keys();  // 这里不推荐使用keys(),使用.map() 更加强调不会传递Partitioner;
    }

    @Override
    public DataSet<R> rePartition(int numPartition)
    {
        ShuffleMapOperator<R, R> shuffleMapOperator =
                new ShuffleMapOperator<>(this.map(x -> new Tuple2<>(x, null)), numPartition);
        ShuffledOperator<R, R> shuffleReducer = new ShuffledOperator<>(shuffleMapOperator, shuffleMapOperator.getPartitioner());
        return shuffleReducer.map(Tuple2::f1);
    }

    @Override
    public DataSet<R> union(DataSet<R> dataSet)
    {
        return unionAll(dataSet).distinct();
    }

    @Override
    public DataSet<R> union(DataSet<R> dataSet, int numPartition)
    {
        return union(dataSet, new HashPartitioner(numPartition));
    }

    @Override
    public DataSet<R> union(DataSet<R> dataSets, Partitioner partitioner)
    {
        return unionAll(dataSets).distinct(partitioner);
    }

    @Override
    public DataSet<R> unionAll(DataSet<R> dataSet)
    {
        requireNonNull(dataSet, "dataSet is null");
        checkState(dataSet instanceof Operator, dataSet + "not instanceof Operator");
        return new UnionAllOperator<>(this, (Operator<R>) dataSet);
    }

    @Override
    public <K, V> KvOperator<K, V> kvDataSet(Mapper<R, Tuple2<K, V>> kvMapper)
    {
        requireNonNull(kvMapper, "kvMapper is null");
        Mapper<R, Tuple2<K, V>> clearedFunc = Utils.clear(kvMapper);
        Operator<Tuple2<K, V>> mapOperator = this.map(clearedFunc);
        return new KvOperator<>(mapOperator);
    }

    @Override
    public <K> KeyValueGroupedOperator<K, R> groupByKey(Mapper<R, K> mapFunc)
    {
        requireNonNull(mapFunc, "mapFunc is null");
        Mapper<R, K> clearedFunc = Utils.clear(mapFunc);
        return new KeyValueGroupedOperator<>(this, clearedFunc);
    }

    @Override
    public <O> Operator<O> map(Mapper<R, O> mapper)
    {
        requireNonNull(mapper, "mapper is null");
        Mapper<R, O> clearedFunc = Utils.clear(mapper);
        return new MapPartitionOperator<>(
                this,
                it -> Iterators.map(it, clearedFunc::map),
                false);
    }

    @Override
    public <O> DataSet<O> flatMap(Mapper<R, O[]> flatMapper)
    {
        requireNonNull(flatMapper, "flatMapper is null");
        Mapper<R, O[]> clearedFunc = Utils.clear(flatMapper);
        return new FlatMapOperator<>(this, clearedFunc);
    }

    @Override
    public <O> Operator<O> flatMapIterator(Mapper<R, Iterator<O>> flatMapper)
    {
        requireNonNull(flatMapper, "flatMapper is null");
        Mapper<R, Iterator<O>> clearedFunc = Utils.clear(flatMapper);
        return new FlatMapIteratorOperator<>(this, clearedFunc);
    }

    @Override
    public <O> DataSet<O> mapPartition(Mapper<Iterator<R>, Iterator<O>> mapper)
    {
        requireNonNull(mapper, "mapper is null");
        Mapper<Iterator<R>, Iterator<O>> clearedFunc = Utils.clear(mapper);
        return new MapPartitionOperator<>(this, clearedFunc, false);
    }

    @Override
    public <O> DataSet<O> mapPartitionWithId(KvMapper<Integer, Iterator<R>, Iterator<O>> mapper)
    {
        requireNonNull(mapper, "mapper is null");
        KvMapper<Integer, Iterator<R>, Iterator<O>> clearedFunc = Utils.clear(mapper);
        return new MapPartitionOperator<>(this, clearedFunc, false);
    }

    @Override
    public DataSet<R> filter(Filter<R> filter)
    {
        requireNonNull(filter, "filter is null");
        Filter<R> clearedFunc = Utils.clear(filter);
        return new MapPartitionOperator<>(
                this,
                it -> Iterators.filter(it, clearedFunc::filter),
                false);
    }

    @Override
    public KvDataSet<R, Long> zipWithIndex()
    {
        List<Tuple2<Integer, Long>> list = this.mapPartitionWithId((id, it) ->
                Iterators.of(new Tuple2<>(id, Iterators.size(it))))
                .collect()
                .stream()
                .sorted((x, y) -> x.f1().compareTo(y.f1()))
                .collect(Collectors.toList());

        long index = 0;
        Map<Integer, Long> info = new HashMap<>();
        for (Tuple2<Integer, Long> it : list) {
            info.put(it.f1(), index);
            index = index + it.f2();
        }
        Operator<Tuple2<R, Long>> operator = (Operator<Tuple2<R, Long>>) this.mapPartitionWithId((id, it) ->
                Iterators.zipIndex(it, info.get(id)));
        return new KvOperator<>(operator);
    }

    //---action operator
    @Override
    public List<R> collect()
    {
        //todo: 使用其他比ImmutableList复杂度更低的操作
        return context.runJob(unboxing(this), MutableList::copy)
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public long count()
    {
        return context.runJob(unboxing(this), Iterators::size)
                .stream()
                .mapToLong(x -> x)
                .sum();
    }

    @Override
    public Optional<R> reduce(Reducer<R> reducer)
    {
        requireNonNull(reducer, "reducer is null");
        Reducer<R> clearedFunc = Utils.clear(reducer);
        return context.runJob(unboxing(this), iterator -> Iterators.reduce(iterator, clearedFunc::reduce))
                .stream().reduce(clearedFunc::reduce);
    }

    @Override
    public void foreach(Foreach<R> foreach)
    {
        requireNonNull(foreach, "foreach is null");
        Foreach<R> clearedFunc = Utils.clear(foreach);
        context.runJob(unboxing(this), iterator -> {
            while (iterator.hasNext()) {
                clearedFunc.apply(iterator.next());
            }
            return null;
        });
    }

    @Override
    public void foreachPartition(Foreach<Iterator<R>> partitionForeach)
    {
        requireNonNull(partitionForeach, "partitionForeach is null");
        Foreach<Iterator<R>> clearedFunc = Utils.clear(partitionForeach);

        context.runJob(unboxing(this), iterator -> {
            clearedFunc.apply(iterator);
            return Iterators.empty();
        });
    }

    @Override
    public void print(int limit)
    {
        context.runJob(unboxing(this), iterator -> {
            Iterator<R> limitIterator = Iterators.limit(iterator, limit);
            while (limitIterator.hasNext()) {
                System.out.println((limitIterator.next()));
            }
            return null;
        });
    }

    @Override
    public void print()
    {
        context.runJob(unboxing(this), iterator -> {
            while (iterator.hasNext()) {
                System.out.println((iterator.next()));
            }
            return null;
        });
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("operatorId", id)
                .toString();
    }
}
