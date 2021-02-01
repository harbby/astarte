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

import com.github.harbby.astarte.core.HashPartitioner;
import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.Utils;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.api.function.KvForeach;
import com.github.harbby.astarte.core.api.function.KvMapper;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.api.function.Reducer;
import com.github.harbby.astarte.core.deprecated.JoinExperiment;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class KvOperator<K, V>
        extends Operator<Tuple2<K, V>>
        implements KvDataSet<K, V>
{
    private final Operator<Tuple2<K, V>> dataSet;
    /*
     * 启用map端combine功能
     */
    private boolean combine = true;

    public KvOperator(Operator<Tuple2<K, V>> dataSet)
    {
        super(dataSet);
        this.dataSet = unboxing(dataSet);
    }

    public Operator<? extends Tuple2<K, V>> getDataSet()
    {
        return dataSet;
    }

    @Override
    public Partitioner getPartitioner()
    {
        return dataSet.getPartitioner();
    }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        return dataSet.computeOrCache(split, taskContext);
    }

    @Override
    public <O> DataSet<O> map(KvMapper<K, V, O> mapper)
    {
        KvMapper<K, V, O> clearedFunc = Utils.clear(mapper);
        return dataSet.map(x -> clearedFunc.map(x.f1(), x.f2()));
    }

    @Override
    public void foreach(KvForeach<K, V> kvKvForeach)
    {
        KvForeach<K, V> clearedFunc = Utils.clear(kvKvForeach);
        dataSet.foreach(x -> clearedFunc.foreach(x.f1(), x.f2()));
    }

    @Override
    public Map<K, V> collectMap()
    {
        return collect().stream().collect(Collectors.toMap(k -> k.f1, v -> v.f2));
    }

    @Override
    public DataSet<K> keys()
    {
        return new MapPartitionOperator<>(dataSet,
                it -> Iterators.map(it, Tuple2::f1),
                false); //如果想需要保留分区器，则请使用mapValues
    }

    @Override
    public <K1> KvDataSet<K1, V> mapKeys(Mapper<K, K1> mapper)
    {
        Mapper<K, K1> clearedFunc = Utils.clear(mapper);
        Operator<Tuple2<K1, V>> out = new MapPartitionOperator<>(dataSet,
                it -> Iterators.map(it, x -> new Tuple2<>(clearedFunc.map(x.f1()), x.f2())),
                false); //如果想需要保留分区器，则请使用mapValues
        return new KvOperator<>(out);
    }

    @Override
    public <O> KvDataSet<K, O> mapValues(Mapper<V, O> mapper)
    {
        Mapper<V, O> clearedFunc = Utils.clear(mapper);
        Operator<Tuple2<K, O>> out = new MapPartitionOperator<>(
                this.dataSet,
                it -> Iterators.map(it, kv -> new Tuple2<>(kv.f1(), clearedFunc.map(kv.f2()))),
                true);
        return new KvOperator<>(out);
    }

    @Override
    public <O> KvDataSet<K, O> flatMapValues(Mapper<V, Iterator<O>> mapper)
    {
        Mapper<V, Iterator<O>> clearedFunc = Utils.clear(mapper);
        Mapper<Iterator<Tuple2<K, V>>, Iterator<Tuple2<K, O>>> flatMapper =
                input -> Iterators.flatMap(input,
                        kv -> Iterators.map(clearedFunc.map(kv.f2()), o -> new Tuple2<>(kv.f1(), o)));

        Operator<Tuple2<K, O>> dataSet = new MapPartitionOperator<>(
                this.dataSet,
                flatMapper,
                true);
        return new KvOperator<>(dataSet);
    }

    @Override
    public DataSet<V> values()
    {
        return dataSet.map(Tuple2::f2);
    }

    @Override
    public KvDataSet<K, V> distinct()
    {
        return this.distinct(this.numPartitions());
    }

    @Override
    public KvDataSet<K, V> distinct(int numPartition)
    {
        return this.distinct(new HashPartitioner(numPartition));
    }

    @Override
    public KvDataSet<K, V> distinct(Partitioner partitioner)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.distinct(partitioner);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> cache()
    {
        this.dataSet.cache();
        return this;
    }

    @Override
    public KvDataSet<K, V> cache(CacheOperator.CacheMode cacheMode)
    {
        this.dataSet.cache(cacheMode);
        return this;
    }

    @Override
    public KvOperator<K, V> unCache()
    {
        this.dataSet.unCache();
        return this;
    }

    @Override
    public KvDataSet<K, V> partitionLimit(int limit)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.partitionLimit(limit);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> limit(int limit)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.limit(limit);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvOperator<K, V> rePartition(int numPartition)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.rePartition(numPartition);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvDataSet<K, Iterable<V>> groupByKey()
    {
        Partitioner partitioner = dataSet.getPartitioner();
        if (new HashPartitioner(dataSet.numPartitions()).equals(partitioner)) {
            // 因为上一个stage已经按照相同的分区器, 将数据分好，因此这里我们无需shuffle
            return new KvOperator<>(new FullAggOperator<>(dataSet, x -> x));
        }
        else {
            // 进行shuffle
            ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, dataSet.numPartitions());
            ShuffledOperator<K, V> shuffleReducer = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
            return new KvOperator<>(new FullAggOperator<>(shuffleReducer, x -> x));
        }
    }

    @Override
    public KvDataSet<K, V> partitionBy(Partitioner partitioner)
    {
        ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(dataSet, partitioner);
        ShuffledOperator<K, V> shuffledOperator = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
        return new KvOperator<>(shuffledOperator);
    }

    @Override
    public KvDataSet<K, V> partitionBy(int numPartitions)
    {
        return partitionBy(new HashPartitioner(numPartitions));
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer)
    {
        return reduceByKey(reducer, dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, int numPartition)
    {
        return reduceByKey(reducer, new HashPartitioner(dataSet.numPartitions()));
    }

    @Override
    public KvDataSet<K, V> reduceByKey(Reducer<V> reducer, Partitioner partitioner)
    {
        Reducer<V> clearedFunc = Utils.clear(reducer);
        if (partitioner.equals(dataSet.getPartitioner())) {
            // 因为上一个stage已经按照相同的分区器, 将数据分好，因此这里我们无需shuffle
            return new KvOperator<>(new AggOperator<>(dataSet, clearedFunc));
        }
        else {
            Operator<Tuple2<K, V>> combineOperator;
            // combine
            if (combine) {
                combineOperator = new AggOperator<>(dataSet, clearedFunc);
            }
            else {
                combineOperator = dataSet;
            }

            // 进行shuffle
            ShuffleMapOperator<K, V> shuffleMapper = new ShuffleMapOperator<>(combineOperator, partitioner);
            ShuffledOperator<K, V> shuffledOperator = new ShuffledOperator<>(shuffleMapper, shuffleMapper.getPartitioner());
            return new KvOperator<>(new AggOperator<>(shuffledOperator, clearedFunc));
        }
    }

    @Override
    public KvDataSet<K, Double> avgValues(Mapper<V, Double> valueCast)
    {
        return avgValues(valueCast, dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, Double> avgValues(Mapper<V, Double> valueCast, int numPartition)
    {
        return avgValues(valueCast, new HashPartitioner(numPartition));
    }

    @Override
    public KvDataSet<K, Double> avgValues(Mapper<V, Double> valueCast, Partitioner partitioner)
    {
        Mapper<V, Double> clearedFunc = Utils.clear(valueCast);
        return this.mapValues(x -> new Tuple2<>(clearedFunc.map(x), 1L))
                .reduceByKey((x, y) -> new Tuple2<>(x.f1() + y.f1(), x.f2() + y.f2()), partitioner)
                .mapValues(x -> x.f1() / x.f2());
    }

    @Override
    public KvDataSet<K, Long> countByKey()
    {
        return countByKey(dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, Long> countByKey(int numPartition)
    {
        return countByKey(new HashPartitioner(numPartition));
    }

    @Override
    public KvDataSet<K, Long> countByKey(Partitioner partitioner)
    {
        return this.mapValues(x -> 1L).reduceByKey(Long::sum, partitioner);
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> leftJoin(DataSet<Tuple2<K, W>> kvDataSet)
    {
        return join(kvDataSet, JoinExperiment.JoinMode.LEFT_JOIN);
    }

    @Override
    public <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> kvDataSet)
    {
        return join(kvDataSet, JoinExperiment.JoinMode.INNER_JOIN);
    }

    @Deprecated
    private <W> KvDataSet<K, Tuple2<V, W>> join(DataSet<Tuple2<K, W>> rightDataSet, JoinExperiment.JoinMode joinMode)
    {
        checkState(rightDataSet instanceof Operator, rightDataSet + "not instanceof Operator");
        Operator<Tuple2<K, W>> rightOperator = unboxing((Operator<Tuple2<K, W>>) rightDataSet);

        Operator<Tuple2<K, Iterable<?>[]>> joinOperator;
        Partitioner leftPartitioner = dataSet.getPartitioner();
        Partitioner rightPartitioner = rightDataSet.getPartitioner();
        if (leftPartitioner != null && leftPartitioner.equals(rightPartitioner)) {
            // 因为上一个stage已经按照相同的分区器, 将数据分好，因此这里我们无需shuffle
            joinOperator = new LocalJoinOperator<>(dataSet, rightOperator);
        }
        else if ((Object) rightOperator == dataSet) {
            return this.mapValues(x -> new Tuple2<>(x, (W) x));
        }
        else {
            int reduceNum = Math.max(dataSet.numPartitions(), rightDataSet.numPartitions());
            Partitioner partitioner = new HashPartitioner(reduceNum);
            joinOperator = new ShuffleJoinOperator<>(partitioner, dataSet, rightOperator);
        }

        Operator<Tuple2<K, Tuple2<V, W>>> operator = joinOperator.flatMapIterator(x -> {
            @SuppressWarnings("unchecked")
            Iterable<V> v = (Iterable<V>) x.f2()[0];
            @SuppressWarnings("unchecked")
            Iterable<W> w = (Iterable<W>) x.f2()[1];

            Iterator<Tuple2<V, W>> iterator = JoinExperiment.cartesian(v, w, joinMode);
            return Iterators.map(iterator, it -> new Tuple2<>(x.f1(), it));
        });
        return new KvOperator<>(operator);
    }

    @Override
    public KvDataSet<K, V> union(DataSet<Tuple2<K, V>> kvDataSet)
    {
        return unionAll(kvDataSet).distinct();
    }

    @Override
    public KvDataSet<K, V> union(KvDataSet<K, V> kvDataSet, int numPartition)
    {
        return union(kvDataSet, new HashPartitioner(numPartition));
    }

    @Override
    public KvDataSet<K, V> union(KvDataSet<K, V> kvDataSet, Partitioner partitioner)
    {
        return unionAll(kvDataSet).distinct(partitioner);
    }

    @Override
    public KvDataSet<K, V> unionAll(DataSet<Tuple2<K, V>> kvDataSet)
    {
        Operator<Tuple2<K, V>> dataSet = (Operator<Tuple2<K, V>>) super.unionAll(kvDataSet);
        return new KvOperator<>(dataSet);
    }

    @Override
    public KvDataSet<K, V> sortByKey(Comparator<K> comparator)
    {
        return sortByKey(comparator, dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, V> sortByKey(Comparator<K> comparator, int numPartitions)
    {
        Comparator<K> clearedFunc = Utils.clear(comparator);
        Partitioner partitioner = SortShuffleWriter.createPartitioner(numPartitions, (Operator<K>) this.keys(), clearedFunc);
        ShuffleMapOperator<K, V> sortShuffleMapOp = new ShuffleMapOperator<>(
                dataSet,
                partitioner,
                clearedFunc);

        SortShuffleWriter.ShuffledMergeSortOperator<K, V> shuffledOperator = new SortShuffleWriter
                .ShuffledMergeSortOperator<>(
                sortShuffleMapOp,
                clearedFunc,
                sortShuffleMapOp.getPartitioner());
        return new KvOperator<>(shuffledOperator);
    }

    @Override
    public KvDataSet<K, V> sortByValue(Comparator<V> comparator)
    {
        return sortByValue(comparator, dataSet.numPartitions());
    }

    @Override
    public KvDataSet<K, V> sortByValue(Comparator<V> comparator, int numPartitions)
    {
        return this.kvDataSet(x -> new Tuple2<>(x.f2(), x.f1()))
                .sortByKey(comparator, numPartitions)
                .kvDataSet(x -> new Tuple2<>(x.f2(), x.f1()));
    }
}
