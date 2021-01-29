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

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.ShuffleWriter;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.function.exception.Consumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class SortShuffleWriter<K, V>
        extends ShuffleWriter.HashShuffleWriter<K, V>
{
    private final Partitioner partitioner;
    private final Comparator<K> ordering;

    //spillFile
    public SortShuffleWriter(
            String executorUUID,
            int jobId,
            int shuffleId, int mapId,
            Comparator<K> ordering,
            Partitioner partitioner)
    {
        super(executorUUID, jobId, shuffleId, mapId, partitioner);
        this.ordering = ordering;
        this.partitioner = partitioner;
    }

    @Override
    public void write(Iterator<? extends Tuple2<K, V>> iterator)
            throws IOException
    {
        SorterBuffer<K, V> sorter = new SorterBuffer<>(ordering, partitioner);

        sorter.insertAll(iterator);

        sorter.saveTo(super::write);
    }

    public static <K> Partitioner createPartitioner(
            int reduceNumber,
            Operator<K> operator,
            Comparator<K> ordering)
    {
        int sampleSize = Math.min(200 * reduceNumber, 1 << 20); //max 1M rows
        int sampleSizePerPartition = (int) Math.ceil(1.0 * sampleSize / operator.numPartitions());
        K[] points = analyzerSplit(operator,
                ordering,
                sampleSizePerPartition);
        return new SortShuffleRangePartitioner<>(reduceNumber, points, ordering);
    }

    public static <K> K[] analyzerSplit(
            Operator<K> operator,
            Comparator<K> ordering,
            int sampleSizePerPartition)
    {
        List<SampleResult<K>> sampleResults = sketch(operator, sampleSizePerPartition);
        long length = sampleResults.stream().mapToLong(x -> x.getPartitionCount()).sum();
        int numPartitions = operator.numPartitions();

        List<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            int start = (int) ((i * length) / numPartitions);
            int end = (int) (((i + 1) * length) / numPartitions);
            tuple2s.add(Tuple2.of(start, end));
        }

        List<Tuple2<K, Double>> arrays = new ArrayList<>();
        for (SampleResult<K> sample : sampleResults) {
            double weight = (double) length / sample.getPartitionCount();
            for (K k : sample.getData()) {
                arrays.add(new Tuple2<>(k, weight));
            }
        }

        int splitNum = Math.min(numPartitions, arrays.size());
        K[] ks = getPoints(arrays, ordering, splitNum);
        return ks;
    }

    private static <K> K[] getPoints(
            List<Tuple2<K, Double>> candidates,
            Comparator<K> ordering,
            int partitions)
    {
        List<Tuple2<K, Double>> ordered = candidates.stream()
                .sorted((x, y) -> ordering.compare(x.f1(), y.f1()))
                .collect(Collectors.toList());
        int numCandidates = ordered.size();
        double sumWeights = ordered.stream().mapToDouble(x -> x.f2()).sum();
        double step = sumWeights / partitions;
        double cumWeight = 0.0;
        double target = step;
        List<K> bounds = new ArrayList<>();
        int i = 0;
        int j = 0;
        Optional<K> previousBound = Optional.empty();
        while ((i < numCandidates) && (j < partitions - 1)) {
            Tuple2<K, Double> tp = ordered.get(i);
            K key = tp.f1();
            double weight = tp.f2();

            cumWeight += weight;
            if (cumWeight >= target) {
                // Skip duplicate values.
                if (!previousBound.isPresent() || ordering.compare(key, previousBound.get()) > 0) {
                    bounds.add(key);
                    target += step;
                    j += 1;
                    previousBound = Optional.ofNullable(key);
                }
            }
            i += 1;
        }
        return (K[]) bounds.toArray();
    }

    private static <K> List<SampleResult<K>> sketch(Operator<K> operator,
            int sampleSizePerPartition)
    {
        //todo: 使用更小的数据结构
        List<SampleResult<K>> results = operator.mapPartitionWithId((index, it) -> {
            if (!it.hasNext()) {
                throw new UnsupportedOperationException();
            }
            K e = it.next();
            K[] array = (K[]) java.lang.reflect.Array.newInstance(e.getClass(), sampleSizePerPartition);
            array[0] = e;
            int i = 1;
            for (; i < sampleSizePerPartition && it.hasNext(); i++) {
                array[i] = it.next();
            }
            if (i < sampleSizePerPartition) {
                return Iterators.of(new SampleResult<>(i, index, Arrays.copyOf(array, i)));
            }
            else {
                Random random = new Random();
                long l = i;
                for (; it.hasNext(); l++) {
                    int p = random.nextInt(sampleSizePerPartition);
                    array[p] = it.next();
                }
                return Iterators.of(new SampleResult<>(l, index, array));
            }
        }).collect();

        return results;
    }

    static class SampleResult<E>
            implements Serializable
    {
        private final long partitionCount;
        private final int partitionId;
        private final E[] data;

        public SampleResult(long partitionCount, int partitionId, E[] data)
        {
            this.partitionCount = partitionCount;
            this.partitionId = partitionId;
            this.data = data;
        }

        public long getPartitionCount()
        {
            return partitionCount;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public E[] getData()
        {
            return data;
        }
    }

    public static class SorterBuffer<K, V>
    {
        private final List<Tuple2<K, V>>[] writerBuffer;
        private final Comparator<K> ordering;
        private final Partitioner partitioner;

        public SorterBuffer(Comparator<K> ordering, Partitioner partitioner)
        {
            this.ordering = ordering;
            this.partitioner = partitioner;
            writerBuffer = (List<Tuple2<K, V>>[]) new List<?>[partitioner.numPartitions()];
        }

        public void insertAll(Iterator<? extends Tuple2<K, V>> iterator)
        {
            while (iterator.hasNext()) {
                Tuple2<K, V> kv = iterator.next();
                int reduceId = this.partitioner.getPartition(kv.f1());
                List<Tuple2<K, V>> buffer = writerBuffer[reduceId];
                if (buffer == null) {
                    buffer = new LinkedList<>();
                    writerBuffer[reduceId] = buffer;
                }
                buffer.add(kv);
            }
        }

        public void saveTo(Consumer<Iterator<? extends Tuple2<K, V>>, IOException> shuffleWriter)
                throws IOException
        {
            for (List<Tuple2<K, V>> buffer : writerBuffer) {
                if (buffer != null) {
                    buffer.sort((x, y) -> ordering.compare(x.f1(), y.f1()));
                    shuffleWriter.apply(buffer.iterator());
                }
            }
        }
    }

    public static class SortShuffleRangePartitioner<K>
            extends Partitioner
    {
        private final int reduceNumber;
        private final K[] points;
        private final Comparator<K> ordering;

        public SortShuffleRangePartitioner(
                int reduceNumber,
                K[] points,
                Comparator<K> ordering
        )
        {
            this.reduceNumber = reduceNumber;
            this.points = points;
            this.ordering = requireNonNull(ordering, "ordering is null");
        }

        @Override
        public int numPartitions()
        {
            return reduceNumber;
        }

        @Override
        public int getPartition(Object key)
        {
            //todo: 二分法
            for (int i = 0; i < points.length; i++) {
                if (ordering.compare((K) key, points[i]) < 0) {
                    return i;
                }
            }
            return points.length;
        }
    }

    public static class ShuffledMergeSortOperator<K, V>
            extends Operator<Tuple2<K, V>>
    {
        private final Partitioner partitioner;
        private final int shuffleMapOperatorId;
        private final Comparator<K> ordering;
        private final transient Operator<?> dependOperator;

        public ShuffledMergeSortOperator(ShuffleMapOperator<K, V> operator,
                Comparator<K> ordering,
                Partitioner partitioner)
        {
            super(operator);
            this.shuffleMapOperatorId = operator.getId();
            this.partitioner = partitioner;
            this.ordering = ordering;
            this.dependOperator = operator;
        }

        @Override
        public List<? extends Operator<?>> getDependencies()
        {
            return ImmutableList.of(dependOperator);
        }

        @Override
        protected Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
        {
            List<Tuple2<K, V>> buffer = new LinkedList<>();
            Integer shuffleId = taskContext.getDependStages().get(shuffleMapOperatorId);
            checkState(shuffleId != null);
            Iterator<Tuple2<K, V>> reader = taskContext.getShuffleClient().readShuffleData(shuffleId, split.getId());
            while (reader.hasNext()) {
                buffer.add(reader.next());
            }

            buffer.sort((x, y) -> ordering.compare(x.f1(), y.f1()));
            return buffer.iterator();
        }
    }
}
