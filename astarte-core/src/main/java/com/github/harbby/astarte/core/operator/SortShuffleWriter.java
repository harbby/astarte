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
import com.github.harbby.astarte.core.api.ShuffleWriter;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.api.function.Reducer;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderInputStream;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.io.BufferedNioOutputStream;
import com.github.harbby.gadtry.io.LimitInputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class SortShuffleWriter<K, V>
        implements ShuffleWriter<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(SortShuffleWriter.class);
    public static final String MERGE_FILE_NAME = "shuffle_merged_%s_%s.data";

    private final Partitioner partitioner;
    private final Comparator<K> comparator;
    private final Encoder<Tuple2<K, V>> encoder;
    private final File shuffleWorkDir;
    private final String prefix;
    private final File mergeName;
    private final Reducer<V> combine;

    //spillFile
    public SortShuffleWriter(
            File shuffleWorkDir,
            String filePrefix,
            String mergeName,
            Partitioner partitioner,
            Encoder<Tuple2<K, V>> encoder,
            Comparator<K> comparator,
            Reducer<V> combine)
    {
        this.partitioner = partitioner;
        this.encoder = encoder;
        this.shuffleWorkDir = shuffleWorkDir;
        this.prefix = filePrefix;
        this.mergeName = new File(shuffleWorkDir, mergeName);
        this.comparator = comparator;
        this.combine = combine;

        if (!shuffleWorkDir.exists()) {
            checkState(shuffleWorkDir.mkdirs() || shuffleWorkDir.exists(), "create shuffle dir failed %s", shuffleWorkDir);
        }
    }

    @Override
    public ByteBuffer write(Iterator<? extends Tuple2<K, V>> iterator)
            throws IOException
    {
        SorterBuffer sorter = new SorterBuffer(comparator, partitioner, encoder);

        sorter.insertAll(iterator);

        return sorter.mergeFile();
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

    @Override
    public void close()
            throws IOException
    {
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

    public static int getSortMergedFileHarderSize(int segmentSize)
    {
        return Integer.BYTES + segmentSize * Long.BYTES * 2;
    }

    private static class ReduceWriter<K, V>
    {
        private static final int BUFF_SIZE = 8192;
        private final Encoder<Tuple2<K, V>> encoder;
        private final File spillsFile;
        private BufferedNioOutputStream bufferedNioOutput;
        private final List<Long> segmentEnds = new ArrayList<>();
        private final List<Integer> segmentRowSizes = new ArrayList<>();
        private final ArrayList<Tuple2<K, V>> buffer = new ArrayList<>(BUFF_SIZE);
        private final Comparator<K> comparator;
        private long mapTaskReadRowCount = 0;

        public ReduceWriter(
                File spillsFile,
                Encoder<Tuple2<K, V>> encoder,
                Comparator<K> comparator)
        {
            this.encoder = encoder;
            this.comparator = comparator;
            this.spillsFile = spillsFile;
        }

        private void flushSegment()
                throws IOException
        {
            if (bufferedNioOutput == null) {
                this.bufferedNioOutput = new BufferedNioOutputStream(new FileOutputStream(spillsFile, false).getChannel());
            }
            buffer.sort((x, y) -> comparator.compare(x.f1(), y.f1()));
            LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(bufferedNioOutput);
            DataOutputStream dataOutput = new DataOutputStream(lz4BlockOutputStream);
            for (Tuple2<K, V> kv : buffer) {
                encoder.encoder(kv, dataOutput);
            }
            lz4BlockOutputStream.finish();
            segmentEnds.add(bufferedNioOutput.position());
            segmentRowSizes.add(buffer.size());
            buffer.clear();
        }

        public void insert(Tuple2<K, V> kv)
                throws IOException
        {
            mapTaskReadRowCount++;
            if (buffer.size() >= BUFF_SIZE) {
                this.flushSegment();
            }
            buffer.add(kv);
        }

        public Iterator<Tuple2<K, V>> merger()
                throws IOException
        {
            if (segmentEnds.isEmpty()) {
                buffer.sort((x, y) -> comparator.compare(x.f1(), y.f1()));
                return Iterators.wrap(buffer).autoClose(buffer::clear);
            }
            //flush last segment
            this.flushSegment();
            this.buffer.trimToSize();
            //close spill file io
            this.writeFinish();
            EncoderInputStream<Tuple2<K, V>>[] encoderInputStreams = new EncoderInputStream[segmentEnds.size()];
            long start = 0;
            for (int i = 0; i < segmentEnds.size(); i++) {
                long end = segmentEnds.get(i);
                long length = end - start;
                FileInputStream fileInputStream = new FileInputStream(spillsFile);
                fileInputStream.getChannel().position(start);
                int segmentRowSize = segmentRowSizes.get(i);
                EncoderInputStream<Tuple2<K, V>> encoderInputStream = new EncoderInputStream<>(segmentRowSize, encoder, new LZ4BlockInputStream(new BufferedInputStream(new LimitInputStream(fileInputStream, length))));
                encoderInputStreams[i] = encoderInputStream;
                start = end;
            }
            //merger
            return Iterators.mergeSorted((x, y) -> comparator.compare(x.f1, y.f1), encoderInputStreams);
        }

        public long getMapTaskReadRowCount()
        {
            return mapTaskReadRowCount;
        }

        private void writeFinish()
                throws IOException
        {
            if (bufferedNioOutput != null) {
                bufferedNioOutput.close();
            }
        }
    }

    public class SorterBuffer
    {
        private final Comparator<K> ordering;
        private final Partitioner partitioner;
        private final Encoder<Tuple2<K, V>> encoder;
        private final ReduceWriter<K, V>[] reduceWriters;

        @SuppressWarnings("unchecked")
        public SorterBuffer(Comparator<K> ordering, Partitioner partitioner, Encoder<Tuple2<K, V>> encoder)
        {
            this.ordering = ordering;
            this.partitioner = partitioner;
            this.encoder = encoder;
            this.reduceWriters = new ReduceWriter[partitioner.numPartitions()];
        }

        private ReduceWriter<K, V> getReduceWriter(int reduceId)
        {
            ReduceWriter<K, V> reduceWriter = reduceWriters[reduceId];
            if (reduceWriter != null) {
                return reduceWriter;
            }
            File spillsFile = new File(shuffleWorkDir, prefix + reduceId + ".data");
            reduceWriter = new ReduceWriter<>(spillsFile, encoder, ordering);
            reduceWriters[reduceId] = reduceWriter;
            return reduceWriter;
        }

        public void insertAll(Iterator<? extends Tuple2<K, V>> iterator)
                throws IOException
        {
            while (iterator.hasNext()) {
                Tuple2<K, V> kv = iterator.next();
                int reduceId = this.partitioner.getPartition(kv.f1());
                ReduceWriter<K, V> reduceWriter = getReduceWriter(reduceId);
                reduceWriter.insert(kv);
            }
        }

        public ByteBuffer mergeFile()
                throws IOException
        {
            ByteBuffer header = ByteBuffer.allocate(getSortMergedFileHarderSize(reduceWriters.length));
            header.putInt(reduceWriters.length);
            try (FileChannel fileChannel = new FileOutputStream(mergeName, false).getChannel()) {
                //skip header = int + len * long
                fileChannel.position(header.capacity());
                BufferedNioOutputStream bufferedNioOutputStream = new BufferedNioOutputStream(fileChannel);
                for (ReduceWriter<K, V> reduceWriter : reduceWriters) {
                    if (reduceWriter == null) {
                        header.putLong(bufferedNioOutputStream.position());
                        header.putLong(0);
                        continue;
                    }
                    LZ4BlockOutputStream lz4OutputStream = new LZ4BlockOutputStream(bufferedNioOutputStream);
                    //merger
                    Iterator<Tuple2<K, V>> merger = reduceWriter.merger();
                    long rowCount = reduceWriter.getMapTaskReadRowCount();
                    if (combine != null) {
                        long count = 0;
                        merger = Iterators.reduceSorted(merger, combine);
                        while (merger.hasNext()) {
                            count++;
                            encoder.encoder(merger.next(), new DataOutputStream(lz4OutputStream));
                        }
                        logger.info("shuffleMapTask merged combine {}/{} ratio: {}", count, rowCount, count * 1.0f / rowCount);
                        rowCount = count;
                    }
                    else {
                        while (merger.hasNext()) {
                            encoder.encoder(merger.next(), new DataOutputStream(lz4OutputStream));
                        }
                    }
                    lz4OutputStream.finish();
                    //merge index
                    header.putLong(bufferedNioOutputStream.position());
                    header.putLong(rowCount);
                    if (reduceWriter.spillsFile.exists()) {
                        checkState(reduceWriter.spillsFile.delete(), "clear shuffle tmp file failed " + reduceWriter.spillsFile.getCanonicalPath());
                    }
                }
                //write header
                fileChannel.position(0);
                header.flip();
                fileChannel.write(header);
            }
            checkState(header.position() == header.capacity());
            header.position(0);
            return header;
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
}
