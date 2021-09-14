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
package com.github.harbby.astarte.core.runtime;

import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderInputStream;
import com.github.harbby.gadtry.base.Files;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.io.LimitInputStream;
import net.jpountz.lz4.LZ4BlockInputStream;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.astarte.core.operator.SortShuffleWriter.getSortMergedFileHarderSize;
import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static java.util.Objects.requireNonNull;

public interface ShuffleClient
        extends Closeable
{
    public <K, V> Iterator<Tuple2<K, V>> createShuffleReader(Comparator<K> comparator, Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId)
            throws IOException;

    @Override
    default void close()
            throws IOException
    {}

    public static ShuffleClient getClusterShuffleClient(Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks)
    {
        return new SortShuffleClusterClient(dependMapTasks);
    }

    public static class LocalShuffleClient
            implements ShuffleClient
    {
        private final File shuffleBaseDir;
        private final int currentJobId;

        public LocalShuffleClient(File shuffleBaseDir, int currentJobId)
        {
            this.shuffleBaseDir = shuffleBaseDir;
            this.currentJobId = currentJobId;
        }

        @Override
        public <K, V> Iterator<Tuple2<K, V>> createShuffleReader(Comparator<K> comparator, Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId)
                throws IOException
        {
            requireNonNull(encoder, "encoder is null");
            requireNonNull(comparator, "comparator is null");
            checkArgument(reduceId >= 0);
            String prefix = "shuffle_merged_" + shuffleId + "_";
            List<File> files = Files.listFiles(new File(shuffleBaseDir, String.valueOf(currentJobId)), false, file -> file.getName().startsWith(prefix));
            List<Iterator<Tuple2<K, V>>> iterators = new ArrayList<>(files.size());
            for (File file : files) {
                //read header
                FileInputStream fileInputStream = new FileInputStream(file);
                DataInputStream dataInputStream = new DataInputStream(fileInputStream);
                long[] segmentEnds = new long[dataInputStream.readInt()];
                long[] segmentRowSizes = new long[segmentEnds.length];
                for (int i = 0; i < segmentEnds.length; i++) {
                    segmentEnds[i] = dataInputStream.readLong();
                    segmentRowSizes[i] = dataInputStream.readLong();
                }
                long segmentEnd = segmentEnds[reduceId];
                long length = segmentEnd;
                if (reduceId > 0) {
                    int headerSize = getSortMergedFileHarderSize(segmentEnds.length);
                    fileInputStream.getChannel().position(headerSize + segmentEnds[reduceId - 1]);
                    length = segmentEnd - segmentEnds[reduceId - 1];
                }
                if (length > 0) {
                    iterators.add(new EncoderInputStream<>(segmentRowSizes[reduceId], encoder,
                            new LZ4BlockInputStream(new BufferedInputStream(new LimitInputStream(fileInputStream, length))))
                            .autoClose(() -> {
                                try {
                                    fileInputStream.close();
                                }
                                catch (IOException e) {
                                    Throwables.throwThrowable(e);
                                }
                            })
                    );
                }
                else {
                    fileInputStream.close();
                }
            }
            return Iterators.mergeSorted((x, y) -> comparator.compare(x.key(), y.key()), iterators);
        }
    }
}
