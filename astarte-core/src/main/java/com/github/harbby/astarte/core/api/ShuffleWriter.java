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
package com.github.harbby.astarte.core.api;

import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.operator.SortShuffleWriter;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.io.BufferedNioOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public interface ShuffleWriter<K, V>
        extends Closeable
{
    public static String MERGE_FILE_NAME = "shuffle_merged_%s_%s.data";

    public void write(Iterator<? extends Tuple2<K, V>> iterator)
            throws IOException;

    public static <K, V> ShuffleWriter<K, V> createShuffleWriter(
            File shuffleBaseDir,
            int jobId,
            int shuffleId,
            int mapId,
            Partitioner partitioner,
            Encoder<Tuple2<K, V>> encoder)
    {
        String filePrefix = String.format("shuffle_%s_%s_", shuffleId, mapId);
        File shuffleWorkDir = new File(shuffleBaseDir, String.valueOf(jobId));
        return new SortShuffleWriter<>(shuffleWorkDir, filePrefix, String.format(MERGE_FILE_NAME, shuffleId, mapId), partitioner, encoder);
        //return new HashShuffleWriter<>(shuffleWorkDir, filePrefix, partitioner, encoder);
    }

    public static class HashShuffleWriter<K, V>
            implements ShuffleWriter<K, V>
    {
        private static final Logger logger = LoggerFactory.getLogger(HashShuffleWriter.class);

        private final File shuffleWorkDir;
        private final String prefix;
        private final Partitioner partitioner;
        //todo: use array index, not hash
        private final DataOutputStream[] outputStreams;
        private final Encoder<Tuple2<K, V>> encoder;

        public HashShuffleWriter(
                File shuffleWorkDir,
                String filePrefix,
                Partitioner partitioner,
                Encoder<Tuple2<K, V>> encoder)
        {
            this.shuffleWorkDir = shuffleWorkDir;
            this.prefix = filePrefix;
            this.partitioner = partitioner;
            this.outputStreams = new DataOutputStream[partitioner.numPartitions()];
            this.encoder = requireNonNull(encoder, "encoder is null");
        }

        @Override
        public void write(Iterator<? extends Tuple2<K, V>> iterator)
                throws IOException
        {
            while (iterator.hasNext()) {
                Tuple2<K, V> kv = iterator.next();
                int reduceId = partitioner.getPartition(kv.f1());
                DataOutputStream dataOutputStream = getOutputChannel(reduceId);
                encoder.encoder(kv, dataOutputStream);
            }

            for (DataOutputStream dataOutputStream : outputStreams) {
                try {
                    if (dataOutputStream != null) {
                        dataOutputStream.close();
                    }
                }
                catch (Exception e) {
                    logger.error("close shuffle write file outputStream failed", e);
                }
            }
        }

        private DataOutputStream getOutputChannel(int reduceId)
                throws FileNotFoundException
        {
            DataOutputStream dataOutputStream = outputStreams[reduceId];
            if (dataOutputStream == null) {
                File file = new File(shuffleWorkDir, prefix + reduceId + ".data");
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                FileOutputStream fileOutputStream = new FileOutputStream(file, false);
                dataOutputStream = new DataOutputStream(new BufferedNioOutputStream(fileOutputStream.getChannel(), 10240));
                outputStreams[reduceId] = dataOutputStream;
            }
            return dataOutputStream;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }
}
