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
import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static com.github.harbby.astarte.core.runtime.ShuffleManagerService.getShuffleWorkDir;
import static java.util.Objects.requireNonNull;

public interface ShuffleWriter<K, V>
        extends Closeable
{
    public void write(Iterator<? extends Tuple2<K, V>> iterator)
            throws IOException;

    public static <K, V> ShuffleWriter<K, V> createShuffleWriter(
            String executorUUID,
            int jobId,
            int shuffleId,
            int mapId,
            Partitioner partitioner,
            Encoder<Tuple2<K, V>> encoder,
            Comparator<K> ordering)
    {
        if (ordering != null) {
            return new SortShuffleWriter<>(executorUUID, jobId, shuffleId, mapId, ordering, partitioner, encoder);
        }
        return new HashShuffleWriter<>(executorUUID, jobId, shuffleId, mapId, partitioner, encoder);
    }

    public static class HashShuffleWriter<K, V>
            implements ShuffleWriter<K, V>
    {
        private static final Logger logger = LoggerFactory.getLogger(HashShuffleWriter.class);

        private final String executorUUID;
        private final int shuffleId;
        private final int mapId;
        private final int jobId;
        private final Partitioner partitioner;
        //todo: use array index, not hash
        private final NioDataOutputStream[] outputStreams;
        private final Encoder<Tuple2<K, V>> encoder;

        public HashShuffleWriter(
                String executorUUID,
                int jobId,
                int shuffleId,
                int mapId,
                Partitioner partitioner,
                Encoder<Tuple2<K, V>> encoder)
        {
            this.executorUUID = executorUUID;
            this.jobId = jobId;
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.partitioner = partitioner;
            this.outputStreams = new NioDataOutputStream[partitioner.numPartitions()];
            this.encoder = requireNonNull(encoder, "encoder is null");
        }

        @Override
        public void write(Iterator<? extends Tuple2<K, V>> iterator)
                throws IOException
        {
            while (iterator.hasNext()) {
                Tuple2<K, V> kv = iterator.next();
                int reduceId = partitioner.getPartition(kv.f1());
                NioDataOutputStream dataOutputStream = getOutputChannel(reduceId);
                encoder.encoder(kv, dataOutputStream);
                if (dataOutputStream.getPosition() > 81920) {
                    dataOutputStream.flush();
                }
            }
        }

        private NioDataOutputStream getOutputChannel(int reduceId)
                throws FileNotFoundException
        {
            NioDataOutputStream dataOutputStream = outputStreams[reduceId];
            if (dataOutputStream == null) {
                File file = this.getDataFile(shuffleId, mapId, reduceId);
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                FileOutputStream fileOutputStream = new FileOutputStream(this.getDataFile(shuffleId, mapId, reduceId), false);
                dataOutputStream = new NioDataOutputStream(new NioOutputStream(fileOutputStream.getChannel()));
                //bio: dataOutputStream = new DataOutputStream(new LZ4BlockOutputStream(fileOutputStream));
                outputStreams[reduceId] = dataOutputStream;
            }
            return dataOutputStream;
        }

        private static final class NioDataOutputStream
                extends DataOutputStream
        {
            private final NioOutputStream outBuffer;

            public NioDataOutputStream(NioOutputStream out)
            {
                super(new LZ4BlockOutputStream(out));
                this.outBuffer = out;
            }

            public int getPosition()
            {
                return outBuffer.position();
            }
        }

        public static final class NioOutputStream
                extends OutputStream
        {
            private final ByteBuffer buffer;
            private final FileChannel channel;

            public NioOutputStream(FileChannel channel, int buffSize)
            {
                this.buffer = ByteBuffer.allocateDirect(buffSize);
                this.channel = channel;
            }

            private NioOutputStream(FileChannel channel)
            {
                this(channel, 1024 * 1024);
            }

            @Override
            public void write(int b)
            {
                buffer.put((byte) b);
            }

            @Override
            public void write(byte[] b)
            {
                buffer.put(b);
            }

            @Override
            public void write(byte[] b, int off, int len)
            {
                buffer.put(b, off, len);
            }

            @Override
            public void flush()
                    throws IOException
            {
                buffer.flip();
                channel.write(buffer);
                buffer.clear();
            }

            public int position()
            {
                return buffer.position();
            }

            @Override
            public void close()
                    throws IOException
            {
                try (FileChannel ignored = this.channel) {
                    this.flush();
                }
                finally {
                    if (buffer.isDirect() && ((DirectBuffer) buffer).cleaner() != null) {
                        //java8字节码版本为52
                        if (Platform.getVmClassVersion() > 52) {
                            Platform.addOpenJavaModules(((DirectBuffer) buffer).cleaner().getClass(), this.getClass());
                        }
                        ((DirectBuffer) buffer).cleaner().clean();
                    }
                }
            }
        }

        private File getDataFile(int shuffleId, int mapId, int reduceId)
        {
            // spark path /tmp/blockmgr-0b4744ba-bffa-420d-accb-fbc475da7a9d/27/shuffle_101_201_0.data
            String fileName = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data";
            File shuffleWorkDir = getShuffleWorkDir(executorUUID);
            return new File(shuffleWorkDir, String.format("%s/%s", jobId, fileName));
        }

        @Override
        public void close()
                throws IOException
        {
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
    }
}
