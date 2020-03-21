package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public interface ShuffleWriter<KEY, VALUE>
        extends Closeable
{
    public void write(Iterator<? extends Tuple2<KEY, VALUE>> records)
            throws IOException;

    public File getDataFile(int shuffleId, int mapId, int reduceId);

    public static <KEY, VALUE> ShuffleWriter<KEY, VALUE> createShuffleWriter(
            int shuffleId,
            int mapId,
            Partitioner partitioner)
    {
        return new HashShuffle<>(shuffleId, mapId, partitioner);
    }

    public static class HashShuffle<KEY, VALUE>
            implements ShuffleWriter<KEY, VALUE>
    {
        private final Partitioner partitioner;
        private final int shuffleId;
        private final int mapId;

        public HashShuffle(int shuffleId, int mapId, Partitioner partitioner)
        {
            this.partitioner = requireNonNull(partitioner, "partitioner is null");
            this.shuffleId = shuffleId;
            this.mapId = mapId;
        }

        @Override
        public void write(Iterator<? extends Tuple2<KEY, VALUE>> records)
                throws IOException
        {
            Map<Integer, DataOutputStream> outputStreamMap = new HashMap<>();
            try {
                while (records.hasNext()) {
                    Tuple2<KEY, VALUE> record = records.next();
                    int reduceId = partitioner.getPartition(record.f1());

                    DataOutputStream dataOutputStream = outputStreamMap.get(reduceId);
                    if (dataOutputStream == null) {
                        File file = this.getDataFile(shuffleId, mapId, reduceId);
                        if (!file.getParentFile().exists()) {
                            file.getParentFile().mkdirs();
                        }
                        dataOutputStream = new DataOutputStream(new FileOutputStream(this.getDataFile(shuffleId, mapId, reduceId), false));
                        outputStreamMap.put(reduceId, dataOutputStream);
                    }

                    byte[] bytes = Serializables.serialize(record);
                    dataOutputStream.writeInt(bytes.length);
                    dataOutputStream.write(bytes);
                }
            }
            finally {
                for (DataOutputStream dataOutputStream : outputStreamMap.values()) {
                    dataOutputStream.writeInt(-1);
                    dataOutputStream.close();
                }
            }
        }

        @Override
        public File getDataFile(int shuffleId, int mapId, int reduceId)
        {
            // spark path /tmp/blockmgr-0b4744ba-bffa-420d-accb-fbc475da7a9d/27/shuffle_101_201_0.data
            return new File("/tmp/shuffle/shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data");
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }
}
