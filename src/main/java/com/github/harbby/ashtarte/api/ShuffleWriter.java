package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.api.function.Comparator;
import com.github.harbby.ashtarte.operator.SortShuffleWriter;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public interface ShuffleWriter<K, V>
        extends Closeable
{
    public File getDataFile(int shuffleId, int mapId, int reduceId);

    public void write(Iterator<? extends Tuple2<K, V>> iterator)
            throws IOException;

    public static <K, V> ShuffleWriter<K, V> createShuffleWriter(
            int shuffleId,
            int mapId,
            Partitioner partitioner,
            Comparator<K> ordering)
    {
        if (ordering != null) {
            return new SortShuffleWriter<>(shuffleId, mapId, ordering, partitioner);
        }
        return new HashShuffleWriter<>(shuffleId, mapId, partitioner);
    }

    public static class HashShuffleWriter<KEY, VALUE>
            implements ShuffleWriter<KEY, VALUE>
    {
        private final int shuffleId;
        private final int mapId;
        private final Partitioner partitioner;
        private final Map<Integer, DataOutputStream> outputStreamMap = new HashMap<>();

        public HashShuffleWriter(int shuffleId, int mapId, Partitioner partitioner)
        {
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.partitioner = partitioner;
        }

        protected void write(int reduceId, Serializable value)
                throws IOException
        {
            DataOutputStream dataOutputStream = outputStreamMap.get(reduceId);
            if (dataOutputStream == null) {
                File file = this.getDataFile(shuffleId, mapId, reduceId);
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                dataOutputStream = new DataOutputStream(new FileOutputStream(this.getDataFile(shuffleId, mapId, reduceId), false));
                outputStreamMap.put(reduceId, dataOutputStream);
            }

            byte[] bytes = Serializables.serialize(value);
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
        }

        @Override
        public void write(Iterator<? extends Tuple2<KEY, VALUE>> iterator)
                throws IOException
        {
            while (iterator.hasNext()) {
                Tuple2<KEY, VALUE> kv = iterator.next();
                int reduceId = partitioner.getPartition(kv.f1());
                this.write(reduceId, kv);
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
            for (DataOutputStream dataOutputStream : outputStreamMap.values()) {
                dataOutputStream.writeInt(-1);
                dataOutputStream.close();
            }
        }
    }
}
