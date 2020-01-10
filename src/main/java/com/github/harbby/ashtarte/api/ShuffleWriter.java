package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.operator.ShufflePartition;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

public interface ShuffleWriter<KEY, VALUE>
        extends AutoCloseable
{
    public void write(Iterator<Tuple2<KEY, VALUE>> records);

    public static class ShuffleWriterImpl<KEY, VALUE>
            implements ShuffleWriter<KEY, VALUE>
    {
        ShufflePartition<KEY, VALUE>[] shufflePartitions;
        private final int reduceSize = 10;

        public ShuffleWriterImpl(int shuffleId, int mapId, ShufflePartition<KEY, VALUE>[] shufflePartitions)
        {
            this.shufflePartitions = shufflePartitions;
        }

        @Override
        public void write(Iterator<Tuple2<KEY, VALUE>> records)
        {
            final Partitioner<KEY> hashPartitioner = new HashPartitioner<>();
            while (records.hasNext()) {
                Tuple2<KEY, VALUE> record = records.next();
                int reduceId = hashPartitioner.getPartition(record.f1(), reduceSize);
                shufflePartitions[reduceId].send(record.f1(), record.f2());
            }
        }

        @Override
        public void close()
                throws Exception
        {

        }
    }
}
