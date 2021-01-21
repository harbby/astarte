package com.github.harbby.astarte.core.runtime;

import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Set;

public interface ShuffleClient
        extends Closeable
{
    public <K, V> Iterator<Tuple2<K, V>> readShuffleData(int shuffleId, int reduceId);

    public static ShuffleClient getLocalShuffleClient(ShuffleManagerService shuffleManagerService)
    {
        return shuffleManagerService::getShuffleDataIterator;
    }

    public static ShuffleClient getClusterShuffleClient(Set<SocketAddress> shuffleServices)
            throws InterruptedException
    {
        return ClusterShuffleClient.start(shuffleServices);
    }

    @Override
    default void close()
            throws IOException
    {}
}
