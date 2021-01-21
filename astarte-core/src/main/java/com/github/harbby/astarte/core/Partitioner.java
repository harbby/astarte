package com.github.harbby.astarte.core;

import java.io.Serializable;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

/**
 * numReduceTasks = numPartitions
 */
public abstract class Partitioner
        implements Serializable
{
    public abstract int numPartitions();

    public abstract int getPartition(Object key);

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numPartitions", numPartitions())
                .toString();
    }
}
