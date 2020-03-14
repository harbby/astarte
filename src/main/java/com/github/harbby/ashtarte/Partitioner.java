package com.github.harbby.ashtarte;

import java.io.Serializable;

/**
 * numReduceTasks = numPartitions
 */
public abstract class Partitioner<KEY>
        implements Serializable
{
    public abstract int numPartitions();

    public abstract int getPartition(KEY key);
}
