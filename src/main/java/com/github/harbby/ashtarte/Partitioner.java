package com.github.harbby.ashtarte;

/**
 * numReduceTasks = numPartitions
 */
public abstract class Partitioner<KEY>
{
    public abstract int getPartition(KEY key, int numReduceTasks);
}
