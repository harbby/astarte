package com.github.harbby.ashtarte;

public class HashPartitioner<K>
        extends Partitioner<K>
{
    private final int numPartitions;

    public HashPartitioner(int numPartitions)
    {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions()
    {
        return numPartitions;
    }

    @Override
    public int getPartition(K key)
    {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
