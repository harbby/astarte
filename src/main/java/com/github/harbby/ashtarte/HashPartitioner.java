package com.github.harbby.ashtarte;

public class HashPartitioner<K>
        extends Partitioner<K>
{
    @Override
    public int getPartition(K key, int numPartitions)
    {
        return (key.hashCode() & 2147483647) % numPartitions;
    }
}
