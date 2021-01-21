package com.github.harbby.astarte.core;

import java.util.Objects;

public class HashPartitioner
        extends Partitioner
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
    public int getPartition(Object key)
    {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    @Override
    public int hashCode()
    {
        return numPartitions;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        HashPartitioner other = (HashPartitioner) obj;
        return Objects.equals(this.numPartitions, other.numPartitions);
    }
}
