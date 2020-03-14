package com.github.harbby.ashtarte.api;

import java.io.Serializable;

public class Partition
        implements Serializable
{
    private final int index;

    public Partition(int index)
    {
        this.index = index;
    }

    public int getId()
    {
        return index;
    }

    @Override
    public int hashCode()
    {
        return index;
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }
}
