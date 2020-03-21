package com.github.harbby.ashtarte.api;

import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Serializable;
import java.util.Objects;

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
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        Partition other = (Partition) obj;
        return Objects.equals(this.index, other.index);
    }
}
