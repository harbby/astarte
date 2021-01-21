package com.github.harbby.astarte.core.api;

import java.io.Serializable;
import java.util.Objects;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

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
    public String toString()
    {
        return toStringHelper(this)
                .add("id", index)
                .toString();
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
