package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.TaskContext;

import java.io.Serializable;

public interface Stage
        extends Serializable
{
    public Partition[] getPartitions();

    public void compute(Partition split);

    public int getStageId();

    public default int getNumPartitions()
    {
        return getPartitions().length;
    }
}
