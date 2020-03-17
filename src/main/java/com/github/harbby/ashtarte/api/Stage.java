package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.operator.Operator;

import java.io.Serializable;

public interface Stage
        extends Serializable
{
    public Partition[] getPartitions();

    public void compute(Partition split, TaskContext taskContext);

    public int getStageId();

    public Operator<?> getFinalOperator();

    public default int getNumPartitions()
    {
        return getPartitions().length;
    }
}
