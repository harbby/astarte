package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.operator.ShuffleMapOperator;

public class ShuffleMapStage
        extends Stage
{
    public ShuffleMapStage(ShuffleMapOperator<?, ?> operator, int jobId, int stageId)
    {
        super(operator, jobId, stageId);
    }

    @Override
    public void compute(Partition split, TaskContext taskContext)
    {
        getFinalOperator().computeOrCache(split, taskContext);
    }
}
