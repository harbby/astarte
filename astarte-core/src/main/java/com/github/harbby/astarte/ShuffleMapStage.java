package com.github.harbby.astarte;

import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.Stage;
import com.github.harbby.astarte.operator.ShuffleMapOperator;

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
