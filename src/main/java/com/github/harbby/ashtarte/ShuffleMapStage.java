package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.operator.ShuffleMapOperator;

public class ShuffleMapStage
        extends Stage
{
    private final ShuffleMapOperator<?, ?> operator;

    public ShuffleMapStage(ShuffleMapOperator<?, ?> operator, int stageId)
    {
        super(operator, stageId);
        this.operator = operator;
    }

    @Override
    public void compute(Partition split, TaskContext taskContext)
    {
        operator.computeOrCache(split, taskContext);
    }
}
