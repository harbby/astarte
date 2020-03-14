package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ShuffleMapStage
        implements Stage
{
    private final Operator<?> operator;
    private final int stageId;

    public ShuffleMapStage(Operator<?> operator,
            int stageId)
    {
        checkState(operator instanceof ShuffleMapOperator, "operator not is ShuffleOperator");
        this.operator = operator;
        this.stageId = stageId;
    }

    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    public void compute(Partition split)
    {
        operator.compute(split, () -> stageId);
    }

    @Override
    public int getStageId()
    {
        return stageId;
    }
}
