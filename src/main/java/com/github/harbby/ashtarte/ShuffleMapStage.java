package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.operator.ShuffleMapOperator;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

public class ShuffleMapStage
        implements Stage
{
    private final Operator<?> operator;
    private final int stageId;

    public ShuffleMapStage(Operator<?> operator, int stageId)
    {
        checkState(operator instanceof ShuffleMapOperator, "operator not is ShuffleOperator");
        this.operator = operator;
        this.stageId = stageId;
    }

    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    public void compute(Partition split, TaskContext taskContext)
    {
        operator.computeOrCache(split, taskContext);
    }

    @Override
    public int getStageId()
    {
        return stageId;
    }

    @Override
    public Operator<?> getFinalOperator()
    {
        return operator;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", stageId)
                .add("finalOperator", operator)
                .toString();
    }
}
