package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;

public class ResultStage<E>
        implements Stage
{
    private final Operator<E> operator;
    private final int stageId;

    public ResultStage(final Operator<E> operator, int stageId)
    {
        this.operator = operator;
        this.stageId = stageId;
    }

    @Override
    public Operator<E> getFinalOperator()
    {
        return operator;
    }

    @Override
    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    @Override
    public void compute(Partition split, TaskContext taskContext)
    {
        operator.compute(split, taskContext);
    }

    @Override
    public int getStageId()
    {
        return stageId;
    }
}
