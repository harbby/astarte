package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.operator.Operator;

public class ResultStage<E>
        extends Stage
{
    public ResultStage(final Operator<E> operator, int jobId, int stageId)
    {
        super(operator, jobId, stageId);
    }

    @Override
    public void compute(Partition split, TaskContext taskContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Operator<E> getFinalOperator()
    {
        return (Operator<E>) super.getFinalOperator();
    }
}
