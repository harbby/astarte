package com.github.harbby.astarte;

import com.github.harbby.astarte.api.Partition;
import com.github.harbby.astarte.api.Stage;
import com.github.harbby.astarte.operator.Operator;

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
