package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.operator.Operator;

public class ResultStage<E>
        extends Stage
{
    public ResultStage(final Operator<E> operator, int stageId)
    {
        super(operator, stageId);
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
