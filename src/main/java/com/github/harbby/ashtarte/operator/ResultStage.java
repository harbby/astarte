package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;

public class ResultStage<E>
        implements Stage
{
    private final Operator<E> operator;

    public ResultStage(final Operator<E> operator)
    {
        this.operator = operator;
    }

    @Override
    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    @Override
    public void compute(Partition split)
    {
        operator.compute(split);
    }

    @Override
    public int getParallel()
    {
        return getPartitions().length;
    }
}
