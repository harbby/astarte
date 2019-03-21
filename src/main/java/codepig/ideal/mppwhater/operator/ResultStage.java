package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.Stage;

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
