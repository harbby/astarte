package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.Stage;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ShuffleMapStage
        implements Stage
{
    ShuffleOperator operator;

    public ShuffleMapStage(Operator<?> operator)
    {
        checkState(operator instanceof ShuffleOperator, "operator not is ShuffleOperator");
        this.operator = (ShuffleOperator) operator;
    }

    public Partition[] getPartitions()
    {
        return operator.getMapPartitions();
    }

    public void compute(Partition split)
    {
        operator.shuffleWriter(split);
    }

    public int getParallel()
    {
        return getPartitions().length;
    }
}
