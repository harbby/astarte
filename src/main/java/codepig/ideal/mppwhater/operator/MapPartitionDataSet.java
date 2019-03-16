package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.function.Mapper;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class MapPartitionDataSet<IN, OUT>
        extends Operator<OUT>
{
    private final Operator<IN> parentOp;
    private final Mapper<IN, OUT> mapper;

    public MapPartitionDataSet(Operator<IN> parentOp, Mapper<IN, OUT> mapper)
    {
        super(parentOp);
        this.parentOp = parentOp;
        this.mapper = mapper;
    }

    @Override
    public Partition[] getPartitions()
    {
        return parentOp.getPartitions();
    }

    @Override
    public Iterator<OUT> compute(Partition partition)
    {
        return Iterators.transform(parentOp.compute(partition), mapper::map);
    }
}
