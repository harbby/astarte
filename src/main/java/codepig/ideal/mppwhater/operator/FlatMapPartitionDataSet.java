package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.function.FlatMapper;
import codepig.ideal.mppwhater.api.function.Mapper;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlatMapPartitionDataSet<IN, OUT>
        extends AbstractDataSet<OUT>
{
    private final FlatMapper<IN, OUT> flatMapper;
    private final AbstractDataSet<IN> parentOp;

    protected FlatMapPartitionDataSet(AbstractDataSet<IN> oneParent, FlatMapper<IN, OUT> flatMapper)
    {
        super(oneParent);
        this.flatMapper = flatMapper;
        this.parentOp = oneParent;
    }

    protected FlatMapPartitionDataSet(AbstractDataSet<IN> oneParent, Mapper<IN, OUT[]> flatMapper)
    {
        super(oneParent);
        this.flatMapper = (row, collector) -> {
            for (OUT value : flatMapper.map(row)) {
                collector.collect(value);
            }
        };
        this.parentOp = oneParent;
    }

    @Override
    public Partition[] getPartitions()
    {
        return parentOp.getPartitions();
    }

    @Override
    public Iterator<OUT> compute(Partition partition)
    {
        return Iterators.concat(Iterators.transform(parentOp.compute(partition), row -> {
            List<OUT> a1 = new ArrayList<>();
            flatMapper.flatMap(row, a1::add);
            return a1.iterator();
        }));
    }
}
