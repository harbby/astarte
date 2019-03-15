package codepig.ideal.mppwhater.api.operator;

import codepig.ideal.mppwhater.MppContext;
import codepig.ideal.mppwhater.api.DataSet;
import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.function.Filter;
import codepig.ideal.mppwhater.api.function.FlatMapper;
import codepig.ideal.mppwhater.api.function.Foreach;
import codepig.ideal.mppwhater.api.function.KeyGetter;
import codepig.ideal.mppwhater.api.function.KeyedFunction;
import codepig.ideal.mppwhater.api.function.Mapper;
import codepig.ideal.mppwhater.api.function.Reducer;

import java.util.Iterator;
import java.util.List;

public abstract class AbstractDataSet<ROW>
        implements DataSet<ROW>
{
    private final transient MppContext yarkContext;

    protected AbstractDataSet(MppContext yarkContext)
    {
        this.yarkContext = yarkContext;
    }

    protected AbstractDataSet(AbstractDataSet<?> oneParent)
    {
        this(oneParent.getYarkContext());
    }

    public MppContext getYarkContext()
    {
        return yarkContext;
    }

    protected AbstractDataSet<ROW> firstParent()
    {
        return null;
    }

    public abstract Iterator<ROW> compute(Partition split);

    @Override
    public <OUT> DataSet<OUT> map(Mapper<ROW, OUT> mapper)
    {
        return new MapPartitionDataSet<>(this, mapper);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(Mapper<ROW, OUT[]> flatMapper)
    {
        return new FlatMapPartitionDataSet<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(FlatMapper<ROW, OUT> flatMapper)
    {
        return new FlatMapPartitionDataSet<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> mapPartition()
    {
        return null;
    }

    @Override
    public DataSet<ROW> filter(Filter<ROW> filter)
    {
        return new FilterPartitionDataSet<>(this, filter);
    }

    @Override
    public ROW reduce(Reducer<ROW> reducer)
    {
        return null;
    }

    @Override
    public <KEY> KeyedFunction<KEY, ROW> groupBy(KeyGetter<ROW, KEY> keyGetter)
    {
        return new KeyedDataSet<>(this, keyGetter);
    }

    //---action operator
    @Override
    public List<ROW> collect()
    {
        return yarkContext.collect(this);
    }

    @Override
    public void foreach(Foreach<ROW> foreach)
    {
        yarkContext.execJob(this, (Iterator<ROW> iterator) -> {
            while (iterator.hasNext()) {
                foreach.apply(iterator.next());
            }
        });
    }

    @Override
    public void foreachPartition(Foreach<Iterator<ROW>> partitionForeach)
    {
        yarkContext.execJob(this, partitionForeach);
    }
}
