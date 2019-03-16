package codepig.ideal.mppwhater.operator;

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

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public abstract class Operator<ROW>
        implements DataSet<ROW>
{
    private final transient MppContext yarkContext;
    private final Operator<?> oneParent;

    protected Operator(MppContext yarkContext)
    {
        this(yarkContext, null);
    }

    private Operator(MppContext yarkContext, Operator<?> oneParent)
    {
        this.yarkContext = yarkContext;
        this.oneParent = oneParent;
    }

    protected Operator(Operator<?> oneParent)
    {
        this(oneParent.getYarkContext(), oneParent);
    }

    public MppContext getYarkContext()
    {
        return yarkContext;
    }

    protected Operator<ROW> firstParent()
    {
        return null;
    }

    @Override
    public Partition[] getPartitions()
    {
        checkState(oneParent != null, this.getClass() + " Parent Operator is null, Source Operator mush @Override Method");
        return oneParent.getPartitions();
    }

    protected void close() {}

    public abstract Iterator<ROW> compute(Partition split);

    @Override
    public <OUT> DataSet<OUT> map(Mapper<ROW, OUT> mapper)
    {
        return new MapDataSet<>(this, mapper);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(Mapper<ROW, OUT[]> flatMapper)
    {
        return new FlatMapDataSet<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> flatMap(FlatMapper<ROW, OUT> flatMapper)
    {
        return new FlatMapDataSet<>(this, flatMapper);
    }

    @Override
    public <OUT> DataSet<OUT> mapPartition(FlatMapper<Iterator<ROW>, OUT> flatMapper)
    {
        return new MapPartitionOperator<>(this, flatMapper);
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
