package codepig.ideal.mppwhater.api;

import codepig.ideal.mppwhater.api.function.Filter;
import codepig.ideal.mppwhater.api.function.FlatMapper;
import codepig.ideal.mppwhater.api.function.Foreach;
import codepig.ideal.mppwhater.api.function.KeyGetter;
import codepig.ideal.mppwhater.api.function.KeyedFunction;
import codepig.ideal.mppwhater.api.function.Mapper;
import codepig.ideal.mppwhater.api.function.Reducer;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public interface DataSet<Row>
        extends Serializable
{
    public abstract Partition[] getPartitions();

    List<Row> collect();

    <OUT> DataSet<OUT> map(Mapper<Row, OUT> mapper);

    <OUT> DataSet<OUT> flatMap(Mapper<Row, OUT[]> flatMapper);

    <OUT> DataSet<OUT> flatMap(FlatMapper<Row, OUT> flatMapper);

    <OUT> DataSet<OUT> mapPartition(FlatMapper<Iterator<Row>, OUT> flatMapper);

    DataSet<Row> filter(Filter<Row> filter);

    Row reduce(Reducer<Row> reducer);

    <KEY> KeyedFunction<KEY, Row> groupBy(KeyGetter<Row, KEY> keyGetter);

    void foreach(Foreach<Row> foreach);

    void foreachPartition(Foreach<Iterator<Row>> partitionForeach);
}
