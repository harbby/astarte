package codepig.ideal.mppwhater.api.function;

import codepig.ideal.mppwhater.api.DataSet;
import codepig.ideal.mppwhater.api.Tuple2;

public interface KeyedFunction<KEY, ROW>
{
    DataSet<Tuple2<KEY, Long>> count();

    DataSet<ROW> reduce(Reducer<ROW> reducer);
}
