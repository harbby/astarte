package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.function.Mapper;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.Throwables.throwsException;

public class ShuffleReducer<KEY, ROW, AggValue, VALUE>
        implements Serializable
{
    private final int reduceSize = 10;
    private final ShufflePartition<KEY, AggValue>[] reducePartitions;

    private final Mapper<Iterator<AggValue>, VALUE> mapperReduce;

    /**
     * 传入了Mapper(Iterator(AggValue), VALUE) 这样可能需要数据shuffle完毕后才能开始计算
     * 如果是流计算则应该传入 Reducer(VALUE) 这样可以方便已管道方式进行立即计算
     */
    ShuffleReducer(Operator<ROW> oneParent, Mapper<Iterator<AggValue>, VALUE> mapperReduce)
    {
        this.mapperReduce = mapperReduce;

        this.reducePartitions = new ShufflePartition[reduceSize];
        for (int i = 0; i < reduceSize; i++) {
            reducePartitions[i] = new ShufflePartition<KEY, AggValue>(oneParent.getId(), i);
        }
    }

    public ShufflePartition<KEY, AggValue>[] getPartitions()
    {
        return reducePartitions;
    }

    public Iterator<Tuple2<KEY, VALUE>> compute(ShufflePartition<KEY, AggValue> split)
    {
        try (ShufflePartition<KEY, AggValue> partition = split) {
            Map<KEY, List<Tuple2<KEY, AggValue>>> groupBy = partition.getBuffer().stream().collect(Collectors.groupingBy(x -> x.f1()));
            return groupBy.entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).map(entry -> {
                VALUE value = mapperReduce.map(entry.getValue().stream().map(x -> x.f2()).iterator());
                return Tuple2.of(entry.getKey(), value);
            }).iterator();
        }
        catch (Exception e) {
            throw throwsException(e);
        }
    }
}

