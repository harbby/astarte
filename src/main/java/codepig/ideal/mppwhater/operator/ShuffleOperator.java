package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.ShuffleWriter;
import codepig.ideal.mppwhater.api.function.KeyGetter;
import codepig.ideal.mppwhater.api.function.Mapper;
import codepig.ideal.mppwhater.utils.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

/**
 * 按宽依赖将state进行划分
 * state之间串行执行
 */
public class ShuffleOperator<KEY, ROW, AggValue, VALUE>
        extends Operator<Tuple2<KEY, VALUE>>
{
    private final Operator<ROW> oneParent;
    private final KeyGetter<ROW, KEY> keyGetter;
    private final KeyGetter<ROW, AggValue> aggIf;

    private final int reduceSize = 10;
    private final ShuffleReducer<KEY, ROW, AggValue, VALUE> shuffleReducer;

    public ShuffleOperator(Operator<ROW> oneParent, KeyGetter<ROW, KEY> keyGetter,
            KeyGetter<ROW, AggValue> aggIf,
            Mapper<Iterator<AggValue>, VALUE> mapperReduce)
    {
        super(oneParent);
        this.oneParent = oneParent;
        this.keyGetter = keyGetter;
        this.aggIf = aggIf;

        this.shuffleReducer = new ShuffleReducer<>(oneParent, mapperReduce);
    }

    @Override
    public ShufflePartition<KEY, AggValue>[] getPartitions()
    {
        return this.getReducePartitions();
    }

    public Partition[] getMapPartitions()
    {
        return oneParent.getPartitions();
    }

    public ShufflePartition<KEY, AggValue>[] getReducePartitions()
    {
        return shuffleReducer.getPartitions();
    }

    public void shuffleWriter(Partition mapSplit)
    {
        ShuffleWriter<KEY, AggValue> shuffleWriter = new ShuffleWriter.ShuffleWriterImpl<>(getId(), mapSplit.getId(), getReducePartitions());
        Iterator<ROW> iterator = oneParent.compute(mapSplit);
        // aggIf.apply(row) 大幅度降低需要shuffle的数据量
        Iterator<Tuple2<KEY, AggValue>> shuffleIterator = Iterators.map(iterator, row -> Tuple2.of(keyGetter.apply(row), aggIf.apply(row)));

        shuffleWriter.write(shuffleIterator);
    }

    @Override
    public Iterator<Tuple2<KEY, VALUE>> compute(Partition split)
    {
        ShufflePartition<KEY, AggValue> reducePartitions = (ShufflePartition<KEY, AggValue>) split;
        return shuffleReducer.compute(reducePartitions);
    }
}


