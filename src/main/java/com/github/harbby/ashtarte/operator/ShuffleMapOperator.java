package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.AshtarteException;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleWriter;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.IOException;
import java.util.Iterator;

/**
 * 按宽依赖将state进行划分
 * state之间串行执行
 */
public class ShuffleMapOperator<K, V>
        extends Operator<Void>
{
    private final Operator<Tuple2<K, V>> operator;
    private final Partitioner<K> partitioner;

    public ShuffleMapOperator(Operator<Tuple2<K, V>> operator, Partitioner<K> partitioner)
    {
        //use default HashPartitioner
        super(operator);
        this.partitioner = partitioner;
        this.operator = operator;
    }

    public ShuffleMapOperator(Operator<Tuple2<K, V>> operator, int numReducePartitions)
    {
        //use default HashPartitioner
        this(operator, new HashPartitioner<>(numReducePartitions));
    }

    @Override
    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    public Partitioner<K> getPartitioner()
    {
        return partitioner;
    }

    @Override
    public Iterator<Void> compute(Partition split, TaskContext taskContext)
    {
        try (ShuffleWriter<K, V> shuffleWriter = ShuffleWriter.createShuffleWriter(
                taskContext.getStageId(),
                split.getId(),
                partitioner)) {

            Iterator<Tuple2<K, V>> iterator = operator.compute(split, taskContext);
            shuffleWriter.write(iterator);
        }
        catch (IOException e) {
            throw new AshtarteException("shuffle map task failed", e);
        }
        return Iterators.empty();
    }
}


