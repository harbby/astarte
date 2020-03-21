package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.HashPartitioner;
import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.AshtarteException;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleWriter;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * 按宽依赖将state进行划分
 * state之间串行执行
 */
public class ShuffleMapOperator<K, V>
        extends Operator<Void>
{
    private final Operator<? extends Tuple2<K, V>> operator;
    private final Partitioner partitioner;

    public ShuffleMapOperator(Operator<? extends Tuple2<K, V>> operator, Partitioner partitioner)
    {
        //use default HashPartitioner
        super(operator);
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        this.operator = unboxing(operator);
    }

    public ShuffleMapOperator(Operator<? extends Tuple2<K, V>> operator, int numReducePartitions)
    {
        //use default HashPartitioner
        this(operator, new HashPartitioner(numReducePartitions));
    }

    @Override
    public Partition[] getPartitions()
    {
        return operator.getPartitions();
    }

    public Partitioner getPartitioner()
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
            Iterator<? extends Tuple2<K, V>> iterator = operator.computeOrCache(split, taskContext);
            shuffleWriter.write(iterator);
        }
        catch (Exception e) {
            throw new AshtarteException("shuffle map task failed", e);
        }
        return Iterators.empty();
    }
}


