package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.HashPartitioner;
import com.github.harbby.astarte.core.Partitioner;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.ShuffleWriter;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.IOException;
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
    private final Comparator<K> sortShuffle;

    public ShuffleMapOperator(
            Operator<? extends Tuple2<K, V>> operator,
            Partitioner partitioner)
    {
        this(operator, partitioner, null);
    }

    public ShuffleMapOperator(
            Operator<? extends Tuple2<K, V>> operator,
            Partitioner partitioner,
            Comparator<K> sortShuffle)
    {
        //use default HashPartitioner
        super(operator);
        this.partitioner = requireNonNull(partitioner, "partitioner is null");
        this.operator = unboxing(operator);
        this.sortShuffle = sortShuffle;
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
                taskContext.executorUUID(),
                taskContext.getJobId(),
                taskContext.getStageId(), split.getId(), partitioner, sortShuffle)) {
            Iterator<? extends Tuple2<K, V>> iterator = operator.computeOrCache(split, taskContext);
            shuffleWriter.write(iterator);
        }
        catch (IOException e) {
            throw new AstarteException("shuffle map task failed", e);
        }
        return Iterators.empty();
    }
}
