package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleManager;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * shuffle Reducer reader
 */
public class ShuffledOperator<KEY, AggValue>
        extends Operator<Tuple2<KEY, AggValue>>
{

    private final Partitioner partitioner;

    public ShuffledOperator(ShuffleMapOperator<KEY, AggValue> operator, Partitioner partitioner)
    {
        super(operator);
        this.partitioner = partitioner;
    }

    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public Partitioner getPartitioner()
    {
        // ShuffledOperator在设计中应该为一切shuffle的后端第一个Operator
        //这里我们提供明确的Partitioner给后续Operator
        return partitioner;
    }

    @Override
    public int numPartitions()
    {
        return partitioner.numPartitions();
    }

    @Override
    public Iterator<Tuple2<KEY, AggValue>> compute(Partition split, TaskContext taskContext)
    {
        for (int shuffleId : taskContext.getDependStages()) {
            return ShuffleManager.getReader(shuffleId, split.getId());
        }
        throw new UnsupportedOperationException();
    }
}

