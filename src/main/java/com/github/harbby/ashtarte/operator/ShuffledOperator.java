package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleManager;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Iterator;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * 每个stage只需包含自己相关算子的引用。这样序列化dag时将只会包含自己相关引用
 * 以此目前Stage仅有的两个firstOperator是[ShuffledOperator, ShuffleJoinOperator]
 * 我们在[ShuffledOperator, ShuffleJoinOperator]算子里不能包含任何Operator的引用.
 * see: clearOperatorDependencies
 *
 * <p>
 * shuffle Reducer reader
 */
public class ShuffledOperator<K, V>
        extends Operator<Tuple2<K, V>>
{
    private final Partitioner partitioner;
    private final int shuffleMapOperatorId;

    public ShuffledOperator(ShuffleMapOperator<K, V> operator, Partitioner partitioner)
    {
        super(operator);
        this.shuffleMapOperatorId = operator.getId();
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
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext taskContext)
    {
        Integer shuffleId = taskContext.getDependStages().get(shuffleMapOperatorId);
        checkState(shuffleId != null);
        return ShuffleManager.getReader(shuffleId, split.getId());
    }
}

