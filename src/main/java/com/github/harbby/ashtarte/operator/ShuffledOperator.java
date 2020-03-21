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
        extends Operator<Tuple2<KEY, AggValue>> {

    private final ShuffleMapOperator<KEY, AggValue> operator;

    /**
     * 传入了Mapper(Iterator(AggValue), VALUE) 这样可能需要数据shuffle完毕后才能开始计算
     * 如果是流计算则应该传入 Reducer(VALUE) 这样可以方便已管道方式进行立即计算
     */
    public ShuffledOperator(ShuffleMapOperator<KEY, AggValue> operator) {
        super(operator);
        this.operator = operator;
    }

    public ShuffledOperator(ShuffleMapOperator<KEY, AggValue> operator, Partitioner partitioner) {
        super(operator);
        this.operator = operator;
    }

    public Partition[] getPartitions() {
        return IntStream.range(0, operator.getPartitioner().numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public Partitioner getPartitioner() {
        // ShuffledOperator在设计中应该为一切shuffle的后端第一个Operator
        //这里我们提供明确的Partitioner给后续Operator
        return operator.getPartitioner();
    }

    @Override
    public int numPartitions() {
        return operator.getPartitioner().numPartitions();
    }

    @Override
    public Iterator<Tuple2<KEY, AggValue>> compute(Partition split, TaskContext taskContext) {
        for (int shuffleId : taskContext.getDependStages()) {
            return ShuffleManager.getReader(shuffleId, split.getId());
        }
        throw new UnsupportedOperationException();
    }
}

