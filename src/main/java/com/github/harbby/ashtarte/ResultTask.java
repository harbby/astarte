package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.utils.SerializableObj;

import java.util.Iterator;

public class ResultTask<E, R> implements Task<R> {

    private final SerializableObj<Stage> serializableStage;
    private final Mapper<Iterator<E>, R> func;
    private final Partition partition;

    public ResultTask(
            SerializableObj<Stage> serializableStage,
            Mapper<Iterator<E>, R> func,
            Partition partition) {
        this.serializableStage = serializableStage;
        this.func = func;
        this.partition = partition;
    }

    @Override
    public long getTaskId() {
        return 0;
    }

    @Override
    public R runTask(TaskContext taskContext) {
        @SuppressWarnings("unchecked")
        ResultStage<E> resultStage = (ResultStage<E>) serializableStage.getValue();
        Operator<E> operator = resultStage.getFinalOperator();
        Iterator<E> iterator = operator.computeOrCache(partition, taskContext);
        R r = func.map(iterator);
        return r;
    }
}
