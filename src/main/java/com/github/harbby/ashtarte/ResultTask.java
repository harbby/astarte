package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.operator.Operator;

import java.util.Iterator;

public class ResultTask<E, R>
        implements Task<R>
{

    private final Stage stage;
    private final Mapper<Iterator<E>, R> func;
    private final Partition partition;

    public ResultTask(
            Stage stage,
            Mapper<Iterator<E>, R> func,
            Partition partition)
    {
        this.stage = stage;
        this.func = func;
        this.partition = partition;
    }

    @Override
    public Stage getStage()
    {
        return stage;
    }

    @Override
    public long getTaskId()
    {
        return partition.getId();
    }

    @Override
    public R runTask(TaskContext taskContext)
    {
        @SuppressWarnings("unchecked")
        ResultStage<E> resultStage = (ResultStage<E>) stage;
        Operator<E> operator = resultStage.getFinalOperator();
        Iterator<E> iterator = operator.computeOrCache(partition, taskContext);
        R r = func.map(iterator);
        return r;
    }
}
