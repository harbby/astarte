package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.function.FlatMapper;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlatMapDataSet<IN, OUT>
        extends Operator<OUT>
{
    private final FlatMapper<IN, OUT> flatMapper;
    private final Operator<IN> parentOp;

    protected FlatMapDataSet(Operator<IN> oneParent, FlatMapper<IN, OUT> flatMapper)
    {
        super(oneParent);
        this.flatMapper = flatMapper;
        this.parentOp = oneParent;
    }

    protected FlatMapDataSet(Operator<IN> oneParent, Mapper<IN, OUT[]> flatMapper)
    {
        super(oneParent);
        this.flatMapper = (row, collector) -> {
            for (OUT value : flatMapper.map(row)) {
                collector.collect(value);
            }
        };
        this.parentOp = oneParent;
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext)
    {
        /**
         * list容器放在外面，通过每次使用前clear保证功能正常
         * 这会极大降低垃圾回收压力,并且严格管道化
         * */
        List<OUT> list = new ArrayList<>();
        return Iterators.concat(Iterators.transform(parentOp.compute(partition, taskContext), row -> {
            list.clear();
            flatMapper.flatMap(row, list::add);
            return list.iterator();
        }));
    }
}
