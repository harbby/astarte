package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.function.FlatMapper;
import codepig.ideal.mppwhater.api.function.Mapper;
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
    public Iterator<OUT> compute(Partition partition)
    {
        /**
         * list容器放在外面，通过每次使用前clear保证功能正常
         * 这会极大降低垃圾回收压力,并且严格管道化
         * */
        List<OUT> list = new ArrayList<>();
        return Iterators.concat(Iterators.transform(parentOp.compute(partition), row -> {
            list.clear();
            flatMapper.flatMap(row, list::add);
            return list.iterator();
        }));

        //---算法2: 存在没有完全管道化的问题，大数据量会OOM，空间浪费严重,fgc加重
        //---管道化是非常重要的一个因素
//        List<OUT> list = new ArrayList<>();
//        Iterator<IN> inIterator = parentOp.compute(partition);
//        while (inIterator.hasNext()) {
//            flatMapper.flatMap(inIterator.next(), list::add);
//        }
//        return list.iterator();
    }
}
