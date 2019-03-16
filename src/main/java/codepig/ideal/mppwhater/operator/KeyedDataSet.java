package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.HashPartitioner;
import codepig.ideal.mppwhater.Partitioner;
import codepig.ideal.mppwhater.api.DataSet;
import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.Tuple2;
import codepig.ideal.mppwhater.api.function.KeyGetter;
import codepig.ideal.mppwhater.api.function.KeyedFunction;
import codepig.ideal.mppwhater.api.function.Reducer;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * shuffle
 */
public class KeyedDataSet<KEY, ROW>
        implements KeyedFunction<KEY, ROW>
{
    private final KeyGetter<ROW, KEY> keyGetter;
    private final Operator<ROW> oneParent;

    protected KeyedDataSet(Operator<ROW> oneParent, KeyGetter<ROW, KEY> keyGetter)
    {
        this.keyGetter = keyGetter;
        this.oneParent = oneParent;
    }

    @Override
    public DataSet<Tuple2<KEY, Long>> count()
    {
        Partition[] partitions = oneParent.getPartitions();
        List<ROW> a1 = new ArrayList<>();
        for (Partition partition : partitions) {
            Iterator<ROW> iterator = null;
            iterator = oneParent.compute(partition);
            a1.addAll(ImmutableList.copyOf(iterator));
        }

        Map<KEY, List<ROW>> groupby = a1.stream().collect(Collectors.groupingBy(x -> keyGetter.apply(x)));
        List<Tuple2<KEY, Long>> out = groupby.entrySet().stream().map(x -> Tuple2.of(x.getKey(), (long) x.getValue().size()))
                .collect(Collectors.toList());

        return oneParent.getYarkContext().fromCollection(out);
    }

    private void shuffle()
    {
        final Partitioner<KEY> hashPartitioner = new HashPartitioner<>();
        Partition[] partitions = oneParent.getPartitions();
        for (Partition split : partitions) {
            Iterator<ROW> iterator = oneParent.compute(split);
            while (iterator.hasNext()) {
                ROW row = iterator.next();
                KEY key = keyGetter.apply(row);   //
                int partition = hashPartitioner.getPartition(key, partitions.length);
            }
        }
    }

    @Override
    public DataSet<ROW> reduce(Reducer<ROW> reducer)
    {
        //new ReduceDataSet<KEY, ROW> (oneParent, reducer);

        final Partitioner<KEY> hashPartitioner = new HashPartitioner<>();
        Partition[] partitions = oneParent.getPartitions();

        List<ROW> a1 = new ArrayList<>();
        for (Partition split : partitions) {
            Iterator<ROW> iterator = oneParent.compute(split);
//            while (iterator.hasNext()) {
//                ROW row = iterator.next();
//                KEY key = keyGetter.apply(row);
//                int partition = hashPartitioner.getPartition(key, partitions.length);
//            }
            a1.addAll(ImmutableList.copyOf(iterator));
        }

        Map<KEY, List<ROW>> groupby = a1.stream().collect(Collectors.groupingBy(x -> keyGetter.apply(x)));
        List<ROW> out = groupby.values().stream().map(list -> {
            return list.stream().reduce(reducer::reduce).get();
        }).collect(Collectors.toList());
        return oneParent.getYarkContext().fromCollection(out);
        //reducer.reduce()
    }

//    public static class ReduceDataSet<KEY, E>
//            extends Operator<E>
//    {
//        private final Operator<E> oneParent;
//        protected ReduceDataSet(Operator<E> oneParent, Reducer<E> reducer)
//        {
//            super(oneParent);
//            this.oneParent = oneParent;
//
//            final Partitioner<KEY> hashPartitioner = new HashPartitioner<>();
//            Partition[] partitions = oneParent.getPartitions();
//
//            List<ROW> a1 = new ArrayList<>();
//            for (Partition split : partitions) {
//                Iterator<ROW> iterator = oneParent.compute(split);
//                while (iterator.hasNext()) {
//                    ROW row = iterator.next();
//                    KEY key = keyGetter.apply(row);
//                    int partition = hashPartitioner.getPartition(key, partitions.length);
//                }
//                a1.addAll(ImmutableList.copyOf(iterator));
//            }
//
//            Map<KEY, List<ROW>> groupby = a1.stream().collect(Collectors.groupingBy(x -> keyGetter.apply(x)));
//            List<ROW> out = groupby.values().stream().map(list -> {
//                return list.stream().reduce(reducer::reduce).get();
//            }).collect(Collectors.toList());
//        }
//
//        @Override
//        public Partition[] getPartitions()
//        {
//            return new Partition[0];
//        }
//
//        @Override
//        public Iterator<E> compute(Partition split)
//        {
//            Iterator<E> iterator = oneParent.compute(split);
//            while (iterator.hasNext()) {
//                E row = iterator.next();
//                KEY key = keyGetter.apply(row);
//                int partition = hashPartitioner.getPartition(key, partitions.length);
//            }
//        }
//    }
}
