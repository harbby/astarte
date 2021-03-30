package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;

public class PageRank
{
    public static void main(String[] args)
            throws Exception
    {
        int iters = 3;
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataSet<Tuple2<Integer, int[]>> links = env.fromElements("/data/data/ClueWeb09_WG_50m.graph-txt")
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, int[]>>()
                {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Integer, int[]>> collector)
                            throws Exception
                    {
                        Iterator<Tuple2<Integer, int[]>> iterator = new ClueWeb09IteratorReader(s);
                        while (iterator.hasNext()) {
                            collector.collect(iterator.next());
                        }
                    }
                }).returns(new TupleTypeInfo<>(TypeExtractor.createTypeInfo(int.class), TypeExtractor.createTypeInfo(int[].class)))
                .partitionByHash(new KeySelector<Tuple2<Integer, int[]>, Integer>()
                {
                    @Override
                    public Integer getKey(Tuple2<Integer, int[]> integerTuple2)
                            throws Exception
                    {
                        return integerTuple2.f0;
                    }
                })
                .setParallelism(2);
        DataSet<Tuple2<Integer, Double>> ranks = links.map(new MapFunction<Tuple2<Integer, int[]>, Tuple2<Integer, Double>>()
        {
            @Override
            public Tuple2<Integer, Double> map(Tuple2<Integer, int[]> integerTuple2)
                    throws Exception
            {
                return Tuple2.of(integerTuple2.f0, 1.0);
            }
        }).returns(new TupleTypeInfo<>(TypeExtractor.createTypeInfo(int.class), TypeExtractor.createTypeInfo(double.class)));

        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<Integer, Double>> contribs = links.join(ranks).where((KeySelector<Tuple2<Integer, int[]>, Integer>) integerTuple2 -> integerTuple2.f0)
                    .equalTo((KeySelector<Tuple2<Integer, Double>, Integer>) integerDoubleTuple2 -> integerDoubleTuple2.f0)
                    .flatMap(new FlatMapFunction<Tuple2<Tuple2<Integer, int[]>, Tuple2<Integer, Double>>, Tuple2<Integer, Double>>()
                    {
                        @Override
                        public void flatMap(Tuple2<Tuple2<Integer, int[]>, Tuple2<Integer, Double>> row, Collector<Tuple2<Integer, Double>> collector)
                                throws Exception
                        {
                            int[] urls = row.f0.f1;
                            double rank = row.f1.f1;
                            int size = urls.length;
                            for (int url : urls) {
                                collector.collect(Tuple2.of(url, rank / size));
                            }
                        }
                    }).returns(new TupleTypeInfo<>(TypeExtractor.createTypeInfo(int.class), TypeExtractor.createTypeInfo(double.class)));

            ranks = contribs.groupBy((KeySelector<Tuple2<Integer, Double>, Integer>) integerDoubleTuple2 -> integerDoubleTuple2.f0)
                    .reduce((ReduceFunction<Tuple2<Integer, Double>>) (v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1))
                    .map(new MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>()
                    {
                        @Override
                        public Tuple2<Integer, Double> map(Tuple2<Integer, Double> integerDoubleTuple2)
                                throws Exception
                        {
                            return Tuple2.of(integerDoubleTuple2.f0, 0.15 + 0.85 * integerDoubleTuple2.f1);
                        }
                    }).returns(new TupleTypeInfo<>(TypeExtractor.createTypeInfo(int.class), TypeExtractor.createTypeInfo(double.class)));
        }
        List<Tuple2<Integer, Double>> output = ranks.mapPartition(new MapPartitionFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>()
        {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, Double>> iterable, Collector<Tuple2<Integer, Double>> collector)
                    throws Exception
            {
                Iterator<Tuple2<Integer, Double>> iterator = iterable.iterator();
                int i = 0;
                while (iterator.hasNext() && i++ < 10) {
                    collector.collect(iterator.next());
                }
            }
        }).returns(new TupleTypeInfo<>(TypeExtractor.createTypeInfo(int.class), TypeExtractor.createTypeInfo(double.class)))
                .collect();
        output.forEach(tup -> System.out.printf("%s has rank:  %s .%n", tup.f0, tup.f1));
    }
}
