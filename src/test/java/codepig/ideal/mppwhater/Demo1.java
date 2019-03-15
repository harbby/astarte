package codepig.ideal.mppwhater;

import com.google.common.collect.ImmutableList;
import codepig.ideal.mppwhater.api.DataSet;
import codepig.ideal.mppwhater.api.Tuple2;

public class Demo1
{
    public static void main(String[] args)
    {
        MppContext mppContext = MppContext.getOrCreate();
        DataSet<String> dataSet = mppContext.fromCollection(ImmutableList.of("1", "2", "3"));

        DataSet<Integer> a1 = dataSet.map(k -> {
            return Integer.parseInt(k);
        });
        a1 = a1.map(k -> k + 1);

        a1.foreach(x -> {
            System.out.println(x);
        });

        DataSet<String> ds = mppContext.textFile("/ideal/hadoop/spark/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        DataSet<Tuple2<String, Integer>> worldCounts = worlds.map(x -> Tuple2.of(x, 1))
                .groupBy(x -> x.f1())
                .reduce((x, y) ->
                        Tuple2.of(x.f1(), x.f2() + y.f2())
                );

        worldCounts.foreach(x -> System.out.println(x.f1() + "," + x.f2()));

        DataSet<Tuple2<String, Long>> worldCounts2 = worlds
                .groupBy(x -> "a1")
                .count();
        worldCounts2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));
    }
}
