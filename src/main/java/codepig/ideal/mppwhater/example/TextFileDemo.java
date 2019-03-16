package codepig.ideal.mppwhater.example;

import codepig.ideal.mppwhater.MppContext;
import codepig.ideal.mppwhater.api.DataSet;
import codepig.ideal.mppwhater.api.Tuple2;

public class TextFileDemo
{
    public static void main(String[] args)
    {
        MppContext mppContext = MppContext.getOrCreate();

        DataSet<String> ds = mppContext.textFile("/ideal/hadoop/spark/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        DataSet<Tuple2<String, Integer>> worldCounts = worlds.map(x -> Tuple2.of(x, 1))
                .groupBy(x -> x.f1())
                .reduce((x, y) ->
                        Tuple2.of(x.f1(), x.f2() + y.f2())
                );

        worldCounts.collect()
                .forEach(x -> System.out.println(x.f1() + "," + x.f2()));

        DataSet<Tuple2<String, Long>> worldCounts2 = worlds
                .groupBy(x -> "a1")
                .count();
        worldCounts2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));
    }
}
