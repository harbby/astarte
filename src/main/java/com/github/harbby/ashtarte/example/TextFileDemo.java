package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.List;

public class TextFileDemo
{
    public static void main(String[] args)
    {
        MppContext mppContext = MppContext.getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");
        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        DataSet<Tuple2<String, Integer>> worldCounts = worlds.map(x -> Tuple2.of(x, 1))
                .groupBy(x -> x.f1())
                .agg(x -> x.f2(), (x, y) -> x + y);

        List<String> out = worldCounts.map(x -> x.f1() + "," + x.f2()).collect();  //job1
        out.forEach(System.out::println);

        DataSet<Tuple2<String, Long>> worldCounts2 = worldCounts
                .groupBy(x -> x.f1().substring(0, 1))
                .agg(x -> 1L, (x, y) -> x + y);

        List a1 = worldCounts2.collect();  //job2
        long cnt = worldCounts2.count();  //job3
        worldCounts2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
