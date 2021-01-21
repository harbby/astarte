package com.github.harbby.astarte.example.batch;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.api.DataSet;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class TextFileDemo
{
    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");
        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Long> worldCounts = worlds.kvDataSet(x -> Tuple2.of(x, 1L))
                .reduceByKey((x, y) -> x + y);

        //List<String> out = worldCounts.map(x -> x.f1() + "," + x.f2()).collect();  //job1
        //out.forEach(System.out::println);

        DataSet<Tuple2<String, Long>> worldCounts2 = worldCounts.rePartition(4)
                .mapKeys(k -> k.substring(0, 1))
                .reduceByKey((x, y) -> x + y);

        //List a1 = worldCounts2.collect();  //job2
        //long cnt = worldCounts2.count();  //job3
        worldCounts2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
