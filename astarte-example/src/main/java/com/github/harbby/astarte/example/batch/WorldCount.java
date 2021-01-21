package com.github.harbby.astarte.example.batch;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.api.DataSet;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class WorldCount
{
    private WorldCount() {}

    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");
        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Integer> worldCounts = worlds.kvDataSet(x -> Tuple2.of(x.toLowerCase(), 1))
                .reduceByKey((x, y) -> x + y)
                .sortByValue((x, y) -> y.compareTo(x))
                .limit(10);

        worldCounts.collect().forEach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
