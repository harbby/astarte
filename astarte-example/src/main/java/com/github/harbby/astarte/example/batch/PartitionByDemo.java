package com.github.harbby.astarte.example.batch;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.api.DataSet;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class PartitionByDemo
{
    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");
        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Long> kvDataSet = worlds.kvDataSet(x -> new Tuple2<>(x, 1L));
        KvDataSet<String, Long> worldCounts = kvDataSet.partitionBy(2).reduceByKey(Long::sum);

        KvDataSet<String, Long> worldCounts2 = worldCounts
                .rePartition(4)
                .mapKeys(k -> k.substring(0, 1))
                .countByKey(3);

        //List a1 = worldCounts2.collect();  //job2
        //long cnt = worldCounts2.count();  //job3
        worldCounts2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
