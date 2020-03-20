package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class JoinDemo {
    public static void main(String[] args) {
        MppContext mppContext = MppContext.builder().setParallelism(1).getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");

        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Long> kvDataSet = worlds.kvDataSet(x -> new Tuple2<>(x.substring(0, 1), 1L));
        KvDataSet<String, Long> worldCounts = kvDataSet.partitionBy(2).reduceByKey(Long::sum);
        //worldCounts.print();

        KvDataSet<String, Long> worldLengths = worlds.kvDataSet(x -> new Tuple2<>(x.substring(0, 1), (long) x.length()))
                .partitionBy(2).reduceByKey(Long::sum);
        //worldLengths.print();

        KvDataSet<String, Tuple2<Long, Long>> ds2 = worldLengths
                .join(worldCounts);

        // a,(143, 41)
        ds2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
