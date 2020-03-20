package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;

public class Join2 {
    public static void main(String[] args) {
        MppContext mppContext = MppContext.builder().setParallelism(1).getOrCreate();

        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ));

        KvDataSet<String, String> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", "xi'an"),
                Tuple2.of("hp1", "chengdu")
        ), 2);

        KvDataSet<String, Integer> worldCounts = ds1.reduceByKey(Integer::sum);
        worldCounts.print();

        KvDataSet<String, Tuple2<Integer, String>> out = worldCounts
                .leftJoin(ds2);

        // a,(143, 41)
        out.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
