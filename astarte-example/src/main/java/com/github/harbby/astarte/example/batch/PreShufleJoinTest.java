package com.github.harbby.astarte.example.batch;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;

public class PreShufleJoinTest
{
    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();

        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, String> cityDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", "xi'an"),
                Tuple2.of("hp1", "chengdu")
        ), 2)
                .partitionBy(1);
                //.distinct();

        //ageDs.print();
        KvDataSet<String, Tuple2<Integer, String>> out = ageDs.leftJoin(cityDs);

        // a,(143, 41)
        out.distinct().foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
