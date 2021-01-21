package com.github.harbby.astarte.example.batch;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;

public class UnionAllDemo
{
    private UnionAllDemo() {}

    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();

        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum);
        //.distinct();

        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        out.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
