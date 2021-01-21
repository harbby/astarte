package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.MutableMap;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class UnionAllOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();

    @Test
    public void baseUnionAllTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Collections.singletonList(
                Tuple2.of("hp", 18)
        ));
        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Collections.singletonList(
                Tuple2.of("hp1", 20)
        ));

        Map<String, Integer> out = ds1.union(ds2).collectMap();
        Assert.assertEquals(MutableMap.of(
                "hp", 18,
                "hp1", 20
        ), out);
    }

    @Test
    public void kvDsUnionAllTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21)
        ), 2).reduceByKey(Integer::sum);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        Map<String, Integer> data = out.collectMap();
        Assert.assertEquals(data,
                MutableMap.of(
                        "hp", 20,
                        "hp1", 19,
                        "hp2", 21
                ));
    }

    @Test
    public void unionAllOneShuffleTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18)
        )).mapValues(x -> x);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21)
        ), 2).reduceByKey(Integer::sum).mapValues(x -> x);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        Map<String, Integer> data = out.collectMap();
        Assert.assertEquals(data,
                MutableMap.of(
                        "hp", 20,
                        "hp1", 19,
                        "hp2", 21
                ));
    }

    @Test
    public void unionAllNoShuffleTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18)
        )).mapValues(x -> x);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21)
        ), 2).mapValues(x -> x);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);
        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        Map<String, Integer> data = out.collectMap();
        Assert.assertEquals(data,
                MutableMap.of(
                        "hp", 20,
                        "hp1", 19,
                        "hp2", 21
                ));
    }
}