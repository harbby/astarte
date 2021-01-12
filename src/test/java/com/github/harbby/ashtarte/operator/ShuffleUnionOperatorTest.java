package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.MutableSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ShuffleUnionOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder().setParallelism(1).getOrCreate();

    @Test
    public void unionTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18),
                Tuple2.of("hp2", 21)
        ), 2).reduceByKey(Integer::sum);
        //.distinct();

        //ageDs.print();
        KvDataSet<String, Integer> out = ds1.union(ds2);

        Assert.assertEquals(out.numPartitions(), ds1.numPartitions() + ds2.numPartitions());

        List<Tuple2<String, Integer>> data = out.collect();
        Assert.assertEquals(MutableSet.copy(data),
                MutableSet.of(Tuple2.of("hp", 18),
                        Tuple2.of("hp2", 21)));
    }
}