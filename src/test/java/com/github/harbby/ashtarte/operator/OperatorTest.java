package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.api.AshtarteException;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OperatorTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .setParallelism(2)
            .getOrCreate();

    @Test
    public void zipWithIndex()
    {
        DataSet<String> links = mppContext.makeDataSet(Arrays.asList("a",
                "b",
                "c",
                "d",
                "e"), 2);

        KvDataSet<String, Long> zipIndex = links.zipWithIndex();
        List<Long> indexs = zipIndex.values().collect();
        Assert.assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), indexs);
    }

    @Test
    public void clearOperatorDependenciesTest()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum);

        Assert.assertEquals(ageDs.collectMap(), ageDs.collectMap());
    }

    @Test(expected = AshtarteException.class)
    public void jobRunFailedTest()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2).mapValues(x -> {
            int a1 = x / 0;
            return x;
        }).reduceByKey(Integer::sum);
        ageDs.collect();
    }
}