package com.github.harbby.astarte;

import com.github.harbby.astarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Test;

import java.util.Arrays;

public class ForkVmJobSchedulerTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
            .getOrCreate();

    @Test
    public void forkVmSchedulerPrintTest()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum);
        ageDs.print();
    }

    @Test
    public void forkVmSchedulerCollectTest()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum);
        ageDs.collect().forEach(x -> System.out.println(x));
    }
}