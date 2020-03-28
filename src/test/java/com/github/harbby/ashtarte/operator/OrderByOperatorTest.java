package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Test;

import java.util.Arrays;

public class OrderByOperatorTest
{
    private final MppContext mppContext = MppContext.builder()
            .setParallelism(2)
            .getOrCreate();

    @Test
    public void orderByTest1()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum);
        ageDs
                .sortByKey(String::compareTo)
                .print();
    }
}