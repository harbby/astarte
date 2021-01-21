package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Test;

import java.util.Arrays;

public class CacheOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();

    @Test
    public void cacheUnCacheTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).mapValues(x -> x).cache().reduceByKey(Integer::sum).cache();

        ds1.print();

        //todo: 未实现该算子
        ds1.unCache();

        System.out.println();
    }
}
