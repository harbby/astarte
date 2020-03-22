package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JoinOperatorTest
{
    private final MppContext mppContext = MppContext.builder()
            .setParallelism(2)
            .getOrCreate();

    @Test
    public void ajoinb_test()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        )).reduceByKey(Integer::sum);

        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ageDs.join(ageDs.kvDataSet(x -> x)).collect();

        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", Tuple2.of(18, 18)),
                        Tuple2.of("hp1", Tuple2.of(19, 19)),
                        Tuple2.of("hp2", Tuple2.of(20, 20))));
    }

    @Test
    public void a_join_a_test()
    {
        //自己join自己测试。 这个例子中，正常情况下会多生成一个无用的shuflleMapStage
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        )).reduceByKey(Integer::sum);

        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ageDs.join(ageDs).collect();

        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", Tuple2.of(18, 18)),
                        Tuple2.of("hp1", Tuple2.of(19, 19)),
                        Tuple2.of("hp2", Tuple2.of(20, 20))));
    }

    @Test
    public void cacheAJoinCacheA_test()
    {
        //自己join自己测试。 这个例子中，正常情况下会多生成一个无用的shuflleMapStage
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        )).reduceByKey(Integer::sum).cache();

        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ageDs.join(ageDs).collect();

        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", Tuple2.of(18, 18)),
                        Tuple2.of("hp1", Tuple2.of(19, 19)),
                        Tuple2.of("hp2", Tuple2.of(20, 20))));
    }
}