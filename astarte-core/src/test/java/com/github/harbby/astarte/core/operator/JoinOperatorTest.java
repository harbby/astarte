package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JoinOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
            .getOrCreate();

    @Test
    public void ajoinAHashShuffleJointest()
    {
        //todo: 这个例子如果使用 ShuffleJoin则无法跑通，只能使用localJoin跑通
        //原因在dag 依赖信息存在缺少(仅在该场景存在)
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        )).distinct();
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ageDs.join(ageDs).collect();

        Assert.assertEquals(data,
                Arrays.asList(Tuple2.of("hp", Tuple2.of(18, 18)),
                        Tuple2.of("hp1", Tuple2.of(19, 19)),
                        Tuple2.of("hp2", Tuple2.of(20, 20))));
    }

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
