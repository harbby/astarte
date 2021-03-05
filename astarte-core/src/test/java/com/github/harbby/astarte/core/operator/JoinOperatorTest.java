/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class JoinOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
            .getOrCreate();

    @Test
    public void aJoinAHashShuffleJoinTest()
    {
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
    public void aJoinBTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 1),
                Tuple2.of("hp", 1),
                Tuple2.of("hp1", 1)
        ));
        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 2),
                Tuple2.of("hp2", 2)
        ));
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds1.join(ds2.kvDataSet(x -> x)).collect();
        Assert.assertEquals(data,
                Arrays.asList(
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp1", Tuple2.of(1, 2))));
    }

    @Test
    public void aJoinBRightJoinTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 1),
                Tuple2.of("hp", 1),
                Tuple2.of("hp1", 1)
        ));
        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 2),
                Tuple2.of("hp2", 2)
        ));
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds1.rightJoin(ds2.kvDataSet(x -> x)).collect();
        Assert.assertEquals(data,
                Arrays.asList(
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp1", Tuple2.of(1, 2)),
                        Tuple2.of("hp2", Tuple2.of(null, 2))));
    }

    @Test
    public void aJoinBLeftJoinTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 1),
                Tuple2.of("hp", 1),
                Tuple2.of("hp1", 1)
        ));
        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 2),
                Tuple2.of("hp2", 2)
        ));
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds1.leftJoin(ds2.kvDataSet(x -> x)).collect();
        Assert.assertEquals(data,
                Arrays.asList(
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp1", Tuple2.of(1, 2))));
    }

    @Test
    public void aJoinBFullJoinTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 1),
                Tuple2.of("hp", 1),
                Tuple2.of("hp1", 1),
                Tuple2.of("hp3", 1)
        ));
        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 2),
                Tuple2.of("hp2", 2)
        ));
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds1.fullJoin(ds2.kvDataSet(x -> x)).collect();
        Assert.assertEquals(data,
                Arrays.asList(
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp", Tuple2.of(1, 2)),
                        Tuple2.of("hp1", Tuple2.of(1, 2)),
                        Tuple2.of("hp2", Tuple2.of(null, 2)),
                        Tuple2.of("hp3", Tuple2.of(1, null))));
    }

    @Test
    public void a_join_a_test()
    {
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
    public void sameEqualLocalJoinTest()
    {
        KvDataSet<String, Integer> ds = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ));
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds.join(ds).collect();
        Assert.assertEquals(Arrays.asList(
                Tuple2.of("hp", Tuple2.of(8, 8)),
                Tuple2.of("hp", Tuple2.of(8, 10)),
                Tuple2.of("hp", Tuple2.of(10, 8)),
                Tuple2.of("hp", Tuple2.of(10, 10)),
                Tuple2.of("hp1", Tuple2.of(19, 19)),
                Tuple2.of("hp2", Tuple2.of(20, 20))),
                data);
    }

    @Test
    public void should3StageUseShuffleJoinTest2()
    {
        KvDataSet<String, Integer> ds = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 2);

        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds.mapValues(x -> x).join(ds.mapValues(x -> x)).collect();
        data.sort(Comparator.comparing(it -> it.f1));
        Assert.assertEquals(Arrays.asList(
                Tuple2.of("hp", Tuple2.of(8, 8)),
                Tuple2.of("hp", Tuple2.of(10, 8)),
                Tuple2.of("hp", Tuple2.of(8, 10)),
                Tuple2.of("hp", Tuple2.of(10, 10)),
                Tuple2.of("hp1", Tuple2.of(19, 19)),
                Tuple2.of("hp2", Tuple2.of(20, 20))),
                data);
    }

    /**
     * 同source源 join可以尝试使用如下方式进行`pre shuffle local join`
     * 这里优化后只有两个stage，比上面方法{@link JoinOperatorTest#should3StageUseShuffleJoinTest2}少一个stage。
     */
    @Test
    public void should2StageUseLocalJoinTest3()
    {
        KvDataSet<String, Integer> ds = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 2).rePartitionByKey();
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = ds.mapValues(x -> x).mapValues(x -> x)
                .join(ds.mapValues(x -> x)).collect();
        data.sort(Comparator.comparing(it -> it.f1));
        Assert.assertEquals(Arrays.asList(
                Tuple2.of("hp", Tuple2.of(8, 8)),
                Tuple2.of("hp", Tuple2.of(8, 10)),
                Tuple2.of("hp", Tuple2.of(10, 8)),
                Tuple2.of("hp", Tuple2.of(10, 10)),
                Tuple2.of("hp1", Tuple2.of(19, 19)),
                Tuple2.of("hp2", Tuple2.of(20, 20))),
                data);
    }

    @Test
    public void cacheLocalJoinTest()
    {
        KvDataSet<String, int[]> ds = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 1).groupByKey()
                .mapValues(x -> ((List<Integer>) x).stream().mapToInt(y -> y).toArray())
                .cache();
        Assert.assertEquals(3, ds.mapValues(x -> 1).join(ds).count());
        Assert.assertEquals(3, ds.mapValues(x -> 1).mapValues(x -> x).join(ds).count());
        ds.unCache();
    }

    @Test
    public void cacheLocalJoinTest2()
    {
        mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        ), 1).mapValues(x -> x + 1)
                .mapKeys(x -> x + 1)
                .filter(x -> true)
                .map(x -> x)
                .flatMap(x -> new Object[] {x, x})
                //.map(x -> x)
                .print();

//        KvDataSet<String, Integer> ds = mppContext.makeKvDataSet(Arrays.asList(
//                Tuple2.of("hp", 8),
//                Tuple2.of("hp", 10)
//        ), 2).cache();
//
//        ds.mapKeys(x -> x).join(ds).print();
//        ds.unCache();
    }
}
