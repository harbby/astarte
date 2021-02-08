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
}
