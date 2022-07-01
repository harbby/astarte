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
package com.github.harbby.astarte.core.concurrent;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.collection.MutableMap;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StabilityTest
{
    private static final Logger logger = LoggerFactory.getLogger(StabilityTest.class);
    private final BatchContext mppContext = BatchContext.builder()
            .netLocal(2)
            .getOrCreate();

    @Test
    public void for100ReduceByKeyTest()
    {
        DataSet<String> ds1 = mppContext.makeDataSet(Arrays.asList(
                "a",
                "a",
                "b",
                "b",
                "b"), 2);
        KvDataSet<String, String> ds2 = ds1.kvDataSet(x -> Tuple2.of(x, x))
                .encoder(Encoders.tuple2(Encoders.asciiString(), Encoders.asciiString()))
                .reduceByKey((x, y) -> x + y, 2);

        for (int i = 0; i < 100; i++) {
            Map<String, String> rs = ds2.collectMap();
            Assert.assertEquals(MutableMap.of("b", "bbb", "a", "aa"), rs);
        }
    }

    @Test
    public void for100JoinTest()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 2);

        for (int i = 0; i < 100; i++) {
            List<Tuple2<String, Tuple2<Integer, Integer>>> data = ageDs.mapKeys(x -> x).join(ageDs).collect();
            Assert.assertEquals(data,
                    Arrays.asList(Tuple2.of("hp", Tuple2.of(18, 18)),
                            Tuple2.of("hp2", Tuple2.of(20, 20)),
                            Tuple2.of("hp1", Tuple2.of(19, 19))));
        }
    }

    @Test
    public void dataSetZipIndexCollectOrderlyTest()
    {
        DataSet<String> links = mppContext.makeDataSet(Arrays.asList(
                "a",
                "b",
                "c",
                "d",
                "e"), 2);

        KvDataSet<String, Long> zipIndex = links.zipWithIndex();
        for (int i = 0; i < 100; i++) {
            List<Long> indexs = zipIndex.values().collect();
            Assert.assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), indexs);
        }
    }

    @Test
    public void unionAllTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 1).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);
        Map<String, Integer> map = MutableMap.of(
                "hp", 20,
                "hp2", 20,
                "hp1", 19);
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(map, out.collectMap());
        }
    }
}
