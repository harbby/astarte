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
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.gadtry.collection.MutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OperatorTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
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

    @Test(expected = AstarteException.class)
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

    @Test
    public void limitTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum).limit(2);
        Assert.assertEquals(ds1.collectMap(), MutableMap.of("hp", 18, "hp2", 20));
    }

    @Test
    public void groupByReduce()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 21),
                Tuple2.of("hp", 18)
        ), 3).reduceByKey(Integer::sum, 3);
        Assert.assertEquals(ds1.collectMap(), MutableMap.of(
                "hp", 20,
                "hp1", 19,
                "hp2", 21));
    }
}
