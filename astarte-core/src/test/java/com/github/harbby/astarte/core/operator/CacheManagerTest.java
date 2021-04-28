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
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.astarte.core.coders.Encoders;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CacheManagerTest
{
    private final BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();

    @Test
    public void cacheUnCacheTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).mapValues(x -> x)
                .reduceByKey(Integer::sum)
                .kvDataSet(x -> x);
        ds1.cache();
        int opId = Operator.unboxing((Operator<?>) ds1).getId();
        ds1.print();
        Assert.assertTrue(CacheManager.cacheDone(opId));
        ds1.unCache();
        Assert.assertFalse(CacheManager.cacheDone(opId));
    }

    @Test
    public void cacheLoopTest()
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19)
        )).encoder(Encoders.javaEncoder()).reduceByKey(Integer::sum).cache();
        //local join loop dep cacheDataSet.  see: pagerank
        ds1.join(ds1.mapValues(x -> x)).collect().forEach(x -> System.out.println(x));
        ds1.unCache();
    }

    @Test
    public void cacheTest()
    {
        DataSet<String> dataSet = mppContext.makeDataSet(Arrays.asList("1", "2", "3"), 4);
        DataSet<String> rdd = dataSet.cache();
        List<String> r1 = rdd.collect();
        Assert.assertEquals(r1, Arrays.asList("1", "2", "3"));
        Assert.assertTrue(CacheManager.cacheDone(dataSet.getId()));
        dataSet.unCache();
        Assert.assertFalse(CacheManager.cacheDone(dataSet.getId()));
    }

    @Test
    public void cacheTest1()
    {
        DataSet<String> lines = mppContext.makeDataSet(new String[] {"1", "2", "3"});
        KvDataSet<String, Integer> links = lines.kvDataSet(x -> Tuple2.of(x, x.length()))
                .encoder(Encoders.tuple2(Encoders.asciiString(), Encoders.jInt()))
                .rePartitionByKey(2)
                .cache();
        links.foreach(line -> System.out.println(line));
    }

    @Test
    public void cacheTest2()
    {
        KvDataSet<String, Integer> ageDs = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 2);

        KvDataSet<String, Integer> ageDs2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 18),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 1);
        ageDs.join(ageDs2).cache()
                .foreach(line -> System.out.println(line));
    }
}
