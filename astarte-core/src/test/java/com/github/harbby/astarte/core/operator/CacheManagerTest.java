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
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

public class CacheManagerTest
{
    private final BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();

    @Test
    public void cacheUnCacheTest()
            throws NoSuchFieldException, IllegalAccessException
    {
        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).mapValues(x -> x)
                .reduceByKey(Integer::sum)
                .kvDataSet(x -> x);
        ds1.cache();
        int opId = Operator.unboxing((Operator) ds1).getId();

        Field field = CacheManager.class.getDeclaredField("cacheMemMap");
        field.setAccessible(true);

        ds1.print();
        Assert.assertTrue(((Map<?, ?>) field.get(null)).containsKey(opId));
        ds1.unCache();
        Assert.assertFalse(((Map<?, ?>) field.get(null)).containsKey(opId));
    }

    @Test
    public void cacheLoopTest()
            throws NoSuchFieldException, IllegalAccessException
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
}
