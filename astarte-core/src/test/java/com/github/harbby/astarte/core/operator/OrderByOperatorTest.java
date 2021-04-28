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
import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.IteratorPlus;
import com.github.harbby.gadtry.collection.MutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

public class OrderByOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
            .getOrCreate();

    @Test
    public void orderByUseDataSourceTest1()
    {
        KvDataSet<String, Integer> ds = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp2", 20)
        ), 2);
        Assert.assertEquals(ds.sortByValue((x, y) -> y.compareTo(x)).collect(),
                ImmutableList.of(
                        Tuple2.of("hp2", 20),
                        Tuple2.of("hp1", 19),
                        Tuple2.of("hp", 10),
                        Tuple2.of("hp", 8)));
    }

    @Test
    public void orderByTest1()
    {
        KvDataSet<String, Integer> ds = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp1", 19),
                Tuple2.of("hp", 18),
                Tuple2.of("hp2", 20)
        ), 1);
        Assert.assertEquals(ds.sortByValue((x, y) -> y.compareTo(x)).collect(),
                ImmutableList.of(
                        Tuple2.of("hp2", 20),
                        Tuple2.of("hp1", 19),
                        Tuple2.of("hp", 18)));
    }

    private static final IteratorPlus<Tuple2<String, Integer>> it = new IteratorPlus<Tuple2<String, Integer>>()
    {
        private final Random random = new Random(1);
        private int i = 0;

        @Override
        public boolean hasNext()
        {
            return i < 50;
        }

        @Override
        public Tuple2<String, Integer> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return Tuple2.of("a" + random.nextInt(5), i++ % 5);
        }
    };

    @Test
    public void sortMergeGroupBySumTest()
    {
        KvDataSet<String, Integer> ds = mppContext.makeDataSet(new String[] {"1"})
                .flatMapIterator(x -> it)
                .kvDataSet(x -> x)
                .reduceByKey(Integer::sum, 3);
        Map<String, Integer> result = ds.collectMap();
        Assert.assertEquals(result, MutableMap.of(
                "a0", 14,
                "a1", 8,
                "a2", 32,
                "a3", 24,
                "a4", 22));
    }
}
