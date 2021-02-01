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
package com.github.harbby.astarte.example.batch;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;

public class UnionAllDemo
{
    private UnionAllDemo() {}

    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().getOrCreate();

        KvDataSet<String, Integer> ds1 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10)
        )).reduceByKey(Integer::sum);

        KvDataSet<String, Integer> ds2 = mppContext.makeKvDataSet(Arrays.asList(
                Tuple2.of("hp", 2),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20)
        ), 2).reduceByKey(Integer::sum);
        //.distinct();

        KvDataSet<String, Integer> out = ds1.unionAll(ds2).reduceByKey(Integer::sum);

        out.collect().forEach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
