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
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class SumAvgAggDemo
{
    private SumAvgAggDemo() {}

    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");
        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Long> kvDataSet = worlds.kvDataSet(x -> Tuple2.of(x, 1L));
        KvDataSet<String, Long> worldCounts = kvDataSet.rePartitionByKey(2).reduceByKey(Long::sum);

        DataSet<Tuple2<String, Double>> worldCounts2 = worldCounts
                .rePartition(4)
                .mapKeys(k -> k.substring(0, 1))
                .avgValues(Double::valueOf, 3);

        worldCounts2.foreach(x -> System.out.println(x.key() + "," + x.value()));  //job4
    }
}
