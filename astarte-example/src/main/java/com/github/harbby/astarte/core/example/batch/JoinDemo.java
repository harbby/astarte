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
package com.github.harbby.astarte.core.example.batch;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

public class JoinDemo
{
    private JoinDemo() {}

    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder().local(1).getOrCreate();
        String sparkHome = System.getenv("SPARK_HOME");

        DataSet<String> ds = mppContext.textFile(sparkHome + "/README.md");
        DataSet<String> worlds = ds.flatMap(input -> input.toLowerCase().split(" "))
                .filter(x -> !"".equals(x.trim()));

        KvDataSet<String, Long> kvDataSet = worlds.kvDataSet(x -> new Tuple2<>(x.substring(0, 1), 1L));
        KvDataSet<String, Long> worldCounts = kvDataSet.partitionBy(2).reduceByKey(Long::sum);
        //worldCounts.print();

        KvDataSet<String, Long> worldLengths = worlds.kvDataSet(x -> new Tuple2<>(x.substring(0, 1), (long) x.length()))
                .partitionBy(2).reduceByKey(Long::sum);
        //worldLengths.print();

        KvDataSet<String, Tuple2<Long, Long>> ds2 = worldLengths
                .join(worldCounts);

        // a,(143, 41)
        ds2.foreach(x -> System.out.println(x.f1() + "," + x.f2()));  //job4
    }
}
