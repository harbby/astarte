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

import java.util.Collection;
import java.util.List;

public class PageRank
{
    private PageRank() {}

    public static void main(String[] args)
    {
        BatchContext mppContext = BatchContext.builder()
                .local(2)
                .getOrCreate();
        int iters = 4;  //迭代次数
        String sparkHome = System.getenv("SPARK_HOME");

        DataSet<String> lines = mppContext.textFile(sparkHome + "/data/mllib/pagerank_data.txt");
        //KvDataSet<String,? extends Iterable<String>>
        KvDataSet<String, Iterable<String>> links = lines.kvDataSet(s -> {
            String[] parts = s.split("\\s+");
            return new Tuple2<>(parts[0], parts[1]);
        }).cache().union(mppContext.makeEmptyDataSet()).mapValues(x -> x).groupByKey().cache();

//        links = mppContext.makeKvDataSet(Arrays.asList(
//                new Tuple2<>("1", Arrays.asList("2", "3", "4")),
//                new Tuple2<>("2", Arrays.asList("1")),
//                new Tuple2<>("3", Arrays.asList("1")),
//                new Tuple2<>("4", Arrays.asList("1"))
//        ));

        //links.join(links.mapValues(v -> 1.0)).mapValues(v->1.0).join(links).print();
        //links.print();

        //System.exit(0);

        KvDataSet<String, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<String, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                Collection<String> urls = (Collection<String>) it.f1();
                Double rank = it.f2();

                long size = urls.size();
                return urls.stream().map(url -> new Tuple2<>(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
        }

        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.f1(), tup.f2())));
    }
}
