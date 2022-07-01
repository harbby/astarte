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
package com.github.harbby.astarte.core.example;

import com.github.harbby.astarte.core.BatchContext;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * pageRank 由google创始人 拉里·佩奇（Larry Page）发明.
 * <p>
 * 该算法为迭代型,且结果收敛
 */
public class PageRankTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .local(2)
            .getOrCreate();

    @Test
    public void pageRank150ItersTest()
    {
        int iters = 150;  //迭代次数

        DataSet<String> lines = mppContext.textFile("../data/batch/pagerank_data.txt");
        KvDataSet<String, List<String>> links = lines.kvDataSet(s -> {
            String[] parts = s.split("\\s+");
            return Tuple2.of(parts[0], parts[1]);
        }).distinct(2).groupByKey()
                .mapValues(MutableList::copy)
                .cache();

        KvDataSet<String, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<String, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                List<String> urls = it.key();
                Double rank = it.value();

                long size = urls.size();
                return urls.stream().map(url -> Tuple2.of(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
        }

        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.key(), tup.value())));

        Map<String, Double> data = output.stream().collect(Collectors.toMap(k -> k.key(), v -> v.value()));
        Assert.assertEquals(data.get("1"), 1.918918918918918D, 1e-7);
        Assert.assertEquals(data.get("2"), 0.6936936936936938, 1e-7);
        Assert.assertEquals(data.get("3"), 0.6936936936936938, 1e-7);
        Assert.assertEquals(data.get("4"), 0.6936936936936938, 1e-7);
    }

    @Test
    public void pageRankUseNumberEncoderTest()
    {
        int iters = 150;  //迭代次数

        DataSet<String> lines = mppContext.textFile("../data/batch/pagerank_data.txt");
        KvDataSet<Integer, int[]> links = lines.kvDataSet(s -> {
            String[] parts = s.split("\\s+");
            return Tuple2.of(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }).distinct(2).groupByKey()
                .mapValues(x -> Iterators.toStream(x).mapToInt(y -> y).toArray())
                .encoder(Encoders.tuple2(Encoders.jInt(), Encoders.jIntArray()))
                .cache();
        KvDataSet<Integer, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<Integer, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                int[] urls = it.key();
                Double rank = it.value();

                long size = urls.length;
                return IntStream.of(urls).mapToObj(url -> Tuple2.of(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
        }

        List<Tuple2<Integer, Double>> output = ranks.collect();
        output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.key(), tup.value())));

        Map<Integer, Double> data = output.stream().collect(Collectors.toMap(k -> k.key(), v -> v.value()));
        Assert.assertEquals(data.get(1), 1.918918918918918D, 1e-7);
        Assert.assertEquals(data.get(2), 0.6936936936936938, 1e-7);
        Assert.assertEquals(data.get(3), 0.6936936936936938, 1e-7);
        Assert.assertEquals(data.get(4), 0.6936936936936938, 1e-7);
    }
}
