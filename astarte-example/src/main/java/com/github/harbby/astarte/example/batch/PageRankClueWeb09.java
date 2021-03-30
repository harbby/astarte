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
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;

public class PageRankClueWeb09
{
    private PageRankClueWeb09() {}

    private static final String DATA_PATH = "/data/data/ClueWeb09_WG_50m.graph-txt";

    public static void main(String[] args)
            throws Exception
    {
        BatchContext mppContext = BatchContext.builder().netLocal(2).getOrCreate();
        int iters = 3;  //迭代次数

        DataSet<Tuple2<Integer, int[]>> lines = mppContext.makeDataSet(new String[] {DATA_PATH}).flatMapIterator(FileIteratorReader::new);
        KvDataSet<Integer, int[]> links = KvDataSet.toKvDataSet(lines)
                .encoder(Encoders.tuple2(Encoders.jInt(), Encoders.jIntArray()))
                .rePartitionByKey(2)
                .cache();

        KvDataSet<Integer, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<Integer, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                int[] urls = it.f1();
                double rank = it.f2();

                int size = urls.length;
                return IntStream.of(urls).mapToObj(url -> new Tuple2<>(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs)
                    .encoder(Encoders.tuple2(Encoders.jInt(), Encoders.jDouble()))
                    .reduceByKey(Double::sum).mapValues(x -> 0.15 + 0.85 * x);
        }
        List<Tuple2<Integer, Double>> output = ranks.partitionLimit(10).collect();
        output.forEach(tup -> System.out.printf("%s has rank:  %s .%n", tup.f1(), tup.f2()));

        mppContext.stop();
    }

    private static class FileIteratorReader
            implements Iterator<Tuple2<Integer, int[]>>, Serializable
    {
        private final BufferedReader reader;

        private String line;
        private int number = 0;

        private FileIteratorReader(File file)
        {
            try {
                InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
                this.reader = new BufferedReader(inputStreamReader);
            }
            catch (FileNotFoundException e) {
                throw throwsThrowable(e);
            }
        }

        private FileIteratorReader(String file)
        {
            this(new File(file));
        }

        @Override
        public boolean hasNext()
        {
            if (line != null) {
                return true;
            }
            do {
                try {
                    line = reader.readLine();
                    number++;
                }
                catch (IOException e) {
                    throw throwsThrowable(e);
                }
                if (line == null) {
                    return false;
                }
            }
            while (line.length() == 0);
            return true;
        }

        @Override
        public Tuple2<Integer, int[]> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            checkState(line.length() > 0);
            String[] targets = this.line.trim().split(" ");
            int[] targetIds = new int[targets.length];
            for (int i = 0; i < targets.length; i++) {
                targetIds[i] = Integer.parseInt(targets[i]);
            }
            this.line = null;
            return new Tuple2<>(number, targetIds);
        }
    }
}
