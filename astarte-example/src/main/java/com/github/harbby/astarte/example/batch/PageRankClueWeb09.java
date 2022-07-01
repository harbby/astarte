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
import net.jpountz.lz4.LZ4FrameInputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.Throwables.throwThrowable;

public class PageRankClueWeb09
{
    private PageRankClueWeb09() {}

    private static final String DATA_PATH = "/data/linux/data/page_rank/ideal_ClueWeb09_WG_50m.graph-txt.lz4";

    public static void main(String[] args)
            throws Exception
    {
        BatchContext mppContext = BatchContext.builder().netLocal(2).getOrCreate();
        int iters = 3;  //迭代次数

        DataSet<Tuple2<Integer, int[]>> lines = mppContext.makeDataSet(new String[] {DATA_PATH})
                .flatMapIterator(FileIteratorReader::new);
        //.partitionLimit(100);
        KvDataSet<Integer, int[]> links = KvDataSet.toKvDataSet(lines)
                .encoder(Encoders.tuple2(Encoders.jInt(), Encoders.jIntArray()))
                .rePartitionByKey(2)
                .cache();

        KvDataSet<Integer, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<Integer, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                int[] urls = it.key();
                double rank = it.value();

                int size = urls.length;
                return IntStream.of(urls).mapToObj(url -> Tuple2.of(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs)
                    .encoder(Encoders.tuple2(Encoders.jInt(), Encoders.jDouble()))
                    .reduceByKey(Double::sum).mapValues(x -> 0.15 + 0.85 * x);
        }
        List<Tuple2<Integer, Double>> output = ranks.partitionLimit(10).collect();
        output.forEach(tup -> System.out.printf("%s has rank:  %s .%n", tup.key(), tup.value()));

        mppContext.stop();
    }

    private static class FileIteratorReader
            implements Iterator<Tuple2<Integer, int[]>>, Serializable
    {
        private final DataInputStream reader;
        private int number = 0;
        private short nextLength = -1;

        private FileIteratorReader(String file)
        {
            try {
                this.reader = new DataInputStream(new LZ4FrameInputStream(new BufferedInputStream(new FileInputStream(file))));
            }
            catch (IOException e) {
                throw throwThrowable(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            if (nextLength != -1) {
                return true;
            }
            try {
                do {
                    this.nextLength = reader.readShort();
                    number++;
                }
                while (this.nextLength == 0);
                return true;
            }
            catch (EOFException e) {
                return false;
            }
            catch (IOException e) {
                throw throwThrowable(e);
            }
        }

        @Override
        public Tuple2<Integer, int[]> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                int[] targets = new int[nextLength];
                for (short i = 0; i < nextLength; i++) {
                    targets[i] = reader.readInt();
                }
                nextLength = -1;
                return Tuple2.of(number, targets);
            }
            catch (IOException e) {
                throw throwThrowable(e);
            }
        }
    }
}
