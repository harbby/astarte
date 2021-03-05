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
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.io.BufferedNioOutputStream;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.channels.FileChannel;
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
        BatchContext mppContext = BatchContext.builder().local(2).getOrCreate();
        int iters = 3;  //迭代次数

//        testZeroCopy2();
//        System.exit(0);
//        bioTest1();
//        nioTest2();

        DataSet<Tuple2<Integer, int[]>> lines = mppContext.makeDataSet(() -> new FileIteratorReader(DATA_PATH))
                .partitionLimit(100_0000);
        KvDataSet<Integer, int[]> links = KvDataSet.toKvDataSet(lines)
                .encoder(Encoders.tuple2(Encoders.jInt(), Encoders.jIntArray()))
                .rePartitionByKey(2).mapValues(x -> x, Encoders.jIntArray())
                .cache();

        KvDataSet<Integer, Double> ranks = links.mapValues(v -> 1.0);
        for (int i = 1; i <= iters; i++) {
            DataSet<Tuple2<Integer, Double>> contribs = links.join(ranks).values().flatMapIterator(it -> {
                int[] urls = it.f1();
                double rank = it.f2();

                int size = urls.length;
                return IntStream.of(urls).mapToObj(url -> new Tuple2<>(url, rank / size)).iterator();
            });

            ranks = KvDataSet.toKvDataSet(contribs).reduceByKey((x, y) -> x + y).mapValues(x -> 0.15 + 0.85 * x);
        }
        List<Tuple2<Integer, Double>> output = ranks.partitionLimit(10).collect();
        output.forEach(tup -> System.out.println(String.format("%s has rank:  %s .", tup.f1(), tup.f2())));

        mppContext.stop();
    }

    /**
     * 123135ms
     */
    private static void bioTest1()
            throws Exception
    {
        File file = new File(DATA_PATH);
        long start = System.currentTimeMillis();
        Encoder<Tuple2<Integer, int[]>> encoder = Encoders.tuple2(Encoders.jInt(), Encoders.jIntArray());
        try (InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
                BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream("/tmp/shuffle_test"))) {
            DataOutput dataOutput = new DataOutputStream(fileOutputStream);
            BufferedReader reader = new BufferedReader(inputStreamReader);

            String line = null;
            int number = 0;
            int[] zeroIntArr = new int[0];
            while ((line = reader.readLine()) != null) {
                Tuple2<Integer, int[]> integerTuple2;
                if (line.length() == 0) {
                    integerTuple2 = Tuple2.of(number, zeroIntArr);
                }
                else {
                    String[] targets = line.trim().split(" ");
                    int[] targetIds = new int[targets.length];
                    for (int i = 0; i < targets.length; i++) {
                        targetIds[i] = Integer.parseInt(targets[i]);
                    }
                    integerTuple2 = new Tuple2<>(number, targetIds);
                }
                encoder.encoder(integerTuple2, dataOutput);
                number++;
            }
        }
        System.out.println("bioTest1 : " + (System.currentTimeMillis() - start) + "ms");
    }

    private static void nioTest2()
            throws Exception
    {
        File file = new File(DATA_PATH);
        long start = System.currentTimeMillis();
        Encoder<Tuple2<Integer, int[]>> encoder = Encoders.tuple2(Encoders.jInt(), Encoders.jIntArray());
        try (FileOutputStream fileOutputStream = new FileOutputStream("/tmp/shuffle_test")) {
            FileChannel fileChannel = fileOutputStream.getChannel();
            BufferedNioOutputStream nioOutputStream = new BufferedNioOutputStream(fileChannel, 819200);
            DataOutput dataOutput = new DataOutputStream(nioOutputStream);
            for (Tuple2<Integer, int[]> integerTuple2 : (Iterable<Tuple2<Integer, int[]>>) () -> new FileIteratorReader(file)) {
                dataOutput.writeInt(integerTuple2.f1);
                int[] values = integerTuple2.f2;
                dataOutput.writeInt(values.length);
                for (int v : values) {
                    dataOutput.writeInt(v);
                }
                encoder.encoder(integerTuple2, dataOutput);
                if (nioOutputStream.position() > 81920) {
                    nioOutputStream.flush();
                }
            }
        }
        System.out.println("nioTest2 : " + (System.currentTimeMillis() - start) + "ms");
    }

    private static void testZeroCopy2()
            throws Exception
    {
        File file = new File(DATA_PATH);
        long start = System.currentTimeMillis();
        FileOutputStream outputStream = new FileOutputStream("/tmp/shuffle_test");
        int i = 5;
        while (i-- > 0) {
            try (FileInputStream inputStream = new FileInputStream(file)) {
                inputStream.getChannel().transferTo(0, file.length(), outputStream.getChannel());
            }
            try (FileInputStream inputStream = new FileInputStream(file)) {
                inputStream.getChannel().transferTo(0, file.length(), outputStream.getChannel());
            }
        }
        System.out.println("testZeroCopy2 : " + (System.currentTimeMillis() - start) + "ms");
    }

    private static class FileIteratorReader
            implements Iterator<Tuple2<Integer, int[]>>, Serializable
    {
        private final File file;
        private final BufferedReader reader;

        private String line;
        private int number = 0;

        private FileIteratorReader(File file)
        {
            this.file = file;
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
