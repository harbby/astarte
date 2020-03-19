package com.github.harbby.ashtarte.api;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public final class ShuffleManager {
    public static <K, V> Iterator<Tuple2<K, V>> getReader(int shuffleId, int reduceId) {
        File dataDir = new File("/tmp/shuffle/");
        //todo: 此处为 demo
        Iterator<Iterator<Tuple2<K, V>>> iterator = Stream.of(dataDir.listFiles())
                .filter(x -> x.getName().startsWith("shuffle_" + shuffleId + "_")
                        && x.getName().endsWith("_" + reduceId + ".data"))
                .map(file -> {
                    ArrayList<Tuple2<K, V>> out = new ArrayList<>();
                    try {
                        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file));
                        int length = dataInputStream.readInt();
                        while (length != -1) {
                            byte[] bytes = new byte[length];
                            dataInputStream.read(bytes);
                            out.add(Serializables.byteToObject(bytes));
                            length = dataInputStream.readInt();
                        }
                        dataInputStream.close();
                        return out.iterator();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).iterator();

        return Iterators.concat(iterator);
    }
}
