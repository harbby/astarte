package com.github.harbby.astarte.utils;

import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

public class ShuffleUtil
{
    private ShuffleUtil() {}

    public static <K, V> List<Tuple2<K, V>> readDataFile(String path)
    {
        List<Tuple2<K, V>> out = new ArrayList<>();
        try {
            DataInputStream dataInputStream = new DataInputStream(new FileInputStream(path));
            int length = dataInputStream.readInt();
            while (length != -1) {
                byte[] bytes = new byte[length];
                dataInputStream.read(bytes);
                out.add(Serializables.byteToObject(bytes));
                length = dataInputStream.readInt();
            }
            dataInputStream.close();
            return out;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
