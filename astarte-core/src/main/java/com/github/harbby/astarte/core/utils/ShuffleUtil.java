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

package com.github.harbby.astarte.core.utils;

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
