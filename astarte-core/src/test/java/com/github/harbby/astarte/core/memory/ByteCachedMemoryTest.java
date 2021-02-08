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
package com.github.harbby.astarte.core.memory;

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ByteCachedMemoryTest
{
    @Test
    public void coreFeaturesTest()
    {
        Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple2(Encoders.jInt(), Encoders.jInt());
        ByteCachedMemory<Tuple2<Integer, Integer>> cachedMemory = new ByteCachedMemory<>(encoder);
        cachedMemory.append(Tuple2.of(1, 1));
        cachedMemory.append(Tuple2.of(2, 2));
        cachedMemory.append(Tuple2.of(3, 3));
        cachedMemory.finalCache();
        Iterator<Tuple2<Integer, Integer>> iterator = cachedMemory.prepareIterator();
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(iterator.next(), Tuple2.of(1, 1));
        Assert.assertEquals(iterator.next(), Tuple2.of(2, 2));
        Assert.assertEquals(iterator.next(), Tuple2.of(3, 3));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFinalCacheCheck()
    {
        Encoder<Tuple2<Integer, long[]>> encoder = Encoders.tuple2(Encoders.jInt(), Encoders.jLongArray());
        ByteCachedMemory<Tuple2<Integer, long[]>> cachedMemory = new ByteCachedMemory<>(encoder);
        cachedMemory.prepareIterator();
    }

    @Test(expected = NoSuchElementException.class)
    public void eachCacheEmpty()
    {
        Encoder<Tuple2<Integer, Long>> encoder = Encoders.tuple2(Encoders.jInt(), Encoders.jLong());
        ByteCachedMemory<Tuple2<Integer, Long>> cachedMemory = new ByteCachedMemory<>(encoder);
        cachedMemory.append(Tuple2.of(1, 1L));
        cachedMemory.finalCache();
        Iterator<Tuple2<Integer, Long>> iterator = cachedMemory.prepareIterator();
        while (iterator.hasNext()) {
            Assert.assertEquals(iterator.next(), Tuple2.of(1, 1L));
        }
        iterator.next();
    }

    @Test
    public void doubleForeachCache()
    {
        Encoder<Tuple2<Integer, long[]>> encoder = Encoders.tuple2(Encoders.jInt(), Encoders.jLongArray());
        ByteCachedMemory<Tuple2<Integer, long[]>> cachedMemory = new ByteCachedMemory<>(encoder);
        cachedMemory.append(Tuple2.of(1, new long[] {1, 1}));
        cachedMemory.finalCache();
        // foreach 1
        Iterator<Tuple2<Integer, long[]>> iterator = cachedMemory.prepareIterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        // foreach 2
        iterator = cachedMemory.prepareIterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}
