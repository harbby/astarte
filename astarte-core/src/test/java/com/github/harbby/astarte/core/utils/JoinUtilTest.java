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

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JoinUtilTest
{
    @Test
    public void sameJoinTest()
    {
        Iterator<Tuple2<String, Integer>> iterator = Iterators.of(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10),
                Tuple2.of("hp1", 19),
                Tuple2.of("hp2", 20));

        Iterator<Tuple2<String, Tuple2<Integer, Integer>>> rs = JoinUtil.sameJoin(iterator);
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = MutableList.copy(rs);
        Assert.assertEquals(Arrays.asList(
                Tuple2.of("hp", Tuple2.of(8, 8)),
                Tuple2.of("hp", Tuple2.of(8, 10)),
                Tuple2.of("hp", Tuple2.of(10, 8)),
                Tuple2.of("hp", Tuple2.of(10, 10)),
                Tuple2.of("hp1", Tuple2.of(19, 19)),
                Tuple2.of("hp2", Tuple2.of(20, 20))),
                data);
    }

    @Test
    public void sameJoinTest2()
    {
        Iterator<Tuple2<String, Integer>> iterator = Iterators.of(
                Tuple2.of("hp", 8),
                Tuple2.of("hp", 10));

        Iterator<Tuple2<String, Tuple2<Integer, Integer>>> rs = JoinUtil.sameJoin(iterator);
        List<Tuple2<String, Tuple2<Integer, Integer>>> data = MutableList.copy(rs);
        Assert.assertEquals(Arrays.asList(
                Tuple2.of("hp", Tuple2.of(8, 8)),
                Tuple2.of("hp", Tuple2.of(8, 10)),
                Tuple2.of("hp", Tuple2.of(10, 8)),
                Tuple2.of("hp", Tuple2.of(10, 10))),
                data);
    }

    @Test
    public void innerMergeJoin()
    {
        Iterator<Tuple2<Integer, String>> left = Iterators.of(
                Tuple2.of(1, "v1_1"),
                Tuple2.of(2, "v1_2_1"),
                Tuple2.of(2, "v1_2_2"),
                Tuple2.of(7, "v1_7"),
                Tuple2.of(8, "v1_8"));
        Iterator<Tuple2<Integer, String>> right = Iterators.of(
                Tuple2.of(2, "v2_2"),
                Tuple2.of(4, "v2_4"),
                Tuple2.of(7, "v2_7_1"),
                Tuple2.of(7, "v2_7_2"),
                Tuple2.of(8, "v2_8"),
                Tuple2.of(9, "v2_9"));
        Iterator<Tuple2<Integer, Tuple2<String, String>>> rs = JoinUtil.mergeJoin(JoinUtil.JoinMode.INNER_JOIN, Integer::compare, left, right);
        List<Tuple2<Integer, Tuple2<String, String>>> data = MutableList.copy(rs);
        Assert.assertEquals(Arrays.asList(
                Tuple2.of(2, Tuple2.of("v1_2_1", "v2_2")),
                Tuple2.of(2, Tuple2.of("v1_2_2", "v2_2")),
                Tuple2.of(7, Tuple2.of("v1_7", "v2_7_1")),
                Tuple2.of(7, Tuple2.of("v1_7", "v2_7_2")),
                Tuple2.of(8, Tuple2.of("v1_8", "v2_8"))
        ), data);
    }

    @Test
    public void innerMergeJoin2()
    {
        Iterator<Tuple2<Integer, String>> left = Iterators.of(
                Tuple2.of(1, "v1_1"),
                Tuple2.of(2, "v1_2_1"),
                Tuple2.of(2, "v1_2_2"),
                Tuple2.of(7, "v1_7"),
                Tuple2.of(8, "v1_8"));
        Iterator<Tuple2<Integer, String>> right = Iterators.of(
                Tuple2.of(2, "v2_2"),
                Tuple2.of(4, "v2_4"),
                Tuple2.of(7, "v2_7_1"),
                Tuple2.of(7, "v2_7_2"),
                Tuple2.of(8, "v2_8"));
        Iterator<Tuple2<Integer, Tuple2<String, String>>> rs = JoinUtil.mergeJoin(JoinUtil.JoinMode.INNER_JOIN, Integer::compare, left, right);
        List<Tuple2<Integer, Tuple2<String, String>>> data = MutableList.copy(rs);
        Assert.assertEquals(Arrays.asList(
                Tuple2.of(2, Tuple2.of("v1_2_1", "v2_2")),
                Tuple2.of(2, Tuple2.of("v1_2_2", "v2_2")),
                Tuple2.of(7, Tuple2.of("v1_7", "v2_7_1")),
                Tuple2.of(7, Tuple2.of("v1_7", "v2_7_2")),
                Tuple2.of(8, Tuple2.of("v1_8", "v2_8"))
        ), data);
    }
}
