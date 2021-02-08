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
package com.github.harbby.astarte.core.deprecated;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * 实验性join
 * todo: hash shuffle join存在诸多内存策略问题, 推荐使用sort merge join
 */
public class JoinExperiment
{
    private JoinExperiment() {}

    public enum JoinMode
    {
        LEFT_JOIN,
        RIGHT_JOIN,
        INNER_JOIN,
        FULL_JOIN;
    }

    @SafeVarargs
    public static <K> Iterator<Tuple2<K, Object[]>> join(JoinMode joinMode, Iterator<Tuple2<K, ?>>... iterators)
    {
        if (iterators.length == 2) {
            return broadcastJoin(joinMode, iterators[0], iterators[1]);
        }
        else {
            //todo: sort merge join
            throw new UnsupportedOperationException();
        }
    }

    /**
     * left is small dataSet
     * right is big dataSet
     * right >> left
     */
    public static <K> Iterator<Tuple2<K, Object[]>> broadcastJoin(JoinMode joinMode, Iterator<Tuple2<K, ?>> left, Iterator<Tuple2<K, ?>> right)
    {
        if (joinMode == JoinMode.LEFT_JOIN || joinMode == JoinMode.FULL_JOIN) {
            return broadcastLeftAndFullJoin(joinMode, left, right);
        }
        Map<K, List<Object>> cacheLeft = new HashMap<>();
        while (left.hasNext()) {
            Tuple2<K, ?> row = left.next();
            List<Object> values = cacheLeft.computeIfAbsent(row.f1, key -> new ArrayList<>());
            values.add(row.f2);
        }
        switch (joinMode) {
            case INNER_JOIN:
                return Iterators.flatMap(right, rightRow -> {
                    List<Object> values = cacheLeft.get(rightRow.f1);
                    if (values == null) {
                        return Iterators.empty();
                    }
                    else if (values.size() == 1) {
                        Object[] objects = new Object[] {values.get(0), rightRow.f2};
                        return Iterators.of(Tuple2.of(rightRow.f1, objects));
                    }
                    return values.stream().map(v -> {
                        Object[] objects = new Object[] {v, rightRow.f2};
                        return Tuple2.of(rightRow.f1, objects);
                    }).iterator();
                });
            case RIGHT_JOIN:
                return Iterators.flatMap(right, rightRow -> {
                    List<Object> values = cacheLeft.get(rightRow.f1);
                    if (values == null) {
                        Object[] objects = new Object[] {null, rightRow.f2};
                        return Iterators.of(Tuple2.of(rightRow.f1, objects));
                    }
                    else if (values.size() == 1) {
                        Object[] objects = new Object[] {values.get(0), rightRow.f2};
                        return Iterators.of(Tuple2.of(rightRow.f1, objects));
                    }
                    return values.stream().map(v -> {
                        Object[] objects = new Object[] {v, rightRow.f2};
                        return Tuple2.of(rightRow.f1, objects);
                    }).iterator();
                });
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static <K> Iterator<Tuple2<K, Object[]>> broadcastLeftAndFullJoin(JoinMode joinMode, Iterator<Tuple2<K, ?>> left, Iterator<Tuple2<K, ?>> right)
    {
        checkState(joinMode == JoinMode.LEFT_JOIN || joinMode == JoinMode.FULL_JOIN, "LEFT_JOIN or FULL_JOIN");
        Map<K, Tuple2<List<Object>, Boolean>> cacheLeft = new HashMap<>();
        while (left.hasNext()) {
            Tuple2<K, ?> row = left.next();
            Tuple2<List<Object>, Boolean> values = cacheLeft.computeIfAbsent(row.f1, key -> Tuple2.of(new ArrayList<>(), false));
            values.f1.add(row.f2);
        }
        Iterator<Tuple2<K, Object[]>> innerJoin = Iterators.flatMap(right, rightRow -> {
            Tuple2<List<Object>, Boolean> values = cacheLeft.get(rightRow.f1);
            if (values == null) {
                if (joinMode == JoinMode.LEFT_JOIN) {
                    return Iterators.empty();
                }
                else {
                    Object[] objects = new Object[] {null, rightRow.f2};
                    return Iterators.of(Tuple2.of(rightRow.f1, objects));
                }
            }
            values.f2 = true; //标注为命中
            if (values.f1.size() == 1) {
                Object[] objects = new Object[] {values.f1.get(0), rightRow.f2};
                return Iterators.of(Tuple2.of(rightRow.f1, objects));
            }
            return values.f1.stream().map(v -> {
                Object[] objects = new Object[] {v, rightRow.f2};
                return Tuple2.of(rightRow.f1, objects);
            }).iterator();
        });
        Iterator<Tuple2<K, Object[]>> leftOnly = cacheLeft.entrySet().stream().filter(x -> !x.getValue().f2).map(x -> {
            Object[] objects = new Object[] {x.getValue().f1.get(0), null};
            return Tuple2.of(x.getKey(), objects);
        }).iterator();
        return Iterators.concat(innerJoin, leftOnly);
    }
}
