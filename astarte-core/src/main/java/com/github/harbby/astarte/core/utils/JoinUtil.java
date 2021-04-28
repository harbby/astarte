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

import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.CalcOperator;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.iterator.MarkIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * 实验性join
 * todo: hash shuffle join存在诸多内存策略问题, 推荐使用sort merge join
 */
public class JoinUtil
{
    private JoinUtil() {}

    public enum JoinMode
    {
        LEFT_JOIN,
        RIGHT_JOIN,
        INNER_JOIN,
        FULL_JOIN;
    }

    /**
     * left is small dataSet
     * right is big dataSet
     * right >> left
     */
    public static <K, V1, V2> Iterator<Tuple2<K, Tuple2<V1, V2>>> join(
            JoinMode joinMode,
            Iterator<Tuple2<K, V1>> left,
            Iterator<Tuple2<K, V2>> right)
    {
        if (joinMode == JoinMode.LEFT_JOIN || joinMode == JoinMode.FULL_JOIN) {
            return leftAndFullJoin(joinMode, left, right);
        }
        Map<K, List<V1>> cacheLeft = new HashMap<>();
        while (left.hasNext()) {
            Tuple2<K, V1> row = left.next();
            List<V1> values = cacheLeft.computeIfAbsent(row.key(), key -> new ArrayList<>());
            values.add(row.value());
        }
        switch (joinMode) {
            case INNER_JOIN:
                return Iterators.flatMap(right, rightRow -> {
                    List<V1> values = cacheLeft.get(rightRow.key());
                    if (values == null) {
                        return Iterators.empty();
                    }
                    else if (values.size() == 1) {
                        return Iterators.of(Tuple2.of(rightRow.key(), Tuple2.of(values.get(0), rightRow.value())));
                    }
                    return values.stream().map(v -> {
                        return Tuple2.of(rightRow.key(), Tuple2.of(v, rightRow.value()));
                    }).iterator();
                });
            case RIGHT_JOIN:
                return Iterators.flatMap(right, rightRow -> {
                    List<V1> values = cacheLeft.get(rightRow.key());
                    if (values == null) {
                        return Iterators.of(Tuple2.of(rightRow.key(), Tuple2.of(null, rightRow.value())));
                    }
                    else if (values.size() == 1) {
                        return Iterators.of(Tuple2.of(rightRow.key(), Tuple2.of(values.get(0), rightRow.value())));
                    }
                    return values.stream().map(v -> {
                        return Tuple2.of(rightRow.key(), Tuple2.of(v, rightRow.value()));
                    }).iterator();
                });
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static <K, V1, V2> Iterator<Tuple2<K, Tuple2<V1, V2>>> leftAndFullJoin(
            JoinMode joinMode,
            Iterator<Tuple2<K, V1>> left,
            Iterator<Tuple2<K, V2>> right)
    {
        checkState(joinMode == JoinMode.LEFT_JOIN || joinMode == JoinMode.FULL_JOIN, "LEFT_JOIN or FULL_JOIN");
        Map<K, Tuple2<List<V1>, Boolean>> cacheLeft = new HashMap<>();
        while (left.hasNext()) {
            Tuple2<K, V1> row = left.next();
            Tuple2<List<V1>, Boolean> values = cacheLeft.computeIfAbsent(row.key(), key -> Tuple2.of(new ArrayList<>(), false));
            values.key().add(row.value());
        }
        Iterator<Tuple2<K, Tuple2<V1, V2>>> innerJoin = Iterators.flatMap(right, rightRow -> {
            Tuple2<List<V1>, Boolean> values = cacheLeft.get(rightRow.key());
            if (values == null) {
                if (joinMode == JoinMode.LEFT_JOIN) {
                    return Iterators.empty();
                }
                else {
                    return Iterators.of(Tuple2.of(rightRow.key(), Tuple2.of(null, rightRow.value())));
                }
            }
            values.setValue(true); //标注为命中
            if (values.key().size() == 1) {
                return Iterators.of(Tuple2.of(rightRow.key(), Tuple2.of(values.key().get(0), rightRow.value())));
            }
            return values.key().stream().map(v -> {
                return Tuple2.of(rightRow.key(), Tuple2.of(v, rightRow.value()));
            }).iterator();
        });
        Iterator<Tuple2<K, Tuple2<V1, V2>>> leftOnly = cacheLeft.entrySet().stream()
                .filter(x -> !x.getValue().value()).map(x -> {
                    return Tuple2.of(x.getKey(), Tuple2.of(x.getValue().key().get(0), (V2) null));
                }).iterator();
        return Iterators.concat(innerJoin, leftOnly);
    }

    public static <K, V1, V2> Iterator<Tuple2<K, Tuple2<V1, V2>>> mergeJoin(
            JoinMode joinMode,
            Comparator<K> comparator,
            Iterator<Tuple2<K, V1>> leftStream,
            Iterator<Tuple2<K, V2>> rightStream)
    {
        switch (joinMode) {
            case INNER_JOIN: {
                return AggUtil.mergeJoin(comparator, leftStream, rightStream);
            }
            default:
                return join(joinMode, leftStream, rightStream);
            //throw new UnsupportedOperationException();
        }
    }

    public static <K, V1, V2> Iterator<Tuple2<K, Tuple2<V1, V2>>> sameJoin(
            Iterator<? extends Tuple2<K, ?>> iterator)
    {
        return sameJoin(iterator,
                (Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V1>>>) it -> {
                    return CalcOperator.doCodeGen(it, Collections.emptyList());
                },
                (Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V2>>>) it -> {
                    return CalcOperator.doCodeGen(it, Collections.emptyList());
                });
    }

    public static <E> Iterators.MarkFixLenIterator<E> wrap(List<E> values)
    {
        requireNonNull(values, "values is null");

        return new Iterators.MarkFixLenIterator<E>()
        {
            private int index = 0;

            @Override
            public int length()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext()
            {
                return index < values.size();
            }

            @Override
            public E next()
            {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
                return values.get(index++);
            }

            @Override
            public void mark()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void reset()
            {
                this.index = 0;
            }
        };
    }

    public static <K, V1, V2> Iterator<Tuple2<K, Tuple2<V1, V2>>> sameJoin(
            Iterator<? extends Tuple2<K, ?>> iterator,
            Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V1>>> leftMapOperator,
            Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V2>>> rightMapOperator)
    {
        return new Iterator<Tuple2<K, Tuple2<V1, V2>>>()
        {
            private final List<Tuple2<K, ?>> sameKeyRows = new ArrayList<>();
            private Iterator<Tuple2<K, Tuple2<V1, V2>>> child = Iterators.empty();
            private final MarkIterator<Tuple2<K, ?>> leftIterator = wrap(sameKeyRows);
            private final MarkIterator<Tuple2<K, ?>> rightIterator = wrap(sameKeyRows);
            private Tuple2<K, ?> next;

            @Override
            public boolean hasNext()
            {
                if (child.hasNext()) {
                    return true;
                }
                if (next != null) {
                    sameKeyRows.clear();
                    sameKeyRows.add(next);
                    leftIterator.reset();
                    rightIterator.reset();
                }
                while (iterator.hasNext()) {
                    Tuple2<K, ?> row = iterator.next();
                    if (sameKeyRows.isEmpty() || Objects.equals(row.key(), sameKeyRows.get(0).key())) {
                        sameKeyRows.add(row);
                        continue;
                    }
                    this.next = row;
                    this.child = this.propreChild();
                    if (child.hasNext()) {
                        return true;
                    }
                }
                if (!sameKeyRows.isEmpty()) {
                    //next key
                    this.next = null;
                    this.child = propreChild();
                    return child.hasNext();
                }
                return false;
            }

            private Iterator<Tuple2<K, Tuple2<V1, V2>>> propreChild()
            {
                Iterator<Tuple2<K, V1>> left = leftMapOperator.map(leftIterator);
                Iterator<Tuple2<K, V2>> right = rightMapOperator.map(rightIterator);
                //笛卡尔积,如果是多个dataset同时Join,则唯一变化时这里变成多个笛卡尔积
                return Iterators.flatMap(left,
                        x -> {
                            rightIterator.reset();
                            return Iterators.map(right, y -> Tuple2.of(x.key(), Tuple2.of(x.value(), y.value())));
                        });
            }

            @Override
            public Tuple2<K, Tuple2<V1, V2>> next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return child.next();
            }
        };
    }
}
