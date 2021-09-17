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
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.IteratorPlus;
import com.github.harbby.gadtry.collection.iterator.MarkIterator;
import com.github.harbby.gadtry.collection.iterator.PeekIterator;
import com.github.harbby.gadtry.collection.tuple.Tuple1;
import com.github.harbby.gadtry.function.Function2;
import com.github.harbby.gadtry.function.Reducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;

import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class ReduceUtil
{
    private ReduceUtil() {}

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
                return ReduceUtil.mergeJoin(comparator, leftStream, rightStream);
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
                (Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V1>>>) ReduceUtil::castIterator,
                (Mapper<Iterator<Tuple2<K, ?>>, Iterator<Tuple2<K, V2>>>) ReduceUtil::castIterator);
    }

    @SuppressWarnings("unchecked")
    private static <O> Iterator<O> castIterator(Iterator<?> iterator)
    {
        return (Iterator<O>) iterator;
    }

    public static <E> MarkIterator<E> wrap(List<E> values)
    {
        requireNonNull(values, "values is null");

        return new MarkIterator<E>()
        {
            private int index = 0;

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

    public static <T> Iterator<T> mergeSorted(java.util.Comparator<T> comparator, List<Iterator<T>> inputs)
    {
        requireNonNull(comparator, "comparator is null");
        requireNonNull(inputs, "inputs is null");
        if (inputs.size() == 0) {
            return Iterators.empty();
        }
        if (inputs.size() == 1) {
            return inputs.get(0);
        }
        final PriorityQueue<Tuple2<T, Iterator<T>>> priorityQueue = new PriorityQueue<>(inputs.size(), (o1, o2) -> comparator.compare(o1.key(), o2.key()));
        for (Iterator<T> iterator : inputs) {
            if (iterator.hasNext()) {
                priorityQueue.add(Tuple2.of(iterator.next(), iterator));
            }
        }

        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return !priorityQueue.isEmpty();
            }

            @Override
            public T next()
            {
                Tuple2<T, Iterator<T>> node = priorityQueue.poll();
                if (node == null) {
                    throw new NoSuchElementException();
                }
                T value = node.key();
                if (node.value().hasNext()) {
                    node.setKey(node.value().next());
                    priorityQueue.add(node);
                }
                return value;
            }
        };
    }

    @SafeVarargs
    public static <T> Iterator<T> mergeSorted(java.util.Comparator<T> comparator, Iterator<T>... inputs)
    {
        return mergeSorted(comparator, ImmutableList.copy(inputs));
    }

    public static <K, V> IteratorPlus<Tuple2<K, V>> reduceSorted(Iterator<? extends Tuple2<K, V>> input, Reducer<V> reducer)
    {
        requireNonNull(reducer, "reducer is null");
        requireNonNull(input, "input iterator is null");
        if (!input.hasNext()) {
            return Iterators.empty();
        }
        return new IteratorPlus<Tuple2<K, V>>()
        {
            private Tuple2<K, V> lastRow = input.next();

            @Override
            public boolean hasNext()
            {
                return input.hasNext() || lastRow != null;
            }

            @Override
            public Tuple2<K, V> next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                while (input.hasNext()) {
                    Tuple2<K, V> tp = input.next();
                    if (!Objects.equals(tp.key(), lastRow.key())) {
                        Tuple2<K, V> result = lastRow;
                        this.lastRow = tp;
                        return result;
                    }
                    lastRow.setValue(reducer.reduce(lastRow.value(), tp.value()));
                }
                Tuple2<K, V> result = lastRow;
                lastRow = null;
                return result;
            }
        };
    }

    private static class MergeJoinIteratorByLeftPrimaryKey<K, V1, V2>
            implements IteratorPlus<Tuple2<K, Tuple2<V1, V2>>>
    {
        private final java.util.Comparator<K> comparator;
        private final Iterator<Tuple2<K, V1>> leftIterator;
        private final Iterator<Tuple2<K, V2>> rightIterator;

        private Tuple2<K, V1> leftNode;
        private Tuple2<K, V2> rightNode = null;

        private MergeJoinIteratorByLeftPrimaryKey(java.util.Comparator<K> comparator, Iterator<Tuple2<K, V1>> leftIterator, Iterator<Tuple2<K, V2>> rightIterator)
        {
            this.comparator = comparator;
            this.leftIterator = leftIterator;
            this.rightIterator = rightIterator;
            checkArgument(leftIterator.hasNext());
            leftNode = leftIterator.next();
        }

        @Override
        public boolean hasNext()
        {
            if (rightNode != null) {
                return true;
            }
            if (!rightIterator.hasNext()) {
                return false;
            }
            this.rightNode = rightIterator.next();
            while (true) {
                int than = comparator.compare(leftNode.key(), rightNode.key());
                if (than == 0) {
                    return true;
                }
                else if (than > 0) {
                    if (!rightIterator.hasNext()) {
                        return false;
                    }
                    this.rightNode = rightIterator.next();
                }
                else {
                    if (!leftIterator.hasNext()) {
                        return false;
                    }
                    this.leftNode = leftIterator.next();
                }
            }
        }

        @Override
        public Tuple2<K, Tuple2<V1, V2>> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple2<K, Tuple2<V1, V2>> out = Tuple2.of(leftNode.key(), Tuple2.of(leftNode.value(), rightNode.value()));
            rightNode = null;
            return out;
        }
    }

    private static class MergeJoinIterator<K, V1, V2>
            implements IteratorPlus<Tuple2<K, Tuple2<V1, V2>>>
    {
        private final java.util.Comparator<K> comparator;
        private final Iterator<Tuple2<K, V1>> leftIterator;
        private final Iterator<Tuple2<K, V2>> rightIterator;

        private final List<Tuple2<K, V1>> leftSameKeys = new ArrayList<>();
        private Tuple2<K, V1> leftNode;
        private Tuple2<K, V2> rightNode = null;
        private int index = 0;

        private MergeJoinIterator(java.util.Comparator<K> comparator, Iterator<Tuple2<K, V1>> leftIterator, Iterator<Tuple2<K, V2>> rightIterator)
        {
            this.comparator = comparator;
            this.leftIterator = leftIterator;
            this.rightIterator = rightIterator;

            leftNode = leftIterator.next();
        }

        @Override
        public boolean hasNext()
        {
            if (index < leftSameKeys.size()) {
                return true;
            }
            if (!rightIterator.hasNext()) {
                return false;
            }
            this.rightNode = rightIterator.next();

            if (!leftSameKeys.isEmpty() && Objects.equals(leftSameKeys.get(0).key(), rightNode.key())) {
                index = 0;
                return true;
            }
            while (true) {
                int than = comparator.compare(leftNode.key(), rightNode.key());
                if (than == 0) {
                    leftSameKeys.clear();
                    do {
                        leftSameKeys.add(leftNode);
                        if (leftIterator.hasNext()) {
                            leftNode = leftIterator.next();
                        }
                        else {
                            break;
                        }
                    }
                    while (Objects.equals(leftNode.key(), rightNode.key()));
                    index = 0;
                    return true;
                }
                else if (than > 0) {
                    if (!rightIterator.hasNext()) {
                        return false;
                    }
                    this.rightNode = rightIterator.next();
                }
                else {
                    if (!leftIterator.hasNext()) {
                        return false;
                    }
                    this.leftNode = leftIterator.next();
                }
            }
        }

        @Override
        public Tuple2<K, Tuple2<V1, V2>> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple2<K, V1> x = leftSameKeys.get(index++);
            return Tuple2.of(x.key(), Tuple2.of(x.value(), rightNode.value()));
        }
    }

    public static <K, V1, V2> IteratorPlus<Tuple2<K, Tuple2<V1, V2>>> mergeJoin(
            java.util.Comparator<K> comparator,
            Iterator<Tuple2<K, V1>> leftIterator,
            Iterator<Tuple2<K, V2>> rightIterator)
    {
        requireNonNull(comparator, "comparator is null");
        requireNonNull(leftIterator, "leftIterator is null");
        requireNonNull(rightIterator, "rightIterator is null");
        if (!leftIterator.hasNext() || !rightIterator.hasNext()) {
            return Iterators.empty();
        }
        return new MergeJoinIterator<>(comparator, leftIterator, rightIterator);
    }

    public static <K, V, O> IteratorPlus<Tuple2<K, O>> mapGroupSorted(Iterator<Tuple2<K, V>> input, Function2<K, Iterator<V>, O> mapGroupFunc)
    {
        requireNonNull(input, "input Iterator is null");
        requireNonNull(mapGroupFunc, "mapGroupFunc is null");
        if (!input.hasNext()) {
            return Iterators.empty();
        }
        PeekIterator<Tuple2<K, V>> iterator = Iterators.peekIterator(input);
        return new IteratorPlus<Tuple2<K, O>>()
        {
            private final Tuple1<K> cKey = Tuple1.of(null);

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Tuple2<K, O> next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Iterator<V> child = Iterators.anyMatchStop(iterator, x -> !Objects.equals(x.key(), cKey.f1)).map(x -> x.value());
                cKey.f1 = iterator.peek().key();
                return Tuple2.of(cKey.f1, mapGroupFunc.apply(cKey.f1, child));
            }
        };
    }

    public static <E> IteratorPlus<Tuple2<E, Long>> zipIndex(Iterator<E> iterator, long startIndex)
    {
        requireNonNull(iterator, "input Iterator is null");
        return new IteratorPlus<Tuple2<E, Long>>()
        {
            private long i = startIndex;

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Tuple2<E, Long> next()
            {
                return Tuple2.of(iterator.next(), i++);
            }
        };
    }
}
