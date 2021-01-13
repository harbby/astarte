package com.github.harbby.ashtarte.deprecated;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.MutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

@Deprecated
public class JoinExperiment
{
    public enum JoinMode
    {
        LEFT_JOIN,
        RIGHT_JOIN,
        INNER_JOIN,
        FULL_JOIN;
    }

    @SafeVarargs
    public static <K> Iterator<Tuple2<K, Iterable<?>[]>> join(Iterator<Tuple2<K, Object>>... iterators)
    {
        return join(Iterators.of(iterators), iterators.length);
    }

    public static <K> Iterator<Tuple2<K, Iterable<?>[]>> join(Iterator<Iterator<Tuple2<K, Object>>> iterators, int length)
    {
        Map<K, Iterable<?>[]> memAppendMap = new HashMap<>();
        int i = 0;
        while (iterators.hasNext()) {
            if (i >= length) {
                throw new IllegalStateException("must length = iterators.size()");
            }

            Iterator<? extends Tuple2<K, Object>> iterator = iterators.next();
            while (iterator.hasNext()) {
                Tuple2<K, Object> t = iterator.next();
                Collection<Object>[] values = (Collection<Object>[]) memAppendMap.get(t.f1());
                if (values == null) {
                    values = new Collection[length];
                    for (int j = 0; j < length; j++) {
                        values[j] = new ArrayList<>();
                    }
                    memAppendMap.put(t.f1(), values);
                }

                values[i].add(t.f2());
            }
            i++;
        }

        return memAppendMap.entrySet().stream().map(x -> new Tuple2<>(x.getKey(), x.getValue()))
                .iterator();
    }

    public static <F1, F2> Iterator<Tuple2<F1, F2>> cartesian(
            Iterable<F1> iterable,
            Iterable<F2> iterable2,
            JoinMode joinMode)
    {
        requireNonNull(iterable);
        requireNonNull(iterable2);
        requireNonNull(joinMode);

        final Collection<F2> collection = (iterable2 instanceof Collection) ?
                (Collection<F2>) iterable2 : MutableList.copy(iterable2);

        Function<F1, Stream<Tuple2<F1, F2>>> mapper = null;
        switch (joinMode) {
            case INNER_JOIN:
                if (collection.isEmpty()) {
                    return Iterators.empty();
                }
                mapper = x2 -> collection.stream().map(x3 -> new Tuple2<>(x2, x3));
                break;
            case LEFT_JOIN:
                mapper = x2 -> collection.isEmpty() ?
                        Stream.of(new Tuple2<>(x2, null)) :
                        collection.stream().map(x3 -> new Tuple2<>(x2, x3));
                break;
            default:
                //todo: other
                throw new UnsupportedOperationException();
        }

        return toStream(iterable)
                .flatMap(mapper)
                .iterator();
    }

    private static <T> Stream<T> toStream(Iterable<T> iterable)
    {
        if (iterable instanceof Collection) {
            return ((Collection<T>) iterable).stream();
        }
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static <F1, F2> Iterator<Tuple2<F1, F2>> cartesian(
            Iterator<F1> iterator,
            Iterator<F2> iterator2,
            JoinMode joinMode)
    {
        requireNonNull(iterator);
        requireNonNull(iterator2);
        requireNonNull(joinMode);

        return cartesian(() -> iterator, () -> iterator2, joinMode);
    }
}
