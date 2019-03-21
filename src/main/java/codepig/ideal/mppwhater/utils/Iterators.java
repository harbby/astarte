package codepig.ideal.mppwhater.utils;

import java.util.Iterator;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class Iterators
{
    private Iterators() {}

    public static <E> Iterator<E> of(E... values)
    {
        return new Iterator<E>()
        {
            private int cnt = 0;

            @Override
            public boolean hasNext()
            {
                return cnt++ < values.length;
            }

            @Override
            public E next()
            {
                return values[cnt];
            }
        };
    }

    public static long size(Iterator iterator)
    {
        long i;
        for (i = 0; iterator.hasNext(); i++) {
            iterator.next();
        }
        return i;
    }

    public static <IN, OUT> Iterator<OUT> map(Iterator<IN> iterator, Function<IN, OUT> function)
    {
        return new Iterator<OUT>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public OUT next()
            {
                return function.apply(iterator.next());
            }

            @Override
            public void remove()
            {
                iterator.remove();
            }
        };
    }

    public static <E> Iterator<E> empty()
    {
        return new Iterator<E>()
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            public E next()
            {
                throw new UnsupportedOperationException("this method have't support!");
            }
        };
    }

    public static <IN, VALUE> VALUE reduce(Iterator<IN> iterator, Function<IN, VALUE> mapper, BinaryOperator<VALUE> reducer)
    {
        VALUE lastValue = null;
        while (iterator.hasNext()) {
            VALUE value = mapper.apply(iterator.next());
            if (lastValue != null) {
                lastValue = reducer.apply(lastValue, value);
            }
            else {
                lastValue = value;
            }
        }
        return lastValue;
    }

    public static <VALUE> VALUE reduce(Iterator<VALUE> iterator, BinaryOperator<VALUE> reducer)
    {
        VALUE lastValue = null;
        while (iterator.hasNext()) {
            VALUE value = iterator.next();
            if (lastValue != null) {
                lastValue = reducer.apply(lastValue, value);
            }
            else {
                lastValue = value;
            }
        }
        return lastValue;
    }

    public static <IN, OUT> Iterator<OUT> flatMap(Iterator<IN> iterator, Function<IN, OUT[]> function)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }
}
