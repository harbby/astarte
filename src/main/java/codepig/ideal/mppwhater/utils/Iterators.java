package codepig.ideal.mppwhater.utils;

import java.util.Iterator;
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

//    public static <IN, OUT> Iterator<OUT> flatMap(Iterator<IN> iterator, Function<IN, OUT[]> function)
//    {
//        return new Iterator<OUT>()
//        {
//            @Override
//            public boolean hasNext()
//            {
//                return iterator.hasNext();
//            }
//
//            @Override
//            public OUT next()
//            {
//                OUT[] outs = function.apply(iterator.next());
//            }
//
//            @Override
//            public void remove()
//            {
//                iterator.remove();
//            }
//        };
//    }
}
