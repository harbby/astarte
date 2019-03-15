package codepig.ideal.mppwhater.api;

import java.io.Serializable;

public class Tuple2<A, B>
        implements Serializable
{
    private A a;
    private B b;

    public Tuple2(A a, B b)
    {
        this.a = a;
        this.b = b;
    }

    public static <A, B> Tuple2<A, B> of(A a, B b)
    {
        return new Tuple2<>(a, b);
    }

    public A f1()
    {
        return a;
    }

    public B f2()
    {
        return b;
    }

    @Override
    public String toString()
    {
        return "Tuple2{" +
                "a=" + a +
                ", b=" + b +
                '}';
    }
}
