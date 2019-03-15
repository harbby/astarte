package codepig.ideal.mppwhater.api.function;

import codepig.ideal.mppwhater.api.Collector;

import java.io.Serializable;

public interface FlatMapper<IN, OUT>
        extends Serializable
{
    void flatMap(IN input, Collector<OUT> collector);
}


