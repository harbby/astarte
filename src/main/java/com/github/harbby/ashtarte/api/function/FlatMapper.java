package com.github.harbby.ashtarte.api.function;

import com.github.harbby.ashtarte.api.Collector;

import java.io.Serializable;

public interface FlatMapper<IN, OUT>
        extends Serializable
{
    void flatMap(IN input, Collector<OUT> collector);
}


