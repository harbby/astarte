package com.github.harbby.ashtarte.api.function;

import java.io.Serializable;

public interface Filter<IN>
        extends Serializable
{
    boolean filter(IN input);
}
