package com.github.harbby.astarte.api.function;

import java.io.Serializable;

public interface Filter<IN>
        extends Serializable
{
    boolean filter(IN input);
}
