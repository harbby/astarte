package com.github.harbby.astarte.core.api.function;

import java.io.Serializable;

public interface Filter<IN>
        extends Serializable
{
    boolean filter(IN input);
}
