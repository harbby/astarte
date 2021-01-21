package com.github.harbby.astarte.core.api.function;

import java.io.Serializable;

public interface Mapper<IN, OUT>
        extends Serializable
{
    OUT map(IN input);
}
