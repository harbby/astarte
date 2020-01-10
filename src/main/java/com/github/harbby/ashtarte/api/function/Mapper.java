package com.github.harbby.ashtarte.api.function;

import java.io.Serializable;

public interface Mapper<IN, OUT>
        extends Serializable
{
    OUT map(IN input);
}
