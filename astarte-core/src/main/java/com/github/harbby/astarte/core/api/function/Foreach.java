package com.github.harbby.astarte.core.api.function;

import java.io.Serializable;

public interface Foreach<E>
        extends Serializable
{
    void apply(E value);
}
