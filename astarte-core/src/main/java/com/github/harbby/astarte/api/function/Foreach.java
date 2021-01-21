package com.github.harbby.astarte.api.function;

import java.io.Serializable;

public interface Foreach<E>
        extends Serializable
{
    void apply(E value);
}
