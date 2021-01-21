package com.github.harbby.astarte.api.function;

import java.io.Serializable;

public interface MapGroupFunc<K, ROW, OUT>
        extends Serializable
{
    OUT apply(K k, Iterable<ROW> iterable);
}
