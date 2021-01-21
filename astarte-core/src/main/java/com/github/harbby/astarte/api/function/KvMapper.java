package com.github.harbby.astarte.api.function;

import java.io.Serializable;

public interface KvMapper<K, V, OUT>
        extends Serializable
{
    OUT map(K k, V v);
}
