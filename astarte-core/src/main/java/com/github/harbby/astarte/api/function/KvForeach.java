package com.github.harbby.astarte.api.function;

import java.io.Serializable;

public interface KvForeach<K, V>
        extends Serializable
{
    void foreach(K k, V v);
}