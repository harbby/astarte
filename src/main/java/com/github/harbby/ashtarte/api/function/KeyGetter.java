package com.github.harbby.ashtarte.api.function;

import java.io.Serializable;

public interface KeyGetter<Row, KEY>
        extends Serializable
{
    KEY apply(Row input);
}
