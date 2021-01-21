package com.github.harbby.astarte.core.api.function;

import java.io.Serializable;

public interface Reducer<ROW>
        extends Serializable
{
    ROW reduce(ROW input1, ROW input2);
}
