package com.github.harbby.ashtarte.utils;

import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.base.Throwables;

import java.io.IOException;
import java.io.Serializable;

public class CheckUtil<E extends Serializable>
{
    public static <T extends Serializable> T checkSerialize(T serializable)
    {
        if (serializable == null) {
            return serializable;
        }
        try {
            Serializables.serialize(serializable);
            return serializable;
        }
        catch (IOException e) {
            throw Throwables.throwsThrowable(new IOException(serializable + " check serializable failed", e));
        }
    }
}