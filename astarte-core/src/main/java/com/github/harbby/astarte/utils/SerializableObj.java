package com.github.harbby.astarte.utils;

import com.github.harbby.gadtry.base.Serializables;

import java.io.IOException;
import java.io.Serializable;

import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;
import static java.util.Objects.requireNonNull;

/**
 * 待序列化的对象，在哪里被创建出来。创造时的那里(对象)就需要能被序列化
 */
public class SerializableObj<E extends Serializable>
        implements Serializable
{
    private final byte[] bytes;

    /**
     * @param obj can serialize Obj
     * @see IOException if serialize faild throw IOException
     */
    public SerializableObj(E obj)
    {
        requireNonNull(obj, "obj is null");
        try {
            this.bytes = Serializables.serialize(obj);
        }
        catch (IOException e) {
            throw throwsThrowable(e);
        }
    }

    public static <E extends Serializable> SerializableObj<E> of(E obj)
    {
        return new SerializableObj<>(obj);
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    /**
     * @see IOException if serialize faild throw IOException
     */
    public E getValue()
    {
        return getValue(null);
    }

    /**
     * @see IOException if serialize faild throw IOException
     */
    @SuppressWarnings("unchecked")
    public E getValue(ClassLoader classLoader)
    {
        try {
            return (E) Serializables.byteToObject(bytes, classLoader);
        }
        catch (IOException | ClassNotFoundException e) {
            throw throwsThrowable(e);
        }
    }
}
