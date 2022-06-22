/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.coders;

import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.array.AnyArrayEncoder;
import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataOutputView;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.base.Throwables;

import java.io.IOException;
import java.io.Serializable;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class JavaEncoder<E extends Serializable>
        implements Encoder<E>
{
    public static final Comparator<?> OBJECT_COMPARATOR = JavaEncoder::objectComparator;

    @Override
    public void encoder(E value, DataOutputView output)
    {
        try {
            byte[] bytes = Serializables.serialize(value);
            output.writeInt(bytes.length);
            output.write(bytes);
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
    }

    @Override
    public E decoder(DataInputView input)
    {
        byte[] bytes = new byte[input.readInt()];
        try {
            input.readFully(bytes);
            return Serializables.byteToObject(bytes);
        }
        catch (IOException | ClassNotFoundException e) {
            throw Throwables.throwThrowable(e);
        }
    }

    private static int objectComparator(Object v1, Object v2)
    {
        if (v1 == v2) {
            return 0;
        }
        if (v1 == null) {
            return -1;
        }
        if (v2 == null) {
            return 1;
        }

        checkState(v1.getClass() == v2.getClass(), "only objects of the same class can be sorted [%s than %s]", v1, v2);
        if (v1 instanceof Tuple2) {
            int than = objectComparator(((Tuple2<?, ?>) v1).key(), ((Tuple2<?, ?>) v2).key());
            if (than != 0) {
                return than;
            }
            return objectComparator(((Tuple2<?, ?>) v1).value(), ((Tuple2<?, ?>) v2).value());
        }
        else if (v1.getClass().isArray()) {
            Object[] arr1 = (Object[]) v1;
            Object[] arr2 = (Object[]) v2;
            return AnyArrayEncoder.comparator(JavaEncoder::objectComparator).compare(arr1, arr2);
        }

        if (v1.getClass() == String.class) {
            return ((String) v1).compareTo((String) v2);
        }
        else if (v1.getClass() == Integer.class) {
            return ((Integer) v1).compareTo((Integer) v2);
        }
        else if (v1.getClass() == Long.class) {
            return ((Long) v1).compareTo((Long) v2);
        }
        else if (v1.getClass() == Short.class) {
            return ((Short) v1).compareTo((Short) v2);
        }
        else if (v1.getClass() == Float.class) {
            return ((Float) v1).compareTo((Float) v2);
        }
        else if (v1.getClass() == Double.class) {
            return ((Double) v1).compareTo((Double) v2);
        }
        else if (v1.getClass() == Byte.class) {
            return ((Byte) v1).compareTo((Byte) v2);
        }
        else if (v1.getClass() == Character.class) {
            return ((Character) v1).compareTo((Character) v2);
        }
        else if (v1.getClass() == Boolean.class) {
            return ((Boolean) v1).compareTo((Boolean) v2);
        }
        else {
            throw new UnsupportedOperationException("not support " + v1.getClass());
        }
    }
}
