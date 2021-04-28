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
package com.github.harbby.astarte.core.api;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

public interface BaseRow
        extends Serializable
{
    public int size();

    default boolean isNullAt(int index)
    {
        return getField(index) == null;
    }

    default String getString(int index)
    {
        return getField(index);
    }

    default int getInt(int index)
    {
        return getField(index);
    }

    default long getLong(int index)
    {
        return getField(index);
    }

    default float getFloat(int index)
    {
        return getField(index);
    }

    default double getDouble(int index)
    {
        return getField(index);
    }

    default short getShort(int index)
    {
        return getField(index);
    }

    default byte getByte(int index)
    {
        return getField(index);
    }

    default boolean getBoolean(int index)
    {
        return getField(index);
    }

    default BigDecimal getDecimal(int index)
    {
        return getField(index);
    }

    default Date getDate(int index)
    {
        return getField(index);
    }

    default Timestamp getTimestamp(int index)
    {
        return getField(index);
    }

    public <T> T getField(int index);

    public default BaseRow copy()
    {
        throw new UnsupportedOperationException();
    }
}
