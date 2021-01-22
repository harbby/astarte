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
import java.util.Objects;

import static com.github.harbby.gadtry.base.MoreObjects.toStringHelper;

public class Partition
        implements Serializable
{
    private final int index;

    public Partition(int index)
    {
        this.index = index;
    }

    public int getId()
    {
        return index;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", index)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return index;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        Partition other = (Partition) obj;
        return Objects.equals(this.index, other.index);
    }
}
