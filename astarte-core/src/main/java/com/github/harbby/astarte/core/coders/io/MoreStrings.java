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
package com.github.harbby.astarte.core.coders.io;

import com.github.harbby.gadtry.base.Platform;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class MoreStrings
{
    private static final Unsafe unsafe = Platform.getUnsafe();

    public static final boolean ENABLE_STRING_COMPACT;
    public static final long STRING_CODER_OFFSET;

    private MoreStrings() {}

    static {
        boolean enableStringCompact;
        long offset;
        try {
            Field field = String.class.getDeclaredField("COMPACT_STRINGS");
            enableStringCompact = unsafe.getBoolean(String.class, unsafe.staticFieldOffset(field));

            field = String.class.getDeclaredField("coder");
            offset = unsafe.objectFieldOffset(field);
        }
        catch (NoSuchFieldException e) {
            enableStringCompact = false;
            offset = -1;
        }
        ENABLE_STRING_COMPACT = enableStringCompact;
        STRING_CODER_OFFSET = offset;
    }

    public static boolean isAscii(String s, int len)
    {
        if (ENABLE_STRING_COMPACT) {
            return unsafe.getByte(s, STRING_CODER_OFFSET) == 0;
        }
        // java8
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c > 0x7F) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAscii(String s, int len, int maxCheckLength)
    {
        if (ENABLE_STRING_COMPACT) {
            return unsafe.getByte(s, STRING_CODER_OFFSET) == 0;
        }
        // java8
        if (len > maxCheckLength) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c > 0x7F) {
                return false;
            }
        }
        return true;
    }
}
