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

import java.util.Arrays;

public class BoolArrayZipUtil
{
    private BoolArrayZipUtil() {}

    public static void zip(boolean[] src, int srcPos, byte[] dest, int destPos, int boolArrayLength)
    {
        int byteSize = (boolArrayLength + 7) >> 3;
        Arrays.fill(dest, destPos, destPos + byteSize, (byte) 0);
        for (int i = 0; i < boolArrayLength; i++) {
            if (src[i + srcPos]) {
                dest[(i >> 3) + destPos] |= 0x80 >> (i & 7);
            }
        }
    }

    public static void unzip(byte[] src, int srcPos, boolean[] dest, int destPos, int boolArrayLength)
    {
        for (int i = 0; i < boolArrayLength; i++) {
            byte v = src[(i >> 3) + srcPos];
            dest[i + destPos] = (v & (0x80 >> (i & 7))) != 0;
        }
    }
}
