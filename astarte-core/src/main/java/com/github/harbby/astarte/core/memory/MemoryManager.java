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
package com.github.harbby.astarte.core.memory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryManager
{
    private MemoryManager() {}

    private static final AtomicInteger allocatedMemory = new AtomicInteger(0);

    public static MemoryBlock allocateMemoryBlock()
    {
        return new MemoryBlock();
    }

    public static ByteBuffer allocateMemory(int capacity)
    {
        //ByteBuffer page = Platform.allocateDirectBuffer(capacity);
        ByteBuffer page = ByteBuffer.allocate(capacity);
        allocatedMemory.addAndGet(page.capacity());
        return page;
    }
}
