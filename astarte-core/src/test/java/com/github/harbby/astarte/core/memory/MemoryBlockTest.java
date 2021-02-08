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

import com.github.harbby.gadtry.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MemoryBlockTest
{
    @Test
    public void coreFeaturesTest()
            throws IOException
    {
        MemoryBlock block = new MemoryBlock();
        block.write(-1);
        block.write(-2);
        block.write(3);
        block.write("abc".getBytes(StandardCharsets.UTF_8));
        block.finalData();

        byte[] bytes = IOUtils.readAllBytes(block.prepareInputStream());
        Assert.assertArrayEquals(bytes, new byte[] {-1, -2, 3, 97, 98, 99});
    }
}
