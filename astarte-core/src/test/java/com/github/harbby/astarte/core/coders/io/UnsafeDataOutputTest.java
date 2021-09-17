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

import com.github.harbby.gadtry.aop.MockGo;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import static com.github.harbby.gadtry.aop.MockGo.when;
import static com.github.harbby.gadtry.aop.mockgo.MockGoArgument.any;

public class UnsafeDataOutputTest
{
    @Test
    public void encodeTest()
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        WritableByteChannel channel = MockGo.mock(WritableByteChannel.class);
        when(channel.write(any())).thenAround(p -> {
            ByteBuffer buf = (ByteBuffer) p.getArgument(0);
            int len = buf.remaining();
            buffer.put(buf);
            return len;
        });

        UnsafeDataOutput unsafeDataOutput = new UnsafeDataOutput(channel, 1 << 16);
        unsafeDataOutput.writeInt(1);
        unsafeDataOutput.writeLong(2L);
        unsafeDataOutput.writeDouble(3.14D);
        unsafeDataOutput.close();

        buffer.flip();
        Assert.assertEquals(buffer.getInt(), 1);
        Assert.assertEquals(buffer.getLong(), 2L);
        Assert.assertEquals(buffer.getDouble(), 3.14D, 0.0000001D);
    }

    @Test
    public void encodeTest2()
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
        WritableByteChannel channel = MockGo.mock(WritableByteChannel.class);
        when(channel.write(any())).thenAround(p -> {
            ByteBuffer buf = (ByteBuffer) p.getArgument(0);
            int len = buf.remaining();
            buffer.put(buf);
            return len;
        });
        UnsafeDataOutput unsafeDataOutput = new UnsafeDataOutput(channel, 1 << 16);
        unsafeDataOutput.writeInt(1);
        unsafeDataOutput.writeLong(2L);
        unsafeDataOutput.writeDouble(3.14D);
        unsafeDataOutput.close();

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        //--------------------------------------------------------
        UnsafeDataInput unsafeDataInput = new UnsafeDataInput(new ByteArrayInputStream(bytes));
        Assert.assertEquals(unsafeDataInput.readInt(), 1);
        Assert.assertEquals(unsafeDataInput.readLong(), 2L);
        Assert.assertEquals(unsafeDataInput.readDouble(), 3.14D, 0.0000001D);
    }
}
