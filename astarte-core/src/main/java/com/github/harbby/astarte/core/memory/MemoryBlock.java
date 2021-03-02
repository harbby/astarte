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

import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.io.ByteBufferInputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * 暂不支持溢写到磁盘
 * //todo:　后续会支持溢写到磁盘功能．
 * 该模块主要在以下场景功能中:
 * 1. cache()算子数据缓存
 * 2. groupByKey算子values缓存中
 */
public class MemoryBlock
        extends OutputStream
{
    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private final List<ByteBuffer> pages = new LinkedList<>();
    private ByteBuffer currentPage;
    private long blockSize;

    public void finalData()
    {
        for (ByteBuffer buffer : pages) {
            checkState(buffer.position() > 0);
            buffer.flip();
        }
    }

    /**
     * byte range -128 - 127
     */
    @Override
    public void write(int b)
    {
        if (currentPage != null && currentPage.remaining() > 0) {
            currentPage.put((byte) b);
        }
        else {
            currentPage = MemoryManager.allocateMemory(DEFAULT_PAGE_SIZE);
            currentPage.put((byte) b);
            pages.add(currentPage);
        }
        blockSize++;
    }

    @Override
    public void write(byte[] b)
    {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        for (int i = 0; i < len; i++) {
            this.write(b[i + off]);
        }
    }

    public void free()
    {
        currentPage = null;
        blockSize = 0;
        for (ByteBuffer currentPage : pages) {
            if (currentPage.isDirect()) {
                Platform.freeDirectBuffer(currentPage);
            }
        }
        pages.clear();
    }

    public long getBlockSize()
    {
        return blockSize;
    }

    public InputStream prepareInputStream()
    {
        ByteBuffer[] buffers = pages.stream().map(ByteBuffer::duplicate).toArray(ByteBuffer[]::new);
        return new ByteBufferInputStream(buffers);
    }
}
