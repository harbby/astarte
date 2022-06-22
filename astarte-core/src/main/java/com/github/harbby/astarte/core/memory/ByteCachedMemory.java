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

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderInputStream;
import com.github.harbby.astarte.core.coders.io.DataInputViewImpl;
import com.github.harbby.astarte.core.coders.io.DataOutputView;
import com.github.harbby.astarte.core.coders.io.DataOutputViewImpl;
import com.github.harbby.astarte.core.operator.CacheManager;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * Cache数据，使用字节存储
 */
public class ByteCachedMemory<E>
        extends CacheManager.CacheMemory<E>
{
    private static final Logger logger = LoggerFactory.getLogger(ByteCachedMemory.class);
    private final Encoder<E> encoder;
    private final MemoryBlock block;
    private final DataOutputView outputView;
    private long count;

    public ByteCachedMemory(Encoder<E> encoder)
    {
        this.encoder = encoder;
        this.block = MemoryManager.allocateMemoryBlock();
        this.outputView = new DataOutputViewImpl(new LZ4BlockOutputStream(block));
    }

    @Override
    public void freeMemory()
    {
        block.free();
    }

    @Override
    public Iterator<E> prepareIterator()
    {
        checkState(isFinal, "only reader mode");
        LZ4BlockInputStream lz4BlockInputStream = new LZ4BlockInputStream(block.prepareInputStream());
        return new EncoderInputStream<>(count, encoder, new DataInputViewImpl(lz4BlockInputStream));
    }

    @Override
    public void finalCache()
    {
        super.finalCache();
        outputView.close();
        block.finalData();
    }

    @Override
    public void append(E record)
    {
        checkState(!isFinal, "don't append record to writeMode");
        count++;
        encoder.encoder(record, outputView);
    }

    @Override
    protected void finalize()
            throws Throwable
    {
        if (block.getBlockSize() > 0) {
            logger.error("A memory[size: {}] leak risk is found and is trying to release", block.getBlockSize());
            this.freeMemory();
        }
        super.finalize();
    }
}
