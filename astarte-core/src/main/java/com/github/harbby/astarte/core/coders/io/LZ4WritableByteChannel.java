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
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.zip.Checksum;

public final class LZ4WritableByteChannel
        implements WritableByteChannel
{
    private final WritableByteChannel byteChannel;
    static final byte[] MAGIC = new byte[] {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
    static final int MAGIC_LENGTH = MAGIC.length;

    static final int HEADER_LENGTH = MAGIC_LENGTH // magic bytes
            + 1          // token
            + 4          // compressed length
            + 4          // decompressed length
            + 4;         // checksum

    static final int COMPRESSION_LEVEL_BASE = 10;
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

    static final int COMPRESSION_METHOD_RAW = 0x10;
    static final int COMPRESSION_METHOD_LZ4 = 0x20;

    static final int DEFAULT_SEED = 0x9747b28c;

    private static int compressionLevel(int blockSize)
    {
        if (blockSize < MIN_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be >= " + MIN_BLOCK_SIZE + ", got " + blockSize);
        }
        else if (blockSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be <= " + MAX_BLOCK_SIZE + ", got " + blockSize);
        }
        int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
        //assert (1 << compressionLevel) >= blockSize;
        //assert blockSize * 2 > (1 << compressionLevel);
        compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
        //assert compressionLevel >= 0 && compressionLevel <= 0x0F;
        return compressionLevel;
    }

    private final int blockSize;
    private final int compressionLevel;
    private final LZ4Compressor compressor;
    private final Checksums checksum;
    private final ByteBuffer compressedBuffer;
    private boolean finished;

    public LZ4WritableByteChannel(WritableByteChannel byteChannel, int blockSize, LZ4Compressor compressor, Checksum checksum, boolean syncFlush)
    {
        this.byteChannel = byteChannel;
        this.blockSize = blockSize;
        this.compressor = compressor;
        this.checksum = Checksums.wrapChecksum(checksum);
        this.compressionLevel = compressionLevel(blockSize);
        //this.buffer = new byte[blockSize];
        final int compressedBlockSize = HEADER_LENGTH + compressor.maxCompressedLength(blockSize);
        this.compressedBuffer = Platform.allocateDirectBuffer(compressedBlockSize);
        compressedBuffer.order(ByteOrder.LITTLE_ENDIAN);
        finished = false;
        compressedBuffer.put(MAGIC, 0, MAGIC_LENGTH);
        //System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
    }

    /**
     * Creates a new instance which checks stream integrity using
     * {@link StreamingXXHash32} and doesn't sync flush.
     *
     * @param byteChannel the {@link OutputStream} to feed
     * @param blockSize   the maximum number of bytes to try to compress at once,
     *                    must be &gt;= 64 and &lt;= 32 M
     * @param compressor  the {@link LZ4Compressor} instance to use to compress
     *                    data
     * @see #LZ4WritableByteChannel(WritableByteChannel, int, LZ4Compressor, Checksum, boolean)
     * @see StreamingXXHash32#asChecksum()
     */
    public LZ4WritableByteChannel(WritableByteChannel byteChannel, int blockSize, LZ4Compressor compressor)
    {
        //this(byteChannel, blockSize, compressor, XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), false);
        this(byteChannel, blockSize, compressor, Checksums.lengthCheckSum(), false);
    }

    /**
     * Creates a new instance which compresses with the standard LZ4 compression
     * algorithm.
     *
     * @param out       the {@link WritableByteChannel} to feed
     * @param blockSize the maximum number of bytes to try to compress at once,
     *                  must be &gt;= 64 and &lt;= 32 M
     * @see #LZ4WritableByteChannel(WritableByteChannel, int, LZ4Compressor)
     * @see LZ4Factory#fastCompressor()
     */
    public LZ4WritableByteChannel(WritableByteChannel out, int blockSize)
    {
        this(out, blockSize, LZ4Factory.fastestInstance().fastCompressor());
    }

    public LZ4WritableByteChannel(FileOutputStream out, int blockSize)
    {
        this(out.getChannel(), blockSize);
    }

    /**
     * Creates a new instance which compresses into blocks of 64 KB.
     *
     * @param out the {@link WritableByteChannel} to feed
     * @see #LZ4WritableByteChannel(WritableByteChannel, int)
     */
    public LZ4WritableByteChannel(WritableByteChannel out)
    {
        this(out, 1 << 16);
    }

    @Override
    public int write(ByteBuffer src)
            throws IOException
    {
        int len = flushBufferedData(src);
        src.limit(src.position());
        return len;
    }

    private int flushBufferedData(ByteBuffer buffer)
            throws IOException
    {
        int o = buffer.remaining();
        if (o == 0) {
            return 0;
        }
        compressedBuffer.clear();
        compressedBuffer.position(HEADER_LENGTH);
        checksum.reset();
        checksum.update(buffer, buffer.position(), o);
        final int check = (int) checksum.getValue();
        int compressedLength = compressor.compress(buffer, 0, o, compressedBuffer, HEADER_LENGTH, compressedBuffer.remaining());
        final int compressMethod;
        if (compressedLength >= o) {
            compressMethod = COMPRESSION_METHOD_RAW;
            compressedLength = o;
            compressedBuffer.put(buffer);
            //System.arraycopy(buffer, 0, compressedBuffer, HEADER_LENGTH, o);
        }
        else {
            compressMethod = COMPRESSION_METHOD_LZ4;
        }

        //compressedBuffer[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
        compressedBuffer.put(MAGIC_LENGTH, (byte) (compressMethod | compressionLevel));
        compressedBuffer.putInt(MAGIC_LENGTH + 1, compressedLength);
        compressedBuffer.putInt(MAGIC_LENGTH + 5, o);
        compressedBuffer.putInt(MAGIC_LENGTH + 9, check);
        //assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        compressedBuffer.position(0);
        compressedBuffer.limit(HEADER_LENGTH + compressedLength);
        position += HEADER_LENGTH + compressedLength;
        return byteChannel.write(compressedBuffer);
    }

    /**
     * Same as {@link #close()} except that it doesn't close the underlying stream.
     * This can be useful if you want to keep on using the underlying stream.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void finishBlock()
            throws IOException
    {
        ensureNotFinished();
//        flushBufferedData();
        compressedBuffer.put(MAGIC_LENGTH, (byte) (COMPRESSION_METHOD_RAW | compressionLevel));
        compressedBuffer.putInt(MAGIC_LENGTH + 1, 0);
        compressedBuffer.putInt(MAGIC_LENGTH + 5, 0);
        compressedBuffer.putInt(MAGIC_LENGTH + 9, 0);
        //assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        compressedBuffer.position(0);
        compressedBuffer.limit(HEADER_LENGTH);
        byteChannel.write(compressedBuffer);
        position += HEADER_LENGTH;
        finished = true;
    }

    private long position;

    public long position()
            throws IOException
    {
        return position;
    }

    /**
     * reset pipeline state
     */
    public void beginBlock()
    {
        compressedBuffer.clear();
        finished = false;
        compressedBuffer.put(MAGIC, 0, MAGIC_LENGTH);
        //System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
    }

    /**
     * Same as {@link #close()} except that it doesn't close the underlying stream.
     * This can be useful if you want to keep on using the underlying stream.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void finish()
            throws IOException
    {
        this.finishBlock();
    }

    private void ensureNotFinished()
    {
        if (finished) {
            throw new IllegalStateException("This stream is already closed");
        }
    }

    @Override
    public boolean isOpen()
    {
        return byteChannel.isOpen();
    }

    @Override
    public void close()
            throws IOException
    {
        try (WritableByteChannel ignored = this.byteChannel) {
            if (!finished) {
                finish();
            }
        }
        finally {
            if (compressedBuffer.isDirect()) {
                Platform.freeDirectBuffer(compressedBuffer);
            }
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(outChannel=" + byteChannel + ", blockSize=" + blockSize + ", compressor=" + compressor + ", checksum=" + checksum + ")";
    }
}
