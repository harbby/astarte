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

import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static java.util.Objects.requireNonNull;

public abstract class Checksums
        implements Checksum
{
    public void update(ByteBuffer b, int off, int len)
    {
        if (b.hasArray()) {
            update(b.array(), b.arrayOffset() + off, len);
        }
        else {
            for (int i = 0; i < len; i++) {
                update(b.get(off + i));
            }
        }
    }

    public static Checksums lengthCheckSum()
    {
        return new LengthChecksum();
    }

    static Checksums wrapChecksum(Checksum checksum)
    {
        requireNonNull(checksum, "checksum");
        if (checksum instanceof Checksums) {
            return (Checksums) checksum;
        }
        if (checksum instanceof Adler32) {
            return new WarpedAdler32((Adler32) checksum);
        }
        if (checksum instanceof CRC32) {
            return new WarpedCRC32((CRC32) checksum);
        }
        return new Checksums()
        {
            @Override
            public void update(int b)
            {
                checksum.update(b);
            }

            @Override
            public void update(byte[] b, int off, int len)
            {
                checksum.update(b, off, len);
            }

            @Override
            public long getValue()
            {
                return checksum.getValue();
            }

            @Override
            public void reset()
            {
                checksum.reset();
            }
        };
    }

    public static Checksums adler32()
    {
        return new WarpedAdler32(new Adler32());
    }

    public static Checksums crc32()
    {
        return new WarpedCRC32(new CRC32());
    }

    private static class WarpedAdler32
            extends Checksums
    {
        private final Adler32 adler32;

        private WarpedAdler32(Adler32 adler32)
        {
            this.adler32 = adler32;
        }

        @Override
        public void update(ByteBuffer buffer, int off, int len)
        {
            ByteBuffer buff = buffer.duplicate();
            buff.position(off);
            buff.limit(off + len);
            adler32.update(buff);
        }

        @Override
        public void update(int b)
        {
            adler32.update(b);
        }

        @Override
        public void update(byte[] b, int off, int len)
        {
            adler32.update(b, off, len);
        }

        @Override
        public void reset()
        {
            adler32.reset();
        }

        @Override
        public long getValue()
        {
            return adler32.getValue();
        }
    }

    private static class WarpedCRC32
            extends Checksums
    {
        private final CRC32 crc32;

        private WarpedCRC32(CRC32 crc32)
        {
            this.crc32 = crc32;
        }

        @Override
        public void update(int b)
        {
            crc32.update(b);
        }

        @Override
        public void update(byte[] b, int off, int len)
        {
            crc32.update(b, off, len);
        }

        @Override
        public long getValue()
        {
            return crc32.getValue();
        }

        @Override
        public void reset()
        {
            crc32.reset();
        }

        @Override
        public void update(ByteBuffer buffer, int off, int len)
        {
            ByteBuffer buff = buffer.duplicate();
            buff.position(off);
            buff.limit(off + len);
            crc32.update(buff);
        }
    }

    private static class LengthChecksum
            extends Checksums
    {
        private int count = 0;

        @Override
        public void update(int b)
        {
            count++;
        }

        @Override
        public void update(byte[] b, int off, int len)
        {
            count += len;
        }

        @Override
        public long getValue()
        {
            return count;
        }

        @Override
        public void reset()
        {
            count = 0;
        }

        @Override
        public void update(ByteBuffer buffer, int off, int len)
        {
            count += len;
        }
    }
}
