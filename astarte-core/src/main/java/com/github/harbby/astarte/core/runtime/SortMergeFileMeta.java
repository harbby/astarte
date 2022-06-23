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
package com.github.harbby.astarte.core.runtime;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class SortMergeFileMeta
{
    private final long[] segmentEnds;
    private final long[] segmentRowSize;
    private final int fileHeaderSize;

    public SortMergeFileMeta(long[] segmentEnds, long[] segmentRowSize)
    {
        this.segmentEnds = segmentEnds;
        this.segmentRowSize = segmentRowSize;
        this.fileHeaderSize = getSortMergedFileHarderSize(getSegmentSize());
    }

    public static SortMergeFileMeta readFrom(FileInputStream inputStream)
            throws IOException
    {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        long[] segmentEnds = new long[dataInputStream.readInt()];
        long[] segmentRowSize = new long[segmentEnds.length];
        for (int i = 0; i < segmentEnds.length; i++) {
            segmentEnds[i] = dataInputStream.readLong();
            segmentRowSize[i] = dataInputStream.readLong();
        }
        return new SortMergeFileMeta(segmentEnds, segmentRowSize);
    }

    public static int getSortMergedFileHarderSize(int segmentSize)
    {
        return Integer.BYTES + segmentSize * Long.BYTES * 2;
    }

    public int getSegmentSize()
    {
        return segmentRowSize.length;
    }

    public long getRowCount(int reduceId)
    {
        return segmentRowSize[reduceId];
    }

    public long getPosition(int reduceId)
    {
        if (reduceId == 0) {
            return fileHeaderSize;
        }
        return fileHeaderSize + segmentEnds[reduceId - 1];
    }

    public long getLength(int reduceId)
    {
        if (reduceId == 0) {
            return segmentEnds[reduceId];
        }
        return segmentEnds[reduceId] - segmentEnds[reduceId - 1];
    }
}
