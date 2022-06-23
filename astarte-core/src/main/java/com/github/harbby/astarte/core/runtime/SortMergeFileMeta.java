package com.github.harbby.astarte.core.runtime;

import com.github.harbby.astarte.core.coders.io.IoUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SortMergeFileMeta
{
    private final int size;
    private final long[] segmentEnds;
    private final long[] segmentRowSize;

    private static final int metaSize = 20;

    public SortMergeFileMeta(long[] segmentEnds, long[] segmentRowSize)
    {
        this.segmentEnds = segmentEnds;
        this.segmentRowSize = segmentRowSize;
        this.size = segmentEnds.length;
    }

    public int metaSize()
    {
        return metaSize;
    }

    public static SortMergeFileMeta readBy(InputStream inputStream)
            throws IOException
    {
        byte[] bytes = new byte[metaSize];
        IoUtils.readFully(inputStream, bytes);
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        long[] segmentEnds = new long[dataInputStream.readInt()];
        long[] segmentRowSize = new long[segmentEnds.length];
        for (int i = 0; i < segmentEnds.length; i++) {
            segmentEnds[i] = dataInputStream.readLong();
            segmentRowSize[i] = dataInputStream.readLong();
        }
        return new SortMergeFileMeta(segmentEnds, segmentRowSize);
    }
}
