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
package com.github.harbby.astarte.core;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

public class MapTaskState
        implements Externalizable
{
    private ByteBuffer header;
    private int mapId;
    private long[] segmentEnds;

    public MapTaskState(ByteBuffer header, int mapId)
    {
        this.header = header;
        this.mapId = mapId;
    }

    public MapTaskState() {}

    public int getMapId()
    {
        return mapId;
    }

    public long[] getSegmentEnds()
    {
        return segmentEnds;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException
    {
        out.writeInt(mapId);
        out.write(header.array());
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException
    {
        this.mapId = in.readInt();
        this.segmentEnds = new long[in.readInt()];
        for (int i = 0; i < segmentEnds.length; i++) {
            segmentEnds[i] = in.readLong();
        }
    }
}
