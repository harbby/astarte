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

import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataInputViewImpl;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class SortShuffleClusterClient
        implements ShuffleClient
{
    static final ByteBuf STOP_DOWNLOAD = Unpooled.EMPTY_BUFFER;
    private static final Logger logger = LoggerFactory.getLogger(SortShuffleClusterClient.class);
    private final Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks;
    List<NioEventLoopGroup> eventLoopGroups = new ArrayList<>();

    public SortShuffleClusterClient(Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks)
    {
        this.dependMapTasks = dependMapTasks;
    }

    @Override
    public <K, V> Iterator<Tuple2<K, V>> createShuffleReader(Comparator<K> comparator, Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId)
            throws IOException
    {
        Map<Integer, InetSocketAddress> mapTaskIds = dependMapTasks.get(shuffleId);
        List<ShuffleClientHandler<K, V>> handlers = new ArrayList<>();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(mapTaskIds.size(), r -> {
            Thread thread = new Thread(r);
            thread.setName(String.format("shuffle_client_stageId:%s_reduceId:%s", shuffleId, reduceId));
            thread.setDaemon(true);
            return thread;
        });
        eventLoopGroups.add(workerGroup);
        for (Map.Entry<Integer, InetSocketAddress> entry : mapTaskIds.entrySet()) {
            ShuffleClientHandler<K, V> shuffleClientHandler = new ShuffleClientHandler<>(encoder, shuffleId, reduceId, entry.getKey());
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(SocketChannel ch)
                        {
                            Lz4FrameDecoder lz4FrameDecoder = new Lz4FrameDecoder();
                            ch.pipeline()
                                    .addLast(new HeaderEventHandler(shuffleClientHandler))
                                    .addLast(lz4FrameDecoder)
                                    .addLast(shuffleClientHandler);
                        }
                    });
            ChannelFuture future = bootstrap.connect(entry.getValue());
            handlers.add(shuffleClientHandler);
        }
        return Iterators.mergeSorted((x, y) -> comparator.compare(x.key(), y.key()),
                handlers.stream().map(ShuffleClientHandler::getReader).collect(Collectors.toList()));
    }

    @Override
    public void close()
            throws IOException
    {
        for (NioEventLoopGroup group : eventLoopGroups) {
            group.shutdownGracefully();
        }
    }

    private static class HeaderEventHandler
            extends ChannelInboundHandlerAdapter
    {
        private final ShuffleClientHandler<?, ?> shuffleClientHandler;
        private Long awaitDownLoadSize;

        private HeaderEventHandler(ShuffleClientHandler<?, ?> shuffleClientHandler)
        {
            this.shuffleClientHandler = shuffleClientHandler;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in1 = (ByteBuf) msg;
            //read header
            if (awaitDownLoadSize == null) {
                this.awaitDownLoadSize = in1.readLong();
                long rowCount = in1.readLong();
                if (this.awaitDownLoadSize == 0) {
                    logger.warn("try download size is zero.");
                    ReferenceCountUtil.release(in1);
                    ctx.fireChannelReadComplete();
                    shuffleClientHandler.finish();
                    return;
                }
                else {
                    logger.debug("downloading shuffleId[{}] MapId[{}] reduce[{}] data bytes is {} rowCount {}",
                            shuffleClientHandler.shuffleId,
                            shuffleClientHandler.mapId,
                            shuffleClientHandler.reduceId,
                            this.awaitDownLoadSize,
                            rowCount);
                    shuffleClientHandler.begin(rowCount);
                }
            }
            int readableBytes = in1.readableBytes();
            if (readableBytes == 0) {
                ReferenceCountUtil.release(in1);
                return;
            }
            awaitDownLoadSize -= readableBytes;
            ctx.fireChannelRead(in1);
            if (awaitDownLoadSize <= 0) {
                checkState(awaitDownLoadSize == 0);
                ctx.fireChannelReadComplete();
                shuffleClientHandler.finish();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            shuffleClientHandler.exceptionCaught(ctx, cause);
        }
    }

    private static class ByteBufIteratorReader<K, V>
            extends InputStream
            implements Iterator<Tuple2<K, V>>
    {
        private final Encoder<Tuple2<K, V>> encoder;
        private final BlockingQueue<ByteBuf> buffer = new LinkedBlockingQueue<>(10);
        private final DataInputView inputView = new DataInputViewImpl(this);
        private ByteBuf byteBuf;
        private volatile Throwable cause;
        private long rowCount = -1;
        private long index = 0;

        private void push(ByteBuf byteBuf)
                throws InterruptedException
        {
            buffer.put(byteBuf);
        }

        private void begin(long rowCount)
        {
            this.rowCount = rowCount;
            this.index = 0;
        }

        public void downloadFailed(Throwable e)
                throws InterruptedException
        {
            this.cause = e;
            buffer.clear();
            buffer.put(STOP_DOWNLOAD);
        }

        private ByteBufIteratorReader(Encoder<Tuple2<K, V>> encoder)
        {
            this.encoder = encoder;
        }

        @Override
        public boolean hasNext()
        {
            if (byteBuf == null) {
                try {
                    byteBuf = buffer.take();
                }
                catch (InterruptedException e) {
                    logger.warn("whether the task is being killed?", e);
                    return false;
                }
            }
            return index < rowCount;
        }

        @Override
        public Tuple2<K, V> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple2<K, V> value = encoder.decoder(inputView);
            index++;
            return value;
        }

        @Override
        public int read()
                throws IOException
        {
            if (byteBuf == STOP_DOWNLOAD) {
                if (cause != null) {
                    throw new IOException(cause);
                }
                return -1;
            }
            int v = byteBuf.readByte() & 0xFF;
            if (byteBuf.readableBytes() == 0) {
                ReferenceCountUtil.release(byteBuf);
                nextBuff();
            }
            return v;
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            if (byteBuf == STOP_DOWNLOAD) {
                return -1;
            }
            int rlen = len;
            int offset = off;
            do {
                int cacheSize = byteBuf.readableBytes();
                if (byteBuf.readableBytes() > rlen) {
                    byteBuf.readBytes(b, offset, rlen);
                    return offset - off + rlen;
                }
                else {
                    byteBuf.readBytes(b, offset, cacheSize);
                    offset += cacheSize;
                    rlen -= cacheSize;
                    ReferenceCountUtil.release(byteBuf);
                    nextBuff();
                }
            }
            while (byteBuf != STOP_DOWNLOAD);

            if (cause != null) {
                throw new IOException(cause);
            }
            return offset - off;
        }

        private void nextBuff()
        {
            while (true) {
                ByteBuf buf = buffer.poll();
                if (buf != null) {
                    this.byteBuf = buf;
                    break;
                }
            }
        }
    }

    private static class ShuffleClientHandler<K, V>
            extends ChannelInboundHandlerAdapter
    {
        private final int mapId;
        private final int shuffleId;
        private final int reduceId;
        private final ByteBufIteratorReader<K, V> reader;

        private ChannelHandlerContext ctx;

        private ShuffleClientHandler(Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId, int mapId)
        {
            this.mapId = mapId;
            this.shuffleId = shuffleId;
            this.reduceId = reduceId;
            this.reader = new ByteBufIteratorReader<>(encoder);
        }

        public ByteBufIteratorReader<K, V> getReader()
        {
            return reader;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            this.ctx = ctx;
            //begin
            ByteBuf byteBuf = ctx.alloc().buffer(Integer.BYTES * 3, Integer.BYTES * 3);
            byteBuf.writeInt(shuffleId);
            byteBuf.writeInt(reduceId);
            byteBuf.writeInt(mapId);
            ctx.writeAndFlush(byteBuf);
        }

        public void begin(long rowCount)
        {
            reader.begin(rowCount);
        }

        public void finish()
                throws InterruptedException
        {
            reader.push(STOP_DOWNLOAD);
            ctx.close();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in1 = (ByteBuf) msg;
            if (in1.readableBytes() > 0) {
                reader.push(in1);
            }
            else {
                ReferenceCountUtil.release(in1);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.error("download shuffle data failed", cause);
            ctx.close();
            reader.downloadFailed(cause);
        }
    }
}
