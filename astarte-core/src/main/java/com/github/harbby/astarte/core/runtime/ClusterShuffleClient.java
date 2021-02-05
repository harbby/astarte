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

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.MutableSet;
import com.github.harbby.gadtry.collection.StateOption;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * n * n client
 * <p>
 * 非线程安全的
 */
public class ClusterShuffleClient
        implements ShuffleClient
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterShuffleClient.class);
    private static final ThreadLocal<ClusterShuffleClient> clientManagerTl = new ThreadLocal<>();
    private final Map<SocketAddress, ShuffleClientHandler> concurrentMap = new HashMap<>();
    private final List<ChannelFuture> futures = new ArrayList<>();

    private ClusterShuffleClient(Set<SocketAddress> shuffleServices)
            throws InterruptedException
    {
        Thread taskThread = Thread.currentThread();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        for (SocketAddress address : shuffleServices) {
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
                                throws Exception
                        {
                            ShuffleClientHandler shuffleClientHandler = new ShuffleClientHandler(taskThread);
                            Lz4FrameDecoder lz4FrameDecoder = new Lz4FrameDecoder();
                            ch.pipeline()
                                    .addLast(new FinishEventHandler(shuffleClientHandler, lz4FrameDecoder))
                                    .addLast(lz4FrameDecoder)
                                    .addLast(shuffleClientHandler);
                        }
                    });
            futures.add(bootstrap.connect(address).sync());
        }
        while (concurrentMap.size() < shuffleServices.size()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    static ClusterShuffleClient start(Set<SocketAddress> shuffleServices)
            throws InterruptedException
    {
        ClusterShuffleClient clientManager = clientManagerTl.get();
        if (clientManager == null) {
            clientManager = new ClusterShuffleClient(shuffleServices);
            clientManagerTl.set(clientManager);
        }
        return clientManager;
    }

    @Override
    public <K, V> Iterator<Tuple2<K, V>> readShuffleData(Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId)
    {
        return Iterators.concat(concurrentMap.values().stream().map(handler -> {
            handler.begin(encoder, shuffleId, reduceId);
            return Iterators.map(handler, obj -> (Tuple2<K, V>) obj);
        }).iterator());
    }

    @Override
    public void close()
            throws IOException
    {
        logger.info("close... shuffle client");
        clientManagerTl.remove();
        concurrentMap.forEach((k, v) -> v.close());
        futures.forEach(channelFuture -> channelFuture.channel().close());
        concurrentMap.clear();
        futures.clear();
    }

    private static final class ShuffleDataDecoder
            extends InputStream
    {
        private ByteBuf byteBuf;

        public void setByteBuf(ByteBuf byteBuf)
        {
            this.byteBuf = byteBuf;
        }

        @Override
        public int read()
        {
            return byteBuf.readByte() & 0xFF;
        }
    }

    /**
     * todo: copy to gadtry MoreObject.copyOverwriteObjectState()
     * copy(浅) source object field data to target Object
     *
     * @param modelClass copy model
     * @param source     source object
     * @param target     target object
     */
    private static void copyOverwriteObjectState(Class<?> modelClass, Object source, Object target)
    {
        requireNonNull(modelClass, "modelClass is null");
        requireNonNull(source, "source is null");
        requireNonNull(target, "target is null");
        checkState(!modelClass.isInterface(), "don't copy interface field");

        Set<Field> fields = MutableSet.<Field>builder().addAll(modelClass.getDeclaredFields())
                .addAll(modelClass.getFields())
                .build();

        for (Field field : fields) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            try {
                Object value = field.get(source);
                field.set(target, value);
            }
            catch (IllegalAccessException e) {
                throw Throwables.throwsThrowable(e);
            }
        }
    }

    private class FinishEventHandler
            extends ChannelInboundHandlerAdapter
    {
        private final ShuffleClientHandler shuffleClientHandler;
        private final Lz4FrameDecoder lz4FrameDecoder;
        private long[] fileLengths;
        private long currentSize;
        private int currentIndex;

        private FinishEventHandler(ShuffleClientHandler shuffleClientHandler, Lz4FrameDecoder lz4FrameDecoder)
        {
            this.shuffleClientHandler = shuffleClientHandler;
            this.lz4FrameDecoder = lz4FrameDecoder;
        }

        private void initLz4DecoderCheckFinish()
        {
            boolean finish = lz4FrameDecoder.isClosed();
            Lz4FrameDecoder initState = new Lz4FrameDecoder();
            copyOverwriteObjectState(Lz4FrameDecoder.class, initState, lz4FrameDecoder);
            checkState(finish, "lz4Decoder state failed, not FINISHED");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in1 = (ByteBuf) msg;
            //read header
            if (fileLengths == null) {
                int fileNumber = in1.readInt();
                if (fileNumber == 0) {
                    shuffleClientHandler.finish();
                    ReferenceCountUtil.release(in1);
                    return;
                }
                //read header
                fileLengths = new long[fileNumber];
                for (int i = 0; i < fileNumber; i++) {
                    fileLengths[i] = in1.readLong();
                }
                currentSize = fileLengths[0];
                currentIndex = 0;
                logger.debug("downloading files{}", this.fileLengths);
            }
            int readableBytes = in1.readableBytes();
            if (currentSize > readableBytes) {
                currentSize -= readableBytes;
                ctx.fireChannelRead(in1);
            }
            else if (currentIndex == fileLengths.length - 1) {
                checkState(currentSize == readableBytes, "file size error");
                logger.debug("download files{} succeed", this.fileLengths);
                ctx.fireChannelRead(in1);
                ctx.fireChannelReadComplete();
                shuffleClientHandler.finish();
                this.fileLengths = null;
                initLz4DecoderCheckFinish();
            }
            else {
                int writerIndex = in1.writerIndex();
                ReferenceCountUtil.retain(in1);  //引用计数器加1
                ctx.fireChannelRead(in1.writerIndex(in1.readerIndex() + (int) currentSize)); //引用计数会减1
                initLz4DecoderCheckFinish();
                //----------------------------------------
                if (readableBytes > currentSize) {
                    checkState(in1.refCnt() > 0);
                    ctx.fireChannelRead(in1.writerIndex(writerIndex));
                }
                else {
                    ReferenceCountUtil.release(in1); //引用计数减1
                }
                currentSize = fileLengths[++currentIndex] - readableBytes + currentSize;
                logger.debug("download file[{}/{}] size: {} succeed, next size {}", currentIndex - 1, fileLengths.length,
                        fileLengths[currentIndex - 1], currentSize);
                if (currentSize == 0) {
                    ctx.fireChannelReadComplete();
                    shuffleClientHandler.finish();
                    this.fileLengths = null;
                    initLz4DecoderCheckFinish();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            shuffleClientHandler.exceptionCaught(ctx, cause);
        }
    }

    private class ShuffleClientHandler
            extends ChannelInboundHandlerAdapter
            implements Iterator<Object>, Closeable
    {
        private final Thread taskThread;
        private ChannelHandlerContext ctx;
        //todo: use number size buffer, 建议使用定长ByteBuffer
        private final BlockingQueue<Object> buffer = new LinkedBlockingQueue<>(1024);
        private final StateOption<Object> option = StateOption.empty();

        private volatile boolean downloadEnd = false;
        private volatile Throwable cause;
        private volatile Encoder<?> encoder;

        private ShuffleClientHandler(Thread taskThread)
        {
            this.taskThread = taskThread;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            ShuffleClientHandler old = concurrentMap.put(ctx.channel().remoteAddress(), this);
            checkState(old == null, "");
            this.ctx = ctx;
        }

        public void finish()
        {
            downloadEnd = true;
            taskThread.interrupt();
        }

        private final ShuffleDataDecoder shuffleDataDecoder = new ShuffleDataDecoder();
        private final DataInputStream dataInputStream = new DataInputStream(shuffleDataDecoder);

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in1 = (ByteBuf) msg;
            shuffleDataDecoder.setByteBuf(in1);
            while (in1.readableBytes() > 0) {
                Object value = encoder.decoder(dataInputStream);
                buffer.put(value);
            }
            //ReferenceCountUtil.release(in1);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            this.cause = cause;
            taskThread.interrupt();
        }

        private void begin(Encoder<?> encoder, int shuffleId, int reduceId)
        {
            this.encoder = encoder;
            downloadEnd = false;
            cause = null;
            buffer.clear();
            option.remove();

            ByteBuf byteBuf = ctx.alloc().directBuffer(8, 8);
            byteBuf.writeInt(shuffleId);
            byteBuf.writeInt(reduceId);
            ctx.writeAndFlush(byteBuf);
        }

        @Override
        public boolean hasNext()
        {
            if (this.cause != null) {
                throw new RuntimeException("reduce reader failed", this.cause);
            }
            else if (option.isDefined()) {
                return true;
            }

            while (true) {
                if (this.cause != null) {
                    throw Throwables.throwsThrowable(this.cause);
                }
                else if (this.downloadEnd) {
                    return getEndStateValue();
                }

                try {
                    option.update(this.buffer.take());
                    return true;
                }
                catch (InterruptedException ignored) {
                    logger.debug("task interrupted");
                }
            }
        }

        private boolean getEndStateValue()
        {
            Object bytes = this.buffer.poll();
            if (bytes != null) {
                option.update(bytes);
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public Object next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return option.remove();
        }

        @Override
        public void close()
        {
            ctx.close();
        }
    }
}
