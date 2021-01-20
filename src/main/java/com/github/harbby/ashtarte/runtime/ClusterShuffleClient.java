package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.StateOption;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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
                            ch.pipeline().addLast(new ShuffleClientHandler(taskThread));
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
    public <K, V> Iterator<Tuple2<K, V>> readShuffleData(int shuffleId, int reduceId)
    {
        return Iterators.concat(concurrentMap.values().stream().map(handler -> {
            handler.begin(shuffleId, reduceId);
            return Iterators.map(handler, bytes -> {
                try {
                    return Serializables.<Tuple2<K, V>>byteToObject(bytes);
                }
                catch (IOException | ClassNotFoundException e) {
                    throw Throwables.throwsThrowable(e);
                }
            });
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

    private class ShuffleClientHandler
            extends LengthFieldBasedFrameDecoder
            implements Iterator<byte[]>, Closeable
    {
        private final Thread taskThread;
        private ChannelHandlerContext ctx;
        //todo: use number size buffer, 建议使用定长ByteBuffer
        private final BlockingQueue<byte[]> buffer = new LinkedBlockingQueue<>(1024);
        private final StateOption<byte[]> option = StateOption.empty();

        private volatile boolean downloadEnd = false;
        private volatile Throwable cause;

        private ShuffleClientHandler(Thread taskThread)
        {
            super(1048576, 0, 4);
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

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in1)
                throws Exception
        {
            if (in1.readableBytes() == 4) {
                if (in1.readInt() == -1) {
                    downloadEnd = true;
                    taskThread.interrupt();
                    return null;
                }
            }
            ByteBuf frame = (ByteBuf) super.decode(ctx, in1);
            if (frame == null) {
                return null;
            }
            //check must frame.readableBytes() > 4;
            byte[] bytes = new byte[frame.readInt()];
            frame.readBytes(bytes);
            if (logger.isDebugEnabled()) {
                logger.debug("taskThread {}, io read {}", taskThread, bytes);
            }
            ReferenceCountUtil.release(in1);
            buffer.put(bytes);
            return bytes;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            this.cause = cause;
            taskThread.interrupt();
        }

        private void begin(int shuffleId, int reduceId)
        {
            downloadEnd = false;
            cause = null;
            buffer.clear();
            option.remove();

            ByteBuf byteBuf = ctx.alloc().buffer(8, 8);
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
                    throw new RuntimeException("reducer download shuffle read failed", this.cause);
                }
                else if (this.downloadEnd) {
                    return getEndStateValue();
                }

                try {
                    option.update(this.buffer.take());
                    return true;
                }
                catch (InterruptedException ignored) {
                }
            }
        }

        private boolean getEndStateValue()
        {
            byte[] bytes = this.buffer.poll();
            if (bytes != null) {
                option.update(bytes);
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public byte[] next()
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
