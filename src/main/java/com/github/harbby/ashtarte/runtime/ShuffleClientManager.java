package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.StateOption;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * n * n client
 */
public class ShuffleClientManager
{
    private static final Logger logger = LoggerFactory.getLogger(ShuffleClientManager.class);
    private ConcurrentMap<SocketAddress, ShuffleClientHandler> concurrentMap = new ConcurrentHashMap<>();

    public void start(Set<SocketAddress> shuffleServices)
            throws InterruptedException
    {
        final Thread taskThread = Thread.currentThread();
        for (SocketAddress address : shuffleServices) {
            NioEventLoopGroup workerGroup = new NioEventLoopGroup();
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
            bootstrap.connect(address).sync();
        }
    }

    public <K, V> Iterator<Iterator<Tuple2<K, V>>> readShuffleData(int shuffleId, int reduceId)
    {
        Iterator<Iterator<Tuple2<K, V>>> iterators = concurrentMap.values().stream().map(ctx -> {
            ctx.begin(shuffleId, reduceId);
            Iterator<Tuple2<K, V>> iterator = new Iterator<Tuple2<K, V>>()
            {
                private final StateOption<byte[]> option = StateOption.empty();

                @Override
                public boolean hasNext()
                {
                    if (ctx.cause != null) {
                        throw new RuntimeException("reduce reader failed", ctx.cause);
                    }
                    if (option.isDefined()) {
                        return true;
                    }
                    while (true) {
                        byte[] bytes = ctx.buffer.poll();
                        if (bytes != null) {
                            option.update(bytes);
                            return true;
                        }
                        else if (ctx.downloadEnd) {
                            return false;
                        }
                        else {
                            try {
                                TimeUnit.MILLISECONDS.sleep(1);
                            }
                            catch (InterruptedException e) {
                                // netty data io read failed
                                if (ctx.cause != null) {
                                    throw new RuntimeException("reduce reader failed", ctx.cause);
                                }
                                //todo: 1. 主动kill task
                                return false;  // or use throws
                            }
                        }
                    }
                }

                @Override
                public Tuple2<K, V> next()
                {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    try {
                        return Serializables.byteToObject(option.remove());
                    }
                    catch (IOException | ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            return iterator;
        }).iterator();
        return iterators;
    }

    private class ShuffleClientHandler
            extends ChannelInboundHandlerAdapter
    {
        private final Thread taskThread;
        private ChannelHandlerContext ctx;
        //todo: use number size buffer, 建议使用定长ByteBuffer
        private final BlockingQueue<byte[]> buffer = new ArrayBlockingQueue<>(1024);
        private volatile boolean downloadEnd = false;
        private volatile Throwable cause;

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

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in = (ByteBuf) msg;
            int len;
            while (in.readableBytes() > 0 && (len = in.readInt()) != -1) {
                byte[] bytes = new byte[len];
                in.readBytes(bytes);
                buffer.put(bytes);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)
                throws Exception
        {
            downloadEnd = true;
        }

        public void begin(int shuffleId, int reduceId)
        {
            ByteBuf byteBuf = ctx.alloc().buffer();
            byteBuf.writeInt(shuffleId);
            byteBuf.writeInt(reduceId);
            ctx.writeAndFlush(byteBuf);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            this.cause = cause;
            taskThread.interrupt();
        }
    }
}
