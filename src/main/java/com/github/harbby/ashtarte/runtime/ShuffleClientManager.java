package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.base.Iterators;
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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * n * n client
 */
public class ShuffleClientManager
{
    private ConcurrentMap<SocketAddress, ShuffleClientHandler> concurrentMap = new ConcurrentHashMap<>();

    public void start(Set<SocketAddress> shuffleServices)
            throws InterruptedException
    {
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
                            ch.pipeline().addLast(new ShuffleClientHandler());
                        }
                    });
            bootstrap.connect(address).sync();
        }
    }

    public Iterator<Tuple2<?, ?>> readShuffleData(int shuffleId, int reduceId)
    {
        Iterator<Iterator<Tuple2<?, ?>>> iterators = concurrentMap.values().stream().map(ctx -> {
            ctx.begin(shuffleId, reduceId);
            Iterator<Tuple2<?, ?>> iterator = new Iterator<Tuple2<?, ?>>()
            {
                private final StateOption<byte[]> option = StateOption.empty();

                @Override
                public boolean hasNext()
                {
                    if (option.isDefined()) {
                        return true;
                    }
                    if (ctx.downloadEnd) {
                        return false;
                    }
                    try {
                        option.update(ctx.buffer.take());
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                }

                @Override
                public Tuple2<?, ?> next()
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
        return Iterators.concat(iterators);
    }

    private class ShuffleClientHandler
            extends ChannelInboundHandlerAdapter
    {
        private ChannelHandlerContext ctx;
        //todo: use number size buffer, 建议使用定长ByteBuffer
        private final BlockingQueue<byte[]> buffer = new ArrayBlockingQueue<>(1024);
        private volatile boolean downloadEnd = false;

        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            ShuffleClientHandler old = concurrentMap.put(ctx.channel().remoteAddress(), this);
            checkState(old == null, "");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            super.channelRead(ctx, msg);
            ByteBuf in = (ByteBuf) msg;
            while (in.readableBytes() > 0) {
                int len = in.readInt();
                byte[] bytes = new byte[len];
                buffer.put(bytes);
                in = in.readBytes(bytes);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)
                throws Exception
        {
            super.channelReadComplete(ctx);
            //done
            downloadEnd = true;
        }

        public void begin(int shuffleId, int reduceId)
        {
            ByteBuf byteBuf = ctx.alloc().buffer();
            byteBuf.writeInt(shuffleId);
            byteBuf.writeInt(reduceId);
            ctx.writeAndFlush(byteBuf);
        }
    }
}
