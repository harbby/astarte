package com.github.harbby.ashtarte.runtime;

import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.MutableList;
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
public class ShuffleClientManager
        implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(ShuffleClientManager.class);
    private static final ThreadLocal<ShuffleClientManager> clientManagerTl = new ThreadLocal<>();
    private final Map<SocketAddress, ShuffleClientHandler> concurrentMap = new HashMap<>();
    private final List<ChannelFuture> futures = new ArrayList<>();

    private ShuffleClientManager(Set<SocketAddress> shuffleServices)
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

    public static ShuffleClientManager start(Set<SocketAddress> shuffleServices)
            throws InterruptedException
    {
        ShuffleClientManager clientManager = clientManagerTl.get();
        if (clientManager == null) {
            clientManager = new ShuffleClientManager(shuffleServices);
            clientManagerTl.set(clientManager);
        }
        return clientManager;
    }

    public <K, V> Iterator<Tuple2<K, V>> readShuffleData(int shuffleId, int reduceId)
    {
        ShuffleClientHandler handler = concurrentMap.values().iterator().next();
        handler.begin(shuffleId, reduceId);
        Iterator<Tuple2<K, V>> i1 = Iterators.map(handler, it -> {
            try {
                return Serializables.<Tuple2<K, V>>byteToObject(it);
            }
            catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });

//        Iterator<Tuple2<K, V>> i1 = concurrentMap.values().stream().map(handler -> {
//            handler.begin(shuffleId, reduceId);
//            return Iterators.map(handler, it -> {
//                try {
//                    return Serializables.<Tuple2<K, V>>byteToObject(it);
//                }
//                catch (IOException | ClassNotFoundException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//        }).iterator();

        List<Tuple2<K, V>> list = MutableList.copy(i1);
        logger.info("data line " + list.size());
        if (list.isEmpty()) {
            logger.error("-----------------------find error");
            logger.error("{}", handler.downloadEnd);
            logger.error("", handler.cause);
            logger.error("{}", handler.option.getValue());
            logger.error("{}", handler.buffer.size());
        }
        return list.iterator();
    }

    @Override
    public void close()
            throws IOException
    {
//        concurrentMap.forEach((k, v) -> v.close());
//        futures.forEach(channelFuture -> {
//            try {
//                channelFuture.channel().closeFuture().sync();
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        });
    }

    /**
     * smart code
     */
    private class ShuffleClientHandler
            extends ChannelInboundHandlerAdapter
            implements Iterator<byte[]>, Closeable
    {
        private final Thread taskThread;
        private ChannelHandlerContext ctx;
        //todo: use number size buffer, 建议使用定长ByteBuffer
        private final BlockingQueue<byte[]> buffer = new LinkedBlockingQueue<>(1024);
        private volatile boolean downloadEnd = false;
        private volatile Throwable cause;

        private ShuffleClientHandler(Thread taskThread) {this.taskThread = taskThread;}

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
            ReferenceCountUtil.release(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)
                throws Exception
        {
            downloadEnd = true;
            taskThread.interrupt();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            this.cause = cause;
            taskThread.interrupt();
        }

        private final StateOption<byte[]> option = StateOption.empty();

        private void begin(int shuffleId, int reduceId)
        {
            downloadEnd = false;
            cause = null;

            ByteBuf byteBuf = ctx.alloc().buffer();
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
            if (option.isDefined()) {
                return true;
            }

            if (this.downloadEnd) {
                return getEndStateValue();
            }

            try {
                option.update(this.buffer.take());
                return true;
            }
            catch (InterruptedException e) {
                // netty data io read failed
                if (this.cause != null) {
                    throw new RuntimeException("reduce reader failed", this.cause);
                }
                //主动kill task or download end
                return getEndStateValue();
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
