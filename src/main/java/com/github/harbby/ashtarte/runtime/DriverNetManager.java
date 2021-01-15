package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.gadtry.base.Serializables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class DriverNetManager

{
    private static final Logger logger = LoggerFactory.getLogger(DriverNetManager.class);
    private ChannelFuture future;

    public ConcurrentMap<SocketAddress, DriverNetManagerHandler> handlerMap = new ConcurrentHashMap<>();
    public BlockingQueue<TaskEvent> queue = new LinkedBlockingQueue<>();

    public DriverNetManager()
    {
    }

    public void start()
    {
        int port = 7079;
        NioEventLoopGroup boosGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(boosGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>()
                {
                    @Override
                    protected void initChannel(SocketChannel ch)
                            throws Exception
                    {
                        logger.info("find executor {}", ch.remoteAddress());
                        ch.pipeline().addLast(new DriverNetManagerHandler());
                    }
                });

        try {
            this.future = serverBootstrap.bind(port).sync();
            logger.info("started... driver manager service port is {}", port);
            //future.channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            //todo: ....
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {
        future.channel().close();
    }

    public class DriverNetManagerHandler
            extends LengthFieldBasedFrameDecoder
    {
        private ChannelHandlerContext context;

        public DriverNetManagerHandler()
        {
            super(65536, 0, 4);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            this.context = ctx;
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
                throws Exception
        {
            in = (ByteBuf) super.decode(ctx, in);
            if (in == null) {
                return null;
            }
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readBytes(bytes);
            ReferenceCountUtil.release(in);
            Event event = Serializables.byteToObject(bytes);
            if (event instanceof ExecutorEvent.ExecutorInitSuccessEvent) {
                SocketAddress shuffleService = ((ExecutorEvent.ExecutorInitSuccessEvent) event).getShuffleServiceAddress();
                logger.info("executor {} register succeed", ctx.channel().remoteAddress());
                handlerMap.put(shuffleService, this);
            }
            else if (event instanceof TaskEvent) {
                logger.info("task running end {}", event);
                queue.offer((TaskEvent) event);
            }
            else {
                throw new UnsupportedOperationException();
            }
            return event;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.warn("unknownIO", cause);
        }

        public void submitTask(Task<?> task)
        {
            if (!context.isRemoved()) {
                ByteBuf buffer = context.alloc().buffer();
                byte[] bytes;
                try {
                    bytes = Serializables.serialize(task);
                    buffer.writeInt(bytes.length).writeBytes(bytes);
                    context.writeAndFlush(buffer);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
