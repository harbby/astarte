package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.gadtry.base.Serializables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DriverNetManagerHandler
        extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(DriverNetManagerHandler.class);

    public DriverNetManagerHandler()
    {
    }

    public static Map<InetSocketAddress, DriverNetManagerHandler> handlerMap = new HashMap<>();

    public static void start()
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
                        logger.info("find executor {} register", ch.remoteAddress());
                        DriverNetManagerHandler driverNetManagerHandler = new DriverNetManagerHandler();
                        ch.pipeline().addLast(driverNetManagerHandler);
                        handlerMap.put(ch.remoteAddress(), driverNetManagerHandler);
                    }
                });

        try {
            ChannelFuture future = serverBootstrap.bind(port).sync();
            //future.channel().closeFuture().sync();
        }
        catch (InterruptedException e) {
            //todo: ....
            throw new RuntimeException(e);
        }
    }

    private ChannelHandlerContext context;

    @Override
    public void channelActive(ChannelHandlerContext ctx)
            throws Exception
    {
        this.context = ctx;
    }

    public static BlockingQueue<TaskEvent> queue = new LinkedBlockingQueue<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception
    {
        ByteBuf in = (ByteBuf) msg;
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readBytes(bytes);
        TaskEvent taskEvent = Serializables.byteToObject(bytes);
        queue.offer(taskEvent);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception
    {
        cause.printStackTrace();
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
