package com.github.harbby.ashtarte.runtime;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ExecutorBackendImpl extends ChannelInboundHandlerAdapter implements ExecutorBackend {

    private final BlockingQueue<TaskEvent> events = new LinkedBlockingQueue<>();

    public ExecutorBackendImpl() {
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ExecutorBackendImpl());
                    }
                });
        bootstrap.connect("localhost", 7079);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //读取数据
        super.channelRead(ctx, msg);
        ByteBuf result = (ByteBuf) msg;
        ByteBuf buf = result.readBytes(result.readableBytes());
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //发送数据
        super.channelActive(ctx);
        TaskEvent event = null;
        while (!ctx.isRemoved()) {
            event = events.take();
            ctx.write(event);
            ctx.flush();
        }
    }

    @Override
    public void updateState(TaskEvent state) {
        try {
            events.put(state);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
