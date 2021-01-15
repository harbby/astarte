package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.MapTaskState;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.gadtry.base.Serializables;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;

/**
 * 发送task结束信号给driver
 * 需要启用发送心跳能力
 */
public class ExecutorBackend

{
    private final Executor executor;
    private ExecutorBackendHandler handler;

    public ExecutorBackend(Executor executor)
    {
        this.executor = executor;
    }

    public void start()
            throws InterruptedException
    {
        final ExecutorBackendHandler handler = new ExecutorBackendHandler();

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
                        ch.pipeline().addLast(handler);
                    }
                });
        bootstrap.connect("localhost", 7079).sync();
        this.handler = handler;
    }

    public void updateState(Event event)
            throws IOException
    {
        handler.updateState(event);
    }

    private class ExecutorBackendHandler
            extends LengthFieldBasedFrameDecoder
    {
        private ChannelHandlerContext ctx;

        public ExecutorBackendHandler()
        {
            super(6553600, 0, 4);
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
            Task<MapTaskState> task = Serializables.byteToObject(bytes);
            executor.runTask(task);
            return task;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            this.ctx = ctx;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            cause.printStackTrace();
        }

        public synchronized void updateState(Event event)
                throws IOException
        {
            ByteBuf buffer = ctx.alloc().buffer();
            byte[] bytes = Serializables.serialize(event);
            buffer.writeInt(bytes.length).writeBytes(bytes);
            ctx.writeAndFlush(buffer);
        }
    }
}
