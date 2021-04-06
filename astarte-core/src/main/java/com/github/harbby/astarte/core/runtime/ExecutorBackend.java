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

import com.github.harbby.astarte.core.api.Task;
import com.github.harbby.gadtry.base.Serializables;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * 发送task结束信号给driver
 * 需要启用发送心跳能力
 */
public class ExecutorBackend
{
    private static final Logger logger = LoggerFactory.getLogger(ExecutorBackend.class);
    private final Executor executor;
    private final SocketAddress driverManagerAddress;
    private Channel channel;

    public ExecutorBackend(Executor executor, SocketAddress driverManagerAddress)
    {
        this.executor = executor;
        this.driverManagerAddress = driverManagerAddress;
    }

    public void start(InetSocketAddress shuffleServiceAddress)
            throws InterruptedException
    {
        final ExecutorNetHandler handler = new ExecutorNetHandler();
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
        bootstrap.connect(driverManagerAddress)
                .addListener((ChannelFutureListener) future -> {
                    this.channel = future.channel();
                    writeEvent(channel, new ExecutorEvent.ExecutorInitSuccessEvent(shuffleServiceAddress));
                }).sync()
                .channel().closeFuture()
                .addListener((ChannelFutureListener) future -> {
                    workerGroup.shutdownGracefully();
                });
        logger.info("connecting... driver manager address {}", driverManagerAddress);
    }

    private class ExecutorNetHandler
            extends LengthFieldBasedFrameDecoder
    {
        public ExecutorNetHandler()
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
            ReferenceCountUtil.release(in);

            Task<?> task = Serializables.byteToObject(bytes);
            executor.runTask(task);
            return task;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.error("", cause);
        }
    }

    public synchronized void updateState(Event event)
            throws IOException
    {
        writeEvent(channel, event);
    }

    private static void writeEvent(Channel channel, Event event)
            throws IOException
    {
        ByteBuf buffer = channel.alloc().buffer();
        byte[] bytes = Serializables.serialize(event);
        buffer.writeInt(bytes.length).writeBytes(bytes);
        channel.writeAndFlush(buffer);
    }
}
