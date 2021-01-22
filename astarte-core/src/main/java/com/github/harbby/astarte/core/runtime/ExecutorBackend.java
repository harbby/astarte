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

import com.github.harbby.astarte.core.MapTaskState;
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

import java.io.IOException;
import java.net.SocketAddress;

/**
 * 发送task结束信号给driver
 * 需要启用发送心跳能力
 */
public class ExecutorBackend

{
    private final Executor executor;
    private Channel channel;

    public ExecutorBackend(Executor executor)
    {
        this.executor = executor;
    }

    public void start(SocketAddress shuffleServiceAddress)
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
        bootstrap.connect("localhost", 7079)
                .addListener((ChannelFutureListener) future -> {
                    this.channel = future.channel();
                    writeEvent(channel, new ExecutorEvent.ExecutorInitSuccessEvent(shuffleServiceAddress));
                }).sync();
    }

    private class ExecutorBackendHandler
            extends LengthFieldBasedFrameDecoder
    {
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
            ReferenceCountUtil.release(in);

            Task<MapTaskState> task = Serializables.byteToObject(bytes);
            executor.runTask(task);
            return task;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            cause.printStackTrace();
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
