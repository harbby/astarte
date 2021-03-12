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
import com.github.harbby.gadtry.base.Throwables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class DriverNetManager

{
    private static final Logger logger = LoggerFactory.getLogger(DriverNetManager.class);
    private final ChannelFuture future;

    private final ConcurrentMap<SocketAddress, DriverNetManagerHandler> executorHandlers = new ConcurrentHashMap<>();
    private final BlockingQueue<TaskEvent> queue = new LinkedBlockingQueue<>(65536);
    private final int executorNum;

    //todo: read conf
    private final InetSocketAddress bindAddress;

    public DriverNetManager(int executorNum)
    {
        this.executorNum = executorNum;

        NioEventLoopGroup boosGroup = new NioEventLoopGroup(1);
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
                        logger.info("found executor {}", ch.remoteAddress());
                        ch.pipeline().addLast(new DriverNetManagerHandler());
                    }
                });
        try {
            this.future = serverBootstrap.bind(0).sync();
            int bindPort = ((InetSocketAddress) future.channel().localAddress()).getPort();
            this.bindAddress = InetSocketAddress.createUnresolved(InetAddress.getLocalHost().getHostName(), bindPort);
            logger.info("started driver manager service, bind address is {}", future.channel().localAddress());
            future.channel().closeFuture().addListener((ChannelFutureListener) channelFuture -> {
                boosGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            });
        }
        catch (InterruptedException | UnknownHostException e) {
            throw Throwables.throwsThrowable(e);
        }
    }

    public InetSocketAddress getBindAddress()
    {
        return bindAddress;
    }

    public void awaitAllExecutorRegistered()
            throws InterruptedException
    {
        //wait 等待所有exector上线
        while (executorHandlers.size() != executorNum) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        logger.info("all executor({}) Initialized", executorNum);
    }

    public void stop()
    {
        future.channel().close();
    }

    public TaskEvent awaitTaskEvent()
    {
        try {
            return queue.take();
        }
        catch (InterruptedException e) {
            throw new UnsupportedOperationException("kill task?"); //todo: job kill
        }
    }

    public void initState()
    {
        queue.clear();
    }

    public SocketAddress submitTask(Task<?> task)
    {
        List<SocketAddress> addresses = new ArrayList<>(executorHandlers.keySet());
        //cache算子会使得Executor节点拥有状态，调度时应注意幂等
        SocketAddress address = addresses.get(task.getTaskId() % executorNum);
        executorHandlers.get(address).submitTask(task);
        return address;
    }

    private class DriverNetManagerHandler
            extends LengthFieldBasedFrameDecoder
    {
        private ChannelHandlerContext executorChannel;
        private SocketAddress socketAddress;

        public DriverNetManagerHandler()
        {
            super(1048576, 0, 4);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            this.executorChannel = ctx;
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
                logger.info("executor {} register succeed, shuffle service bind {}", ctx.channel().remoteAddress(), shuffleService);
                executorHandlers.put(shuffleService, this);
            }
            else if (event instanceof TaskEvent) {
                logger.info("task {} finished", ((TaskEvent) event).getTaskId());
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
            checkState(!executorChannel.isRemoved());
            ByteBuf buffer = executorChannel.alloc().buffer();
            byte[] bytes;
            try {
                bytes = Serializables.serialize(task);
                buffer.writeInt(bytes.length).writeBytes(bytes);
                executorChannel.writeAndFlush(buffer);
            }
            catch (IOException e) {
                //todo: 意外的序列化错误 应在每个用户函数输入处check是否可序列化
                throw new UncheckedIOException("found unexpected error from task serializing.", e);
            }
        }
    }
}
