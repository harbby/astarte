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

import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.Task;
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
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class DriverNetManager

{
    private static final Logger logger = LoggerFactory.getLogger(DriverNetManager.class);
    private ChannelFuture future;

    private final ConcurrentMap<SocketAddress, DriverNetManagerHandler> executorHandlers = new ConcurrentHashMap<>();
    private final BlockingQueue<TaskEvent> queue = new LinkedBlockingQueue<>(65536);
    private final int executorNum;

    //todo: read conf
    private final int port;

    public DriverNetManager(AstarteConf astarteConf, int executorNum)
    {
        this.port = astarteConf.getInt(Constant.DRIVER_SCHEDULER_PORT, 7079);
        this.executorNum = executorNum;
    }

    public void start()
    {
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
        this.future = serverBootstrap.bind(port);
        logger.info("started... driver manager service port is {}", port);
        //future.channel().closeFuture().sync();
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
            throws InterruptedException
    {
        return queue.take();
    }

    public void initState()
    {
        queue.clear();
    }

    public void submitTask(Task<?> task)
    {
        task.getStage().setShuffleServices(new HashSet<>(executorHandlers.keySet()));
        //todo: 这里应按优化调度策略进行task调度
        executorHandlers.values().stream().findAny().get().submitTask(task);
    }

    private class DriverNetManagerHandler
            extends LengthFieldBasedFrameDecoder
    {
        private ChannelHandlerContext executorChannel;
        private SocketAddress socketAddress;

        public DriverNetManagerHandler()
        {
            super(65536, 0, 4);
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
