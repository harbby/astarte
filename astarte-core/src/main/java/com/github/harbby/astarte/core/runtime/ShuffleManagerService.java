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

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderInputStream;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class ShuffleManagerService
{
    private static final Logger logger = LoggerFactory.getLogger(ShuffleManagerService.class);
    private final File shuffleWorkDir;
    private ChannelFuture future;
    private volatile int currentJobId;

    public ShuffleManagerService(String executorUUID)
    {
        this.shuffleWorkDir = getShuffleWorkDir(executorUUID);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            File workDir = ShuffleManagerService.getShuffleWorkDir(executorUUID);
            try {
                FileUtils.deleteDirectory(workDir);
                logger.debug("clear shuffle data temp dir {}", workDir);
            }
            catch (IOException e) {
                logger.error("clear shuffle data temp dir failed", e);
            }
        }));
    }

    public void updateCurrentJobId(int jobId)
    {
        this.currentJobId = jobId;
    }

    public SocketAddress start()
            throws UnknownHostException, InterruptedException
    {
        final NioEventLoopGroup boosGroup = new NioEventLoopGroup();
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
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
                        ch.pipeline().addLast(new ShuffleServiceHandler());
                    }
                });
        this.future = serverBootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0)).sync();
        logger.info("stared shuffle service ,the port is {}", future.channel().localAddress());
        future.channel().closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        });
        return future.channel().localAddress();
    }

    public void join()
            throws InterruptedException
    {
        future.channel().closeFuture().sync();
    }

    public void stop()
    {
        future.channel().close();
    }

    private class ShuffleServiceHandler
            extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelActive(ChannelHandlerContext ctx)
                throws Exception
        {
            logger.info("found shuffle client {}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            ByteBuf in = (ByteBuf) msg;
            int shuffleId = in.readInt();
            int reduceId = in.readInt();
            ReferenceCountUtil.release(msg);
            List<File> files = getShuffleDataInput(shuffleWorkDir, currentJobId, shuffleId, reduceId);  //获取多个带发送文件

            if (files.isEmpty()) {
                //not found any file
                ByteBuf finish = ctx.alloc().directBuffer(4, 4).writeInt(0);
                ctx.writeAndFlush(finish);
                return;
            }
            //write header info
            int headerSize = files.size() * 8 + 4;
            ByteBuf header = ctx.alloc().directBuffer(headerSize);
            header.writeInt(files.size());
            for (File file : files) {
                header.writeLong(file.length());
            }
            ctx.write(header);
            //write data by zero copy
            for (File file : files) {
                FileInputStream inputStream = new FileInputStream(file);
                ctx.writeAndFlush(new DefaultFileRegion(inputStream.getChannel(), 0, file.length()), ctx.newProgressivePromise())
                        .addListener((ChannelFutureListener) future -> {
                            logger.debug("send file {} done, size = {}", file, file.length());
                            inputStream.close();
                        });
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.error("", cause);
        }
    }

    public static File getShuffleWorkDir(String executorUUID)
    {
        return new File("/tmp/ashtarte-" + executorUUID);
    }

    public <K, V> Iterator<Tuple2<K, V>> getShuffleDataIterator(Encoder<Tuple2<K, V>> encoder, int shuffleId, int reduceId)
    {
        requireNonNull(encoder, "encoder is null");
        List<File> files = getShuffleDataInput(shuffleWorkDir, currentJobId, shuffleId, reduceId);

        return Iterators.concat(files.stream()
                .map(file -> {
                    try {
                        return new EncoderInputStream<>(new FileInputStream(file), encoder);
                    }
                    catch (FileNotFoundException e) {
                        throw Throwables.throwsThrowable(e);
                    }
                }).iterator());
    }

    private static List<File> getShuffleDataInput(File shuffleWorkDir, int jobId, int shuffleId, int reduceId)
    {
        File[] files = new File(shuffleWorkDir, String.valueOf(jobId)).listFiles();
        if (files == null) {
            return Collections.emptyList();
        }
        return Stream.of(files)
                .filter(x -> x.getName().startsWith("shuffle_" + shuffleId + "_")
                        && x.getName().endsWith("_" + reduceId + ".data"))
                .collect(Collectors.toList());
    }
}
