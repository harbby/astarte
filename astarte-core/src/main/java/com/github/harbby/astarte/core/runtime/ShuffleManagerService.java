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

import com.github.harbby.astarte.core.operator.SortShuffleWriter;
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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public final class ShuffleManagerService
{
    private static final Logger logger = LoggerFactory.getLogger(ShuffleManagerService.class);
    private final File shuffleBaseDir;
    private ChannelFuture future;
    private volatile File currentJobWorkDir;
    private final InetSocketAddress shuffleServiceBindAddress;

    public ShuffleManagerService(File shuffleBaseDir)
            throws UnknownHostException, InterruptedException
    {
        this.shuffleBaseDir = shuffleBaseDir;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                FileUtils.deleteDirectory(shuffleBaseDir);
                logger.debug("clear shuffle data temp dir {}", shuffleBaseDir);
            }
            catch (IOException e) {
                logger.error("clear shuffle data temp dir failed", e);
            }
        }));

        final NioEventLoopGroup boosGroup = new NioEventLoopGroup(1);
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
        this.future = serverBootstrap.bind(0).sync();
        logger.info("stared shuffle service ,the port is {}", future.channel().localAddress());
        future.channel().closeFuture().addListener((ChannelFutureListener) channelFuture -> {
            boosGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        });

        int bindPort = ((InetSocketAddress) future.channel().localAddress()).getPort();
        this.shuffleServiceBindAddress = InetSocketAddress.createUnresolved(InetAddress.getLocalHost().getHostName(), bindPort);
    }

    public void updateCurrentJobId(int jobId)
    {
        this.currentJobWorkDir = new File(shuffleBaseDir, String.valueOf(jobId));
    }

    public InetSocketAddress getShuffleServiceBindAddress()
    {
        return shuffleServiceBindAddress;
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
            int mapId = in.readInt();
            ReferenceCountUtil.release(msg);
            checkState(mapId >= 0, "Bad mapId request, the range should be [0 - ?], but id is %s", reduceId);
            checkState(shuffleId >= 0, "Bad shuffleId request, the range should be [0 - ?], but id is %s", reduceId);

            File shuffleFile = new File(currentJobWorkDir, String.format("shuffle_merged_%s_%s.data", shuffleId, mapId));
            //read shuffle file header
            FileInputStream fileInputStream = new FileInputStream(shuffleFile);
            DataInputStream dataInputStream = new DataInputStream(fileInputStream);
            long[] segmentEnds = new long[dataInputStream.readInt()];
            long[] segmentRowSize = new long[segmentEnds.length];
            for (int i = 0; i < segmentEnds.length; i++) {
                segmentEnds[i] = dataInputStream.readLong();
                segmentRowSize[i] = dataInputStream.readLong();
            }

            int fileHeaderSize = SortShuffleWriter.getSortMergedFileHarderSize(segmentEnds.length);
            final long length;
            final long position;
            if (reduceId == 0) {
                position = fileHeaderSize;
                length = segmentEnds[reduceId];
            }
            else {
                checkState(reduceId >= 0, "Bad reduceId request, the range should be [0 - ?], but id is %s", reduceId);
                position = fileHeaderSize + segmentEnds[reduceId - 1];
                length = segmentEnds[reduceId] - segmentEnds[reduceId - 1];
            }
            //write net header info
            ByteBuf header = ctx.alloc().heapBuffer(16);
            header.writeLong(length);
            header.writeLong(segmentRowSize[reduceId]);
            ctx.write(header);
            //write data by zero copy
            ctx.writeAndFlush(new DefaultFileRegion(fileInputStream.getChannel(), position, length), ctx.newProgressivePromise())
                    .addListener((ChannelFutureListener) future -> {
                        logger.debug("send file {} done, size = {}", shuffleFile, length);
                        fileInputStream.close();
                    });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception
        {
            logger.error("", cause);
        }
    }
}
